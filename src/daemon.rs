use std::collections::HashMap;
/// daemon.rs
///
/// Contains all code that the daemon needs to run.
///
/// # Set up
///
/// 1. The program is detached from stdout, stderr, stdin and the parent
///    process, effectively demonizing it. The main thread is terminated.
///
/// 2. `fn handle()` sets up an http(s) listening socket.
///
/// 3. The job queue is created or restored from a file.
///
/// 4. The condition variable governing the communication between the queue
///    and the client handler is set up.
///
/// 5. A new thread is spawned to process the job queue (see `fn run_queue`)
///
/// 6. A new thread is spawned to process external signals like SIGTERM
///
/// 7. The job queue is notified that it may start/resume operating
///
/// 8. A loop that processes client requests infinitely is started
///
/// # Operations
///
/// Once everything is properly set up, the main thread takes care of
/// accepting and processing client requests. For each client request,
/// the function `handle_client` is invoked that decodes the JSON block
/// and acts upon the request.
///
/// To conserve CPU time, the job queue thread is blocking on a condition
/// variable when it is idle. Once a client requests that a job is submitted
/// to the queue, it is moved into the job queue structure and the thread
/// is woken up. It then starts processing one job after another until the
/// queue is empty again where it blocks on the variable again.
// std
use std::error::Error;
use std::io::Result;
use std::net::SocketAddr;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

// crates
use daemonize::Daemonize;
use reqwest::Url;
use serde_json;
use systemd::daemon;
use tiny_http::{Server, SslConfig};

// modules
use job_queue::{FailReason, Job, JobQueue, JobState, QueueState};
use protocol::{Request, Response};
use state::State;

/// Detaches the current process from the terminal and the current task
/// session. Optionally takes a path to a file where the pid of the
/// process is stored, for later use by managers such as systemd.
fn daemonize(pidfile: Option<PathBuf>) -> Result<()> {
    let uid = nix::unistd::Uid::current().as_raw();
    let pidfile_default = PathBuf::from(&format!("/run/user/{}/qmanager.pid", uid));

    let pidfile = pidfile.unwrap_or(pidfile_default);

    if let Err(e) = Daemonize::new().pid_file(pidfile).start() {
        eprintln!("Failed to daemonize: {}", e);
    }
    Ok(())
}

/// Sets up a HTTP or HTTPS server, depending on whether both `cert`and `key`
/// are given. Listens on the given TCP `port` on all available IP addresses
fn spawn_https(
    tcp_port: u16,
    cert: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
) -> std::result::Result<Server, Box<dyn Error + Sync + Send>> {
    let bind_address = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], tcp_port));

    if cert.is_some() ^ key.is_some() {
        panic!("You must either provide an SSL certificate AND private key or none of them.");
    }

    // decide on whether http or https should be used
    match cert {
        Some(c) => {
            let ssl_config = SslConfig {
                certificate: c,
                private_key: key.unwrap(),
            };
            Server::https(bind_address, ssl_config)
        }
        None => Server::http(bind_address),
    }
}

/// Handles a single HTTP request sent by a single client.
/// Translates the JSON block to a Request, evaluates the
/// request (i.e. adds a job to the queue) and returns
/// a JSON result to the client.
fn handle_client(
    mut httprequest: tiny_http::Request,
    q_mutex: Arc<(Mutex<JobQueue>, Condvar)>,
    dump_protocol: bool,
    state: Arc<Mutex<State>>,
) {
    let (ref q_mutex, ref cvar) = *q_mutex;

    let mut s = String::from("");
    httprequest.as_reader().read_to_string(&mut s).unwrap();

    if dump_protocol {
        debug!("[handle_client] Got data: {}", &s);
    }
    let request = serde_json::from_str(&s);

    debug!("[handle_client] Processing request: {:?}", request);

    let (status_code, response_s) = match request {
        Ok(Request::GetQueuedJobs) => {
            let q = q_mutex.lock().unwrap();
            let items = q.iter_queued().cloned().collect();
            (
                200,
                serde_json::to_string_pretty(&Response::GetJobs(items)).unwrap(),
            )
        }

        Ok(Request::GetQueueState) => {
            let q = q_mutex.lock().unwrap();
            let state = q.get_state();
            (
                200,
                serde_json::to_string_pretty(&Response::QueueState(state)).unwrap(),
            )
        }

        Ok(Request::SetQueueState(new_state)) => {
            let mut q = q_mutex.lock().unwrap();
            q.set_state(new_state);
            cvar.notify_one();
            let state = state.lock().unwrap();
            state.save(&q).expect("Could not write program state");
            (
                200,
                serde_json::to_string_pretty(&Response::QueueState(new_state)).unwrap(),
            )
        }

        Ok(Request::GetFinishedJobs) => {
            let q = q_mutex.lock().unwrap();
            let items = q.iter_finished().cloned().collect();
            (
                200,
                serde_json::to_string_pretty(&Response::GetJobs(items)).unwrap(),
            )
        }

        Ok(Request::RemoveJob(id)) => {
            let mut q = q_mutex.lock().unwrap();
            let s = q.remove(id);
            let state = state.lock().unwrap();
            state.save(&q).expect("Could not write program state");
            match s {
                Ok(job) => (
                    200,
                    serde_json::to_string_pretty(&Response::GetJob(job)).unwrap(),
                ),
                Err(FailReason::NoSuchJob) => (
                    422,
                    serde_json::to_string_pretty(&Response::Error("No such job".to_string()))
                        .unwrap(),
                ),
                Err(FailReason::WrongJobState) => (
                    422,
                    serde_json::to_string_pretty(&Response::Error(
                        "Job is currently running and cannot be removed".to_string(),
                    ))
                    .unwrap(),
                ),
            }
        }

        Ok(Request::KillJob(id)) => {
            let mut q = q_mutex.lock().unwrap();
            match q.send_sigterm(id) {
                Ok(_) => (200, serde_json::to_string_pretty(&Response::Ok).unwrap()),
                _ => (
                    422,
                    serde_json::to_string_pretty(&Response::Error(
                        "Job is currently not running.".to_string(),
                    ))
                    .unwrap(),
                ),
            }
        }

        Ok(Request::SubmitJob(cmdline)) => {
            let mut q = q_mutex.lock().unwrap();
            let id = q.submit(cmdline);
            let state = state.lock().unwrap();
            state.save(&q).expect("Could not write program state");
            cvar.notify_one();
            (
                200,
                serde_json::to_string_pretty(&Response::SubmitJob(id)).unwrap(),
            )
        }

        Err(e) => {
            if e.is_io() {
                (500, e.to_string().to_owned())
            } else {
                (400, e.to_string().to_owned())
            }
        }
    };

    if dump_protocol {
        debug!(
            "[handle_client] Returning {} response: {}",
            status_code, &response_s
        );
    } else if status_code != 200 {
        info!(
            "[handle_client] Sending errorneous status code {} with response: {}",
            status_code, &response_s
        );
    }

    let mut response = tiny_http::Response::from_string(response_s).with_status_code(status_code);

    if status_code == 200 {
        response.add_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
        );
    } else {
        response.add_header(
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap(),
        );
    }

    if let Err(err) = httprequest.respond(response) {
        eprintln!("Failed to send response to client: {:?}", err);
    }
}

/// Calls the notification URL for the given job
fn run_notify_command(job: Job, url: &Url) -> Result<()> {
    let mut url = url.clone();
    url.set_query(Some(&format!("jobid={}", job.id)));
    let s = url.as_str().to_string();

    match reqwest::get(url) {
        Err(e) => {
            error!("Failed to call notify url {:#?}: {}", s, e.to_string());
            Ok(())
        }
        Ok(ref r) if r.status().is_success() => {
            debug!(
                "Notification call to {:#?} succeeded. Response: {}",
                s,
                r.status().as_str()
            );
            Ok(())
        }
        Ok(r) => {
            debug!(
                "Notification call to {:#?} failed. Response: {}",
                s,
                r.status().as_str()
            );
            Ok(())
        }
    }
}

/// Starts working the job queue.
///
/// First, it checks whether a job is available in the queue.
/// Then,
/// 1. no job is available. The thread goes to sleep and waits for a signal
///    on the condition variable within the `q_mutex` tuple.
///
/// 1.1 If the thread is woken up, it checks again for an available job. If
///     there is none, it returns to sleep. If there is, proceed to (2).
///
/// 2. Mark the job as `Running` and execute it
///
/// 3. Collect the return value, stdout and stderr of the job
///
/// 4. Call the notification handler
///
/// 5. Mark the job as `Finished` and return to (1).
fn run_queue(
    q_mutex: &Arc<(Mutex<JobQueue>, Condvar)>,
    notify_url: Option<String>,
    appkeys: HashMap<String, PathBuf>,
) -> ! {
    let (ref q_mutex, ref cvar) = **q_mutex;

    let notify_url = notify_url.map(|s| Url::parse(&s).unwrap());

    // main loop
    loop {
        let mut job: Option<Job> = None;

        // acquire a new job to run
        while job.is_none() {
            let mut q = q_mutex.lock().unwrap();

            if let Some(j) = q.schedule() {
                job = Some(j);
                break;
            } else {
                debug!("[queue runner] Falling asleep");
                q = cvar.wait(q).unwrap();
                debug!("[queue runner] Woke up");
                job = q.schedule();
            }
        }

        let job = job.unwrap();
        info!("[queue runner] Running job {}", job.id);

        /*
        We need to prepend 'exec' to the command line. Otherwise, the command
        spawner would yield the PID of 'sh'. exec replaces the shell with
        the acutal process that we would like to run, keeping the pid.
        */
        let mut cmditer = job.cmdline.split_ascii_whitespace();
        let appkey = cmditer.next().unwrap_or("");
        let args: Vec<&str> = cmditer.collect();
        let cmdline_remainder = args.join(" ");
        let mut actual_cmd = appkeys.get(appkey).cloned();
        if appkey.is_empty() || actual_cmd.is_none() {
            error!("Invalid appkey");
            actual_cmd = Some(PathBuf::from("invalid-appkey"));
        }
        let actual_cmd = actual_cmd.unwrap();
        let cmdline_wrapper = format!(
            "exec {} {}",
            actual_cmd.to_str().unwrap(),
            cmdline_remainder
        );

        // Spawn the process, collect stdout, stderr and pid.
        // Continues once the job is terminated (one way or another).
        let cmd = Command::new("sh")
            .arg("-c")
            .arg(cmdline_wrapper)
            .current_dir("/")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .and_then(|child| {
                {
                    let mut q = q_mutex.lock().unwrap();
                    q.assign_pid(job.id, child.id());
                }
                child.wait_with_output()
            });

        // Collect status of finished job and forward status to the queue
        let job = match cmd {
            // Job was successfully launched. This does not mean that the
            // process itself was successful.
            Ok(output) => {
                let mut q = q_mutex.lock().unwrap();

                // Job was terminated due to a signal, e.g. unhandled SIGTERM,
                // SIGSEGV, etc. see signal(7) for default signal actions.
                if let Some(signum) = output.status.signal() {
                    info!("[queue runner] Job was killed with signal {}", signum);
                    q.finish(
                        JobState::Killed(signum),
                        String::from_utf8_lossy(&output.stdout).to_string(),
                        String::from_utf8_lossy(&output.stderr).to_string(),
                    )
                } else {
                    // Job has terminated by itself and a regular exit code
                    // was returned.
                    let status = output.status.code().unwrap();
                    info!("[queue runner] Job has terminated with code {}", status);
                    q.finish(
                        JobState::Terminated(status),
                        String::from_utf8_lossy(&output.stdout).to_string(),
                        String::from_utf8_lossy(&output.stderr).to_string(),
                    )
                }
            }
            // Job could not be started.
            Err(e) => {
                let mut q = q_mutex.lock().unwrap();
                let message = e.to_string();
                error!("[queue runner] Failed to launch job: {}", message);
                q.finish(
                    JobState::Failed(message),
                    String::from(""),
                    String::from(""),
                )
            }
        };

        // Notify the server of job completion regardless of the result
        if let Some(j) = job {
            if notify_url.is_some() {
                let id = j.id;
                if let Err(e) = run_notify_command(j, notify_url.as_ref().unwrap()) {
                    error!(
                        "Failed to run notify command for job {}: {}",
                        id,
                        e.to_string()
                    );
                }
            }
        }
    }
}

pub fn handle(
    tcp_port: u16,
    pidfile: Option<PathBuf>,
    cert: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    foreground: bool,
    dump_protocol: bool,
    appkeys: HashMap<String, PathBuf>,
    notify_url: Option<String>,
    state: State,
) -> Result<()> {
    if !foreground {
        daemonize(pidfile)?;
    }

    let httpd = match spawn_https(tcp_port, cert, key) {
        Ok(s) => s,
        Err(e) => {
            error!(
                "Could not set up listening socket on port {}: {}",
                tcp_port,
                e.to_string()
            );
            panic!(
                "Could not set up listening socket on port {}: {}",
                tcp_port,
                e.to_string()
            )
        }
    };

    daemon::notify(false, [(daemon::STATE_READY, "1")].iter())?;
    info!("Daemon version {} ready.", crate_version!());
    info!("Application keys available: {:?}", appkeys.keys());

    let job_queue = Arc::new((Mutex::new(state.load_queue()), Condvar::new()));

    // Reset first job to a defined state if the daemon has been interrupted
    {
        let (ref q_mutex, _) = *job_queue;
        let mut q = q_mutex.lock().unwrap();
        match q.get_state() {
            QueueState::Stopped => {}
            QueueState::Stopping => q.reset_first_job(JobState::Queued),
            QueueState::Running => q.reset_first_job(JobState::Failed(
                "Interrupted by system failure, please re-submit or ask for assistence".to_owned(),
            )),
        }
    }

    // spawn queue runner
    let queue_runner_q = job_queue.clone();
    let queue_runner = thread::Builder::new()
        .name("Queue Runner".to_owned())
        .spawn(move || run_queue(&queue_runner_q, notify_url, appkeys))
        .unwrap();

    // set up the program state to be shared among threads,
    // namely the signal handler (ought to save state on SIGTERM)
    // and the current thread, handling client requests
    let state = Arc::new(Mutex::new(state));

    // spawn signal handler to collect SIGTERM signals sent by systemd unit
    // create clones before spawning, otherwise the "originals" would be moved into the closure
    let sig_q = Arc::clone(&job_queue);
    let sig_state = Arc::clone(&state);
    let signal_handler = setup_signal_handler(sig_q, sig_state);

    // handle incoming TCP connections
    for request in httpd.incoming_requests() {
        debug!("Request: {:?}", request);

        handle_client(request, job_queue.clone(), dump_protocol, state.clone());
    }

    // collect threads in case of program termination
    queue_runner.join().unwrap();
    signal_handler.join().unwrap();
    Ok(())
}

// Creates a thread waiting for SIGTERM. Once encountered, state save is triggered and
// the main process terminates.
fn setup_signal_handler(
    job_queue: Arc<(Mutex<JobQueue>, Condvar)>,
    state: Arc<Mutex<State>>,
) -> std::thread::JoinHandle<()> {
    let signals = signal_hook::iterator::Signals::new(&[signal_hook::SIGTERM]).unwrap();

    thread::Builder::new()
        .name("Signal Handler".to_owned())
        .spawn(move || {
            for signal in signals.forever() {
                match signal {
                    signal_hook::SIGTERM => {
                        info!("Caught SIGTERM, initiating state saving");
                        let state = state.lock().unwrap();
                        let q = job_queue.0.lock().unwrap();
                        state.save(&q).expect("Could not write program state");
                        std::process::exit(0);
                    }
                    _ => unreachable!(),
                }
            }
        })
        .unwrap()
}
