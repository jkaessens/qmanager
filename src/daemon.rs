use std::error::Error;
use std::io::Result;
use std::net::{SocketAddr};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::path::PathBuf;

use daemonize::Daemonize;
use tiny_http::{Server, SslConfig};
use systemd::daemon;

use serde_json;

use job_queue::{Job, JobQueue, JobState};
use protocol::{Request, Response};
use std::collections::HashMap;
use reqwest::Url;

fn daemonize(pidfile: Option<PathBuf>) -> Result<()> {
    let uid = nix::unistd::Uid::current().as_raw();
    let pidfile_default = PathBuf::from(&format!("/run/user/{}/qmanager.pid", uid));

    let pidfile = pidfile.unwrap_or(pidfile_default);

    if let Err(e) = Daemonize::new()
        .pid_file(pidfile)
        .start()
    {
        eprintln!("Failed to daemonize: {}", e);
    }
    Ok(())
}

fn spawn_https(tcp_port: u16, cert: Option<Vec<u8>>, key: Option<Vec<u8>>) -> std::result::Result<Server, Box<dyn Error+Sync+Send>> {
    let bind_address = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], tcp_port));

    if cert.is_some() ^ key.is_some() {
        panic!("You must either provide an SSL certificate AND private key or none of them.");
    }

    match cert {
        Some(c) => {
            let ssl_config = SslConfig {
                certificate: c,
                private_key: key.unwrap()
            };
            Server::https(bind_address, ssl_config)
        }
        None => {
            Server::http(bind_address)
        }
    }
}


fn handle_client(
    mut httprequest: tiny_http::Request,
    q_mutex: &Arc<(Mutex<JobQueue>, Condvar)>,
    dump_protocol: bool,
) {
    let (ref q_mutex, ref cvar) = **q_mutex;


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
            (200, serde_json::to_string_pretty(&Response::GetJobs(items)).unwrap())
        }

        Ok(Request::GetFinishedJobs) => {
            let q = q_mutex.lock().unwrap();
            let items = q.iter_finished().cloned().collect();
            (200, serde_json::to_string_pretty(&Response::GetJobs(items)).unwrap())
        }

        Ok(Request::RemoveJob(id)) => {
            let mut q = q_mutex.lock().unwrap();
            match q.remove(id) {
                Ok(job) => {
                    (200, serde_json::to_string_pretty(&Response::GetJob(job)).unwrap())
                }
                _ => {
                    (422, serde_json::to_string_pretty(&Response::Error(
                            "No such job".to_string(),
                        )).unwrap())
                }
            }
        }

        Ok(Request::KillJob(id)) => {
            let mut q = q_mutex.lock().unwrap();
            match q.send_sigterm(id) {
                Ok(_) => {
                    (200, serde_json::to_string_pretty(&Response::Ok).unwrap())
                }
                _ => {
                    (422, serde_json::to_string_pretty(&Response::Error(
                            "No such job".to_string(),
                        )).unwrap())
                }
            }
        }

        Ok(Request::SubmitJob(cmdline)) => {
            let mut q = q_mutex.lock().unwrap();
            let id = q.submit(cmdline);
            cvar.notify_one();
            (200, serde_json::to_string_pretty(&Response::SubmitJob(id)).unwrap())
        }

        Err(e) => {
            if e.is_io() {
                (500, e.description().to_owned())
            } else {
                (400, e.description().to_owned())
            }
        }
    };

    if dump_protocol {
        debug!("[handle_client] Returning {} response: {}", status_code, &response_s);
    } else if status_code != 200 {
        info!("[handle_client] Sending errorneous status code {} with response: {}", status_code, &response_s);
    }

    let mut response =
        tiny_http::Response::from_string(response_s)
            .with_status_code(status_code);

    if status_code == 200 {
        response.add_header(tiny_http::Header::from_bytes(
            &b"Content-Type"[..], &b"application/json"[..]
        ).unwrap());
    } else {
        response.add_header(tiny_http::Header::from_bytes(
            &b"Content-Type"[..], &b"text/plain"[..]
        ).unwrap());
    }

    if let Err(err) = httprequest.respond(response) {
        eprintln!("Failed to send response to client: {:?}", err);
    }
}

fn run_notify_command(job: Job, url: &Url) -> Result<()> {
    let mut url = url.clone();
    url.set_query(Some(&format!("jobid={}", job.id)));
    let s = url.as_str().to_string();
    if let Err(e) = reqwest::get(url) {
        error!("Failed to call notify url {:#?}: {}", s, e.description());
        Ok(())
    } else {
        debug!("Notification call to {:#?} succeeded.", s);
        Ok(())
    }
}

fn run_queue(q_mutex: &Arc<(Mutex<JobQueue>, Condvar)>, notify_url: Option<String>, appkeys: HashMap<String,PathBuf>) {
    let (ref q_mutex, ref cvar) = **q_mutex;

    let notify_url = notify_url.map(|s| Url::parse(&s).unwrap());

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
        let args :Vec<&str> = cmditer.into_iter().collect();
        let cmdline_remainder = args.join(" ");
        let mut actual_cmd = appkeys.get(appkey).cloned();
        if appkey.is_empty() || actual_cmd.is_none() {
            error!("Invalid appkey");
            actual_cmd = Some(PathBuf::from("invalid-appkey"));
        }
        let actual_cmd = actual_cmd.unwrap();
        let cmdline_wrapper = format!("exec {} {}", actual_cmd.to_str().unwrap(), cmdline_remainder);

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

        let job = match cmd {
            Ok(output) => {
                let mut q = q_mutex.lock().unwrap();

                if let Some(signum) = output.status.signal() {
                    info!("[queue runner] Job was killed with signal {}", signum);
                    q.finish(
                        JobState::Killed(signum),
                        String::from_utf8_lossy(&output.stdout).to_string(),
                        String::from_utf8_lossy(&output.stderr).to_string(),
                    )
                } else {
                    let status = output.status.code().unwrap();
                    info!("[queue runner] Job has terminated with code {}", status);
                    q.finish(
                        JobState::Terminated(status),
                        String::from_utf8_lossy(&output.stdout).to_string(),
                        String::from_utf8_lossy(&output.stderr).to_string(),
                    )
                }
            }
            Err(e) => {
                let mut q = q_mutex.lock().unwrap();
                let message = e.description().to_string();
                error!("[queue runner] Failed to launch job: {}", message);
                q.finish(
                    JobState::Failed(message),
                    String::from(""),
                    String::from(""),
                )
            }
        };

        if let Some(j) = job {
            if notify_url.is_some() {
                let id = j.id;
                if let Err(e) = run_notify_command(j, notify_url.as_ref().unwrap()) {
                    error!(
                        "Failed to run notify command for job {}: {}",
                        id,
                        e.description()
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
    appkeys: HashMap<String,PathBuf>,
    notify_url: Option<String>,
) -> Result<()> {

    if !foreground {
        daemonize(pidfile)?;
    }

    let httpd = match spawn_https(tcp_port, cert, key) {
        Ok(s) => s,
        Err(e) => {
            error!("Could not set up listening socket on port {}: {}", tcp_port, e.description());
            panic!("Could not set up listening socket on port {}: {}", tcp_port, e.description())
        }
    };

    daemon::notify(false, [(daemon::STATE_READY, "1" )].iter())?;
    info!("Daemon version {} ready.", crate_version!());
    info!("Application keys available: {:?}", appkeys.keys());

    let job_queue = Arc::new((Mutex::new(JobQueue::new()), Condvar::new()));

    // set up queue runner

    let queue_runner_q = job_queue.clone();
    let queue_runner = thread::Builder::new()
        .name("Queue Runner".to_owned())
        .spawn(move || run_queue(&queue_runner_q, notify_url, appkeys));

    // handle incoming TCP connections
    for request in httpd.incoming_requests() {
        debug!("Request: {:?}", request);

        handle_client(request, &job_queue.clone(), dump_protocol);
    }

    queue_runner.unwrap().join().unwrap();

    Ok(())
}
