use std::fs::File;
use std::io::{Read, Result, ErrorKind, Write};
use std::error::Error;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::process::{Command, Stdio};
use std::os::unix::process::ExitStatusExt;

use daemonize::Daemonize;
use native_tls::{Pkcs12, TlsAcceptor};
use serde_json;

use job_queue::{JobQueue, JobState, Job};
use protocol::{Request, Response, read_and_decode, encode_and_write, Stream};


fn daemonize(pidfile: Option<&str>) -> Result<()> {
    let logfile = File::create("/var/log/qmanager.log").unwrap();
    let errfile = File::create("/var/log/qmanager.errors").unwrap();
    let pidfile = pidfile.unwrap_or("/var/run/user/1000/qmanager.pid");

    if let Err(e) = Daemonize::new()
        .pid_file(pidfile)
        .stdout(logfile)
        .stderr(errfile)
        .start()
    {
        eprintln!("Failed to daemonize: {}", e);
    }
    Ok(())
}

fn create_tcp_socket(tcp_port: u16) -> Result<TcpListener> {
    let bind_address = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], tcp_port));
    let listener = TcpListener::bind(bind_address)?;
    Ok(listener)
}

fn create_tls_acceptor(certfile: &str) -> Result<TlsAcceptor> {

    // load server certificate bundle
    let mut pkcs12 = vec![];
    let mut cert_file = File::open(certfile)?;
    cert_file
        .read_to_end(&mut pkcs12)
        .expect("Failed to read certificate file");

    let pkcs12 = Pkcs12::from_der(&pkcs12, "cl057").expect("Failed to decode certificate");

    let acceptor = TlsAcceptor::builder(pkcs12).unwrap().build().unwrap();
    Ok(acceptor)
}

fn handle_client<T: Stream>(
    mut stream: T,
    q_mutex: &Arc<(Mutex<JobQueue>, Condvar)>,
) -> Result<()> {

    let (ref q_mutex, ref cvar) = **q_mutex;

    loop {
        let s = read_and_decode(&mut stream);
        if let Err(e) = s {
            if e.kind() == ErrorKind::UnexpectedEof {
                println!("[handle_client] Disconnected");
            } else {
                eprintln!("[handle_client] Disconnected: {:?}", e);
            }
            break;
        }
        let request = serde_json::from_str(&s.unwrap())?;

        println!("[handle_client] Got request: {:?}", request);

        match request {
            Request::GetQueuedJobs => {
                let q = q_mutex.lock().unwrap();
                let items = q.iter_queued().cloned().collect();
                encode_and_write(
                    &serde_json::to_string_pretty(&Response::GetJobs(items))?,
                    &mut stream,
                )?;
            }
            Request::GetFinishedJobs => {
                let q = q_mutex.lock().unwrap();
                let items = q.iter_finished().cloned().collect();
                encode_and_write(
                    &serde_json::to_string_pretty(&Response::GetJobs(items))?,
                    &mut stream,
                )?;
            }

            Request::ReapJob(id) => {
                let mut q = q_mutex.lock().unwrap();
                match q.remove_finished(id) {
                    Ok(job) => {
                        encode_and_write(
                            &serde_json::to_string_pretty(&Response::GetJob(job))?,
                            &mut stream,
                        )?;
                    }
                    _ => {
                        encode_and_write(
                            &serde_json::to_string_pretty(&Response::Error(
                                "No such job".to_string(),
                            ))?,
                            &mut stream,
                        )?;
                    }
                }
            }

            Request::SubmitJob(cmdline, expected_duration, notify_email) => {
                let mut q = q_mutex.lock().unwrap();
                let id = q.submit(cmdline, expected_duration, notify_email);
                cvar.notify_one();
                encode_and_write(&serde_json::to_string_pretty(&Response::SubmitJob(id))?, &mut stream)?;
            }
        }
    }
    Ok(())
}

fn run_notify_command(job: Job) -> Result<()> {
    let body = serde_json::to_string_pretty(&job).unwrap();
    let notify_cmd = job.notify_cmd.unwrap();
    Command::new("sh")
        .arg("-c")
        .arg(&notify_cmd)
        .stdin(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child.stdin.as_mut().unwrap().write_all(body.as_bytes())
        })
}

fn run_queue(q_mutex: &Arc<(Mutex<JobQueue>, Condvar)>) {
    let (ref q_mutex, ref cvar) = **q_mutex;

    println!("[queue runner] Now active");

    loop {
        let mut job: Option<Job> = None;

        // acquire a new job to run
        while job.is_none() {
            let mut q = q_mutex.lock().unwrap();

            if let Some(j) = q.schedule() {
                job = Some(j);
                break;
            } else {
                println!("[queue runner] Falling asleep");
                q = cvar.wait(q).unwrap();
                println!("[queue runner] Woke up");
                job = q.schedule();
            }
        }


        let job = job.unwrap();
        println!("[queue runner] Running job {}", job.id);

        let cmd = Command::new("sh")
            .arg("-c")
            .arg(&job.cmdline)
            .current_dir("/")
            .output();

        let job = match cmd {
            Ok(output) => {

                let mut q = q_mutex.lock().unwrap();

                if let Some(signum) = output.status.signal() {
                    println!("[queue runner] Job was killed with signal {}", signum);
                    q.finish(JobState::Killed(signum),
                             String::from_utf8_lossy(&output.stdout).to_string(),
                             String::from_utf8_lossy(&output.stderr).to_string(),
                    )
                } else {
                    let status = output.status.code().unwrap();
                    println!("[queue runner] Job has terminated with code {}", status);
                    q.finish(JobState::Terminated(status),
                             String::from_utf8_lossy(&output.stdout).to_string(),
                             String::from_utf8_lossy(&output.stderr).to_string(),
                    )
                }
            },
            Err(e) => {
                let mut q = q_mutex.lock().unwrap();
                let message = e.description().to_string();
                println!("[queue runner] Failed to launch job: {}", message);
                q.finish(JobState::Failed(message), String::from(""), String::from(""))
            }
        };

        if let Some(j) = job {
            if j.notify_cmd.is_some() {
                let id = j.id;
                if let Err(e) = run_notify_command(j) {
                    eprintln!(
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
    tcp_port: Option<u16>,
    pidfile: Option<&str>,
    cert: Option<&str>,
    foreground: bool,
) -> Result<()> {
    if !foreground {
        daemonize(pidfile)?;
    }

    let listener = create_tcp_socket(tcp_port.unwrap_or(1337u16))?;

    // set up TLS certificates if requested
    let tls_acceptor: Option<TlsAcceptor> = if let Some(c) = cert {
        Some(create_tls_acceptor(c)?)
    } else {
        None
    };

    let job_queue = Arc::new((Mutex::new(JobQueue::new()), Condvar::new()));

    // set up queue runner

    let queue_runner_q = job_queue.clone();
    let queue_runner = thread::Builder::new()
        .name("Queue Runner".to_owned())
        .spawn(move || run_queue(&queue_runner_q));

    // handle incoming TCP connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client {} connected.", stream.peer_addr().unwrap());
                match tls_acceptor {
                    Some(ref tls) => handle_client(tls.clone().accept(stream).expect("TLS handshake failed"), &job_queue.clone())?,
                    None => handle_client(stream, &job_queue.clone())?
                }

            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    queue_runner.unwrap().join().unwrap();

    Ok(())
}
