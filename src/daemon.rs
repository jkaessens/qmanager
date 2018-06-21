use std::fs::File;
use std::io::{Read, Result, ErrorKind};
use std::error::Error;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::process::Command;
use std::os::unix::process::ExitStatusExt;

use daemonize::Daemonize;
use native_tls::{Pkcs12, TlsAcceptor, TlsStream};
use serde_json;

use job_queue::{JobQueue, JobState, Job};
use protocol::{Request, Response, read_and_decode, encode_and_write};


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

fn create_socket(tcp_port: u16, certfile: &str) -> Result<(TcpListener, TlsAcceptor)> {
    let bind_address = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], tcp_port));
    let listener = TcpListener::bind(bind_address)?;

    // load server certificate bundle
    let mut pkcs12 = vec![];
    let mut cert_file = File::open(certfile)?;
    cert_file
        .read_to_end(&mut pkcs12)
        .expect("Failed to read certificate file");

    let pkcs12 = Pkcs12::from_der(&pkcs12, "cl057").expect("Failed to decode certificate");

    let acceptor = TlsAcceptor::builder(pkcs12).unwrap().build().unwrap();
    Ok((listener, acceptor))
}

fn handle_client(
    mut stream: TlsStream<TcpStream>,
    q_mutex: &Arc<(Mutex<JobQueue>, Condvar)>,
) -> Result<()> {

    let (ref q_mutex, ref cvar) = **q_mutex;

    println!("[handle_client] TLS handshake completed.");

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
                encode_and_write(&serde_json::to_string_pretty(&Response::GetJobs(items))?, &mut stream)?;
            }
            Request::GetFinishedJobs => {
                let q = q_mutex.lock().unwrap();
                let items = q.iter_finished().cloned().collect();
                encode_and_write(&serde_json::to_string_pretty(&Response::GetJobs(items))?, &mut stream)?;
            }

            Request::ReapJob(id) => {
                let mut q = q_mutex.lock().unwrap();
                match q.remove_finished(id) {
                    Ok(()) => {
                        serde_json::to_writer(&mut stream, &Response::Ok)?;
                    }
                    _ => {
                        serde_json::to_writer(
                            &mut stream,
                            &Response::Error("No such job".to_string()),
                        )?;
                    }
                }
            }

            Request::SubmitJob(cmdline, expected_duration) => {
                let mut q = q_mutex.lock().unwrap();
                let id = q.submit(cmdline, expected_duration);
                cvar.notify_one();
                encode_and_write(&serde_json::to_string_pretty(&Response::SubmitJob(id))?, &mut stream)?;
            }
        }
    }
    Ok(())
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

        match cmd {
            Ok(output) => {

                let mut q = q_mutex.lock().unwrap();

                if let Some(signum) = output.status.signal() {
                    println!("[queue runner] Job was killed with signal {}", signum);
                    q.finish(JobState::Killed(signum),
                             String::from_utf8_lossy(&output.stdout).to_string(),
                             String::from_utf8_lossy(&output.stderr).to_string(),
                    );
                } else {
                    let status = output.status.code().unwrap();
                    println!("[queue runner] Job has terminated with code {}", status);
                    q.finish(JobState::Terminated(status),
                             String::from_utf8_lossy(&output.stdout).to_string(),
                             String::from_utf8_lossy(&output.stderr).to_string(),
                    );
                }
            },
            Err(e) => {
                let mut q = q_mutex.lock().unwrap();
                let message = e.description().to_string();
                println!("[queue runner] Failed to launch job: {}", message);
                q.finish(JobState::Failed(message), String::from(""), String::from(""));
            }
        }
    }
}

pub fn handle(tcp_port: Option<u16>, pidfile: Option<&str>, cert: Option<&str>) -> Result<()> {
    daemonize(pidfile)?;

    let (listener, acceptor) = create_socket(tcp_port.unwrap_or(1337u16), cert.unwrap())?;

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
                let acceptor = acceptor.clone();
                match acceptor.accept(stream) {
                    Ok(stream) => handle_client(stream, &job_queue.clone())?,
                    Err(e) => eprintln!("Connection failed: {}", e),
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
