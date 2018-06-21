use std::fs::File;
use std::io::prelude::*;
use std::io::{Read, Result};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use daemonize::Daemonize;
use native_tls::{Pkcs12, TlsAcceptor, TlsStream};
use serde_json;

use job_queue::{Job, JobQueue, JobState};
use protocol::{Request, Response};

const MAX_RECV_BUFFER_SIZE: usize = 4096;

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

    // load certificate
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
    q_mutex: Arc<(Mutex<JobQueue>, Condvar)>,
) -> Result<()> {
    let &(ref q_mutex, ref cvar) = &*q_mutex;

    eprintln!("TLS Handshake completed!");

    let mut v = vec![0; 1];
    let mut s = String::new();
    while let Ok(size) = stream.read(&mut v) {
        s.push(v[0] as char);
        eprintln!("Got {} bytes: {:?} (now: {})", size, v, s);
    }

    //   let mut v = vec![0; 20];
    //    eprintln!("Got {} bytes from stream.", stream.read(&mut v)?);

    //let request = serde_json::from_reader(&mut stream)?;
    let request = serde_json::from_str(&s)?;

    eprintln!("Got request: {:?}", request);

    match request {
        Request::GetQueuedJobs => {
            let q = q_mutex.lock().unwrap();
            let items: Vec<Job> = q.iter_queued().map(|j| j.clone()).collect();
            eprintln!("About to send queued job list!");
            serde_json::to_writer(&mut stream, &Response::GetJobs(items))?;
            eprintln!("done!");
        }
        Request::GetFinishedJobs => {
            let q = q_mutex.lock().unwrap();
            let items: Vec<Job> = q.iter_finished().map(|j| j.clone()).collect();
            eprintln!("About to send finished job list!");
            serde_json::to_writer(&mut stream, &Response::GetJobs(items))?;
            eprintln!("done!");
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
            serde_json::to_writer(&mut stream, &Response::SubmitJob(id))?;
        }
    }
    stream.flush();
    Ok(())
}

fn run_queue(q_mutex: Arc<(Mutex<JobQueue>, Condvar)>) {
    let &(ref q_mutex, ref cvar) = &*q_mutex;

    eprintln!("Queue Runner active.");

    loop {
        let mut job_cmdline: Option<String> = None;

        // acquire a new job to run
        while job_cmdline.is_none() {
            let mut q = q_mutex.lock().unwrap();

            if let Some(s) = q.schedule() {
                job_cmdline = Some(s);
                eprintln!("Job scheduled!");
                break;
            } else {
                eprintln!("No job there. Waiting...");
                q = cvar.wait(q).unwrap();
                eprintln!("Woke up! Getting a job.");
                job_cmdline = q.schedule();
            }
        }

        println!("Running {}...", job_cmdline.unwrap());

        // put executed job into finished queue
        q_mutex.lock().unwrap().finish(0, JobState::Terminated);
    }
}

pub fn handle(tcp_port: Option<u16>, pidfile: Option<&str>, cert: Option<&str>) -> Result<()> {
    daemonize(pidfile)?;

    let (listener, acceptor) = create_socket(tcp_port.unwrap_or(1337u16), cert.unwrap())?;

    let mut job_queue = Arc::new((Mutex::new(JobQueue::new()), Condvar::new()));

    // set up queue runner

    let queue_runner_q = job_queue.clone();
    let queue_runner = thread::Builder::new()
        .name("Queue Runner".to_owned())
        .spawn(move || run_queue(queue_runner_q));

    // handle incoming TCP connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client {} connected.", stream.peer_addr().unwrap());
                let acceptor = acceptor.clone();
                match acceptor.accept(stream) {
                    Ok(stream) => handle_client(stream, job_queue.clone())?,
                    Err(e) => eprintln!("Connection failed: {}", e),
                }
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    queue_runner.unwrap().join();

    Ok(())
}
