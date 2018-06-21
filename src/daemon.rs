use std::fs::File;
use std::io::{Read, Result};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use daemonize::Daemonize;
use native_tls::{Pkcs12, TlsAcceptor, TlsStream};
use serde_json;

use job_queue::{JobQueue, JobState};
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

    println!("TLS handshake completed.");

    loop {
        let s = read_and_decode(&mut stream);
        if let Err(e) = s {
            println!("Client disconnected: {:?}", e);
            break;
        }
        let request = serde_json::from_str(&s.unwrap())?;

        eprintln!("Got request: {:?}", request);

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
