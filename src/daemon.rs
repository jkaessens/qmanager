use std::error::Error;
use std::fs::File;
use std::io::{Read, Result};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;

use daemonize::Daemonize;
use native_tls::{Pkcs12, TlsAcceptor, TlsStream};

fn daemonize(pidfile: Option<&str>) -> Result<()> {
    // set up daemonizing
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

fn handle_client(_stream: TlsStream<TcpStream>) {
    println!("TLS Connection established.");
}

pub fn handle(tcp_port: Option<u16>, pidfile: Option<&str>, cert: Option<&str>) -> Result<()> {
    daemonize(pidfile)?;

    let (listener, acceptor) = create_socket(tcp_port.unwrap_or(1337u16), cert.unwrap())?;

    let mut job_queue = Arc::new(Mutex::new(0));

    // handle incoming TCP connections
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client {} connected.", stream.peer_addr().unwrap());
                let acceptor = acceptor.clone();
                let stream = acceptor.accept(stream).unwrap();

                handle_client(stream);
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}
