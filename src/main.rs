#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate clap;
extern crate byteorder;
extern crate daemonize;
extern crate native_tls;
extern crate serde;
extern crate serde_json;

mod clicommands;
mod daemon;
mod job_queue;
mod protocol;

use std::fs::File;
use std::io::prelude::*;
use std::io::{Error, ErrorKind, Result};
use std::net::{TcpStream, ToSocketAddrs};
use std::str::FromStr;

use clap::{App, Arg, SubCommand, Values};
use native_tls::{Certificate, TlsConnector};

use protocol::Stream;

fn connect(
    host: Option<&str>,
    port: Option<u16>,
    ca: Option<Values>,
    ssl: bool,
) -> Result<Box<Stream>> {
    if ssl {
        connect_tls(host, port, ca)
    } else {
        connect_tcp(host, port)
    }
}

fn connect_tcp(host: Option<&str>, port: Option<u16>) -> Result<Box<Stream>> {
    // Set up TCP connection
    let host = host.unwrap_or("localhost");
    let port = port.unwrap_or(1337u16);

    // Resolve IP(s) for given hostname
    let addrs = (host, port).to_socket_addrs()?;

    // Try all addresses until one succeeds
    for addr in addrs {
        if let Ok(s) = TcpStream::connect(addr) {
            return Ok(Box::new(s));
        }
    }

    Err(Error::from(ErrorKind::ConnectionRefused))
}

fn connect_tls(host: Option<&str>, port: Option<u16>, ca: Option<Values>) -> Result<Box<Stream>> {
    // Load CA certificates, if requested

    let mut builder = TlsConnector::builder().unwrap();

    if let Some(values) = ca {
        for v in values {
            // load certificate
            let mut cert = vec![];
            let mut cert_file = File::open(v)?;
            cert_file
                .read_to_end(&mut cert)
                .expect("Failed to read certificate file");

            if let Ok(c) = Certificate::from_pem(&cert) {
                builder.add_root_certificate(c).unwrap();
            }
        }
    }

    let connector = builder.build().unwrap();

    // Set up TCP connection
    let host = host.unwrap_or("localhost");
    let port = port.unwrap_or(1337u16);

    // Resolve IP(s) for given hostname
    let addrs = (host, port).to_socket_addrs()?;

    // Try all addresses until one succeeds
    for addr in addrs {
        let s = TcpStream::connect(addr);

        if let Ok(tcp_stream) = s {
            return Ok(Box::new(
                connector
                    .danger_connect_without_providing_domain_for_certificate_verification_and_server_name_indication(
                        tcp_stream,
                    )
                    .map_err(|_e| Error::from(ErrorKind::ConnectionAborted))?,
            ));
        }
    }

    Err(Error::from(ErrorKind::ConnectionRefused))
}

fn main() -> Result<()> {
    let app = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .arg(
            Arg::with_name("ca")
                .long("ca")
                .help("Set CA certificate")
                .global(true)
                .multiple(true)
                .takes_value(true)
                .conflicts_with("insecure"),
        )
        .arg(
            Arg::with_name("insecure")
                .long("insecure")
                .help("Use plain TCP instead of SSL/TLS")
                .global(true)
                .conflicts_with_all(&["ca", "cert"]),
        )
        .subcommand(
            SubCommand::with_name("daemon")
                .about("Starts the Queue Manager Daemon")
                .arg(
                    Arg::with_name("foreground")
                        .long("foreground")
                        .help("Stay in foreground, do not detach. No pidfile is created.")
                        .conflicts_with("pidfile"),
                )
                .arg(
                    Arg::with_name("cert")
                        .long("cert")
                        .help("Set SSL Certificate")
                        .takes_value(true)
                        .conflicts_with("insecure")
                        .required_unless("insecure"),
                )
                .arg(
                    Arg::with_name("port")
                        .long("port")
                        .help("Set TCP port to listen on")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("pidfile")
                        .long("pidfile")
                        .help("Set PID file location")
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("queue-status")
                .about("Displays the queue status"))
        .subcommand(
            SubCommand::with_name("submit")
                .about("Submits a job to the queue")
                .arg(
                    Arg::with_name("notify-cmd")
                        .long("notify-cmd")
                        .help("Run command after program termination")
                        .takes_value(true),
                )
                .arg(Arg::with_name("cmdline").takes_value(true).required(true)),
        )
        .subcommand(
            SubCommand::with_name("remove")
                .about("Removes a job from the queue or the list of terminated jobs. Does not work for jobs that are currently running")
                .arg(Arg::with_name("jobid")
                    .help("Job ID to retrieve and remove from the status list")
                    .takes_value(true)
                    .required(true),
            ),
        )
        .subcommand(
            SubCommand::with_name("kill")
                .about("Sends the SIGTERM signal to the job's process. Only works for jobs that are currently running.")
                .arg(Arg::with_name("jobid")
                    .help("Job ID to send SIGTERM signal to. Note that this depends on the good manners of the process, it is not guaranteed that the job is actually terminated.")
                    .takes_value(true)
                    .required(true),
            ),
        )
        .get_matches();

    match app.subcommand_name() {
        Some("daemon") => {
            let matches = app.subcommand_matches("daemon").unwrap();
            daemon::handle(
                matches
                    .value_of("port")
                    .map(|p| FromStr::from_str(p).unwrap()),
                matches.value_of("pidfile"),
                matches.value_of("cert"),
                matches.occurrences_of("foreground") > 0,
            )
        }
        Some("queue-status") => {
            let matches = app.subcommand_matches("queue-status").unwrap();
            clicommands::handle_queue_status(connect(
                matches.value_of("host"),
                matches
                    .value_of("port")
                    .map(|p| FromStr::from_str(p).unwrap()),
                matches.values_of("ca"),
                !matches.is_present("insecure"),
            )?)
        }
        Some("submit") => {
            let matches = app.subcommand_matches("submit").unwrap();
            clicommands::handle_submit(
                connect(
                    matches.value_of("host"),
                    matches
                        .value_of("port")
                        .map(|p| FromStr::from_str(p).unwrap()),
                    matches.values_of("ca"),
                    !matches.is_present("insecure"),
                )?,
                matches.value_of("cmdline").unwrap(),
                matches.value_of("notify-cmd"),
            )
        }
        Some("remove") => {
            let matches = app.subcommand_matches("remove").unwrap();
            let result = clicommands::handle_remove(
                connect(
                    matches.value_of("host"),
                    matches
                        .value_of("port")
                        .map(|p| FromStr::from_str(p).unwrap()),
                    matches.values_of("ca"),
                    !matches.is_present("insecure"),
                )?,
                matches.value_of("jobid").unwrap(),
            );
            result.and_then(|job| {
                println!("{:?}", job);
                Ok(())
            })
        },
        Some("kill") => {
            let matches = app.subcommand_matches("kill").unwrap();
            let result = clicommands::handle_kill(
                connect(
                    matches.value_of("host"),
                    matches
                        .value_of("port")
                        .map(|p| FromStr::from_str(p).unwrap()),
                    matches.values_of("ca"),
                    !matches.is_present("insecure"),
                )?,
                matches.value_of("jobid").unwrap(),
            );
            result.and_then(|job| {
                println!("{:?}", job);
                Ok(())
            })
        }
        _ => {
            eprintln!("Please specify a valid subcommand!");
            Ok(())
        }
    }
}
