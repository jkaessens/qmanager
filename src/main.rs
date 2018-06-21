#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate clap;
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
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::str::FromStr;

use clap::{App, Arg, SubCommand, Values};
use native_tls::{Certificate, TlsConnector, TlsStream};

fn connect(
    host: Option<&str>,
    port: Option<u16>,
    ca: Option<Values>,
) -> Result<TlsStream<TcpStream>> {
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

    let host = host.unwrap_or("localhost");
    let port = port.unwrap_or(1337u16);

    let addrs = (host, port).to_socket_addrs()?;

    let mut s = Err(Error::from(ErrorKind::Other));

    for addr in addrs {
        s = TcpStream::connect(addr);
        if let Ok(tcp_stream) = s {
            tcp_stream.set_nodelay(true)?;
            return connector
            //                .connect(host, tcp_stream)
                .danger_connect_without_providing_domain_for_certificate_verification_and_server_name_indication(tcp_stream)
                .map_err(|_e| Error::from(ErrorKind::ConnectionAborted));
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
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("daemon")
                .about("Starts the Queue Manager Daemon")
                .arg(
                    Arg::with_name("cert")
                        .long("cert")
                        .help("Set SSL Certificate")
                        .takes_value(true)
                        .required(true),
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
        .subcommand(SubCommand::with_name("queue-status"))
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
            )?)
        }
        _ => {
            eprintln!("Please specify a valid subcommand!");
            Ok(())
        }
    }
}
