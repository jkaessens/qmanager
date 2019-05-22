#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate clap;
extern crate byteorder;
extern crate daemonize;
extern crate native_tls;
extern crate serde;
extern crate serde_json;

extern crate reqwest;
extern crate tiny_http;

extern crate nix;

mod clicommands;
mod daemon;
mod job_queue;
mod protocol;
mod transport;

use std::fs::File;
use std::io::prelude::*;
use std::io::Result;
use std::str::FromStr;

use clap::{App, Arg, SubCommand};


fn slurp_file(filename: &str) -> Result<Vec<u8>> {
    let mut f = File::open(filename)?;
    let mut buf = Vec::new();

    f.read_to_end(&mut buf)?;
    Ok(buf)
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
        .arg(
            Arg::with_name("dump-json")
                .long("dump-json")
                .help("Dump client requests and responses to stdout")
                .global(true)
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
                        .help("Set SSL certificate")
                        .takes_value(true)
                        .conflicts_with("insecure")
                        .required_unless("insecure")
                        .requires("key"),
                )
                .arg(
                    Arg::with_name("key")
                        .long("key")
                        .help("Private key for SSL certificate")
                        .takes_value(true)
                        .conflicts_with("insecure")
                        .requires("cert")
                        .required_unless("insecure")
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

    let subcommand = app.subcommand_name().unwrap_or_else(|| panic!("Please specify a valid subcommand!"));

    if subcommand == "daemon" {
            let matches = app.subcommand_matches("daemon").unwrap();

            let cert = if let Some(fname) = matches.value_of("cert") {
                Some(slurp_file(fname)?)
            }  else {
                None
            };

            let key = if let Some(fname) = matches.value_of("key") {
                Some(slurp_file(fname)?)
            }  else {
                None
            };

            daemon::handle(
                matches
                    .value_of("port")
                    .map(|p| FromStr::from_str(p).unwrap()),
                matches.value_of("pidfile"),
                cert,
                key,
                matches.is_present("foreground"),
                matches.is_present("dump-json")
            )
        }
    else {

        let client = if app.is_present("insecure") {
            reqwest::Client::new()
        } else {
            let mut buf = Vec::new();
            File::open(app.value_of("ca").unwrap())?.read_to_end(&mut buf).unwrap();
            let pkcs12 = reqwest::Certificate::from_pem(&buf).unwrap();
            reqwest::Client::builder()
                .add_root_certificate(pkcs12)
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build().unwrap()
        };

        let url = if app.is_present("insecure") {
            reqwest::Url::parse(&format!("http://{}:{}/",
                                         app.value_of("host").unwrap_or("localhost"),
                                         app.value_of("port").unwrap_or("1337")))
                .unwrap()
        } else {
            reqwest::Url::parse(&format!("http://{}:{}/",
                                         app.value_of("host").unwrap_or("localhost"),
                                         app.value_of("port").unwrap_or("1337")))
                .unwrap()
        };

        match subcommand {
            "queue-status" => {
                let matches = app.subcommand_matches("queue-status").unwrap();
                clicommands::handle_queue_status(&client, url,
                                                 matches.is_present("dump-json"))
            }
            "submit" => {
                let matches = app.subcommand_matches("submit").unwrap();
                clicommands::handle_submit(&client, url,
                    matches.value_of("cmdline").unwrap(),
                    matches.value_of("notify-cmd"),
                    matches.is_present("dump-json")
                )
            }
            "remove" => {
                let matches = app.subcommand_matches("remove").unwrap();
                let result = clicommands::handle_remove(& client, url,
                    matches.value_of("jobid").unwrap(),
                    matches.is_present("dump-json")
                );
                result.and_then(|job| {
                    println!("{:?}", job);
                    Ok(())
                })
            },
            "kill" => {
                let matches = app.subcommand_matches("kill").unwrap();
                let result = clicommands::handle_kill(
                    & client, url,
                    matches.value_of("jobid").unwrap(),
                    matches.is_present("dump-json")
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
        } // match
    } // if daemon
}
