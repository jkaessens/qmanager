#[macro_use]
extern crate serde_derive;

extern crate clap;
extern crate daemonize;
extern crate native_tls;
extern crate serde;
extern crate serde_json;

mod daemon;
mod queue_status;

mod job_queue;

use clap::{App, Arg, SubCommand};
use std::io::Result;
use std::str::FromStr;

fn main() -> Result<()> {
    let app = App::new("Queue Manager")
        .version("1.0")
        .author("Jan Christian Kässens <j.kaessens@ikmb.uni-kiel.de>")
        .about("Manages work queues")
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
        Some("queue-status") => queue_status::handle(),
        _ => {
            println!("Please specify a valid subcommand!");
            Ok(())
        }
    }
}
