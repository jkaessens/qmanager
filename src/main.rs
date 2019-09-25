#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate clap;

#[macro_use]
extern crate log;
extern crate syslog;

extern crate daemonize;
extern crate serde;
extern crate serde_json;
extern crate reqwest;
extern crate tiny_http;
extern crate nix;
extern crate structopt;
extern crate systemd;


mod clicommands;
mod daemon;
mod job_queue;
mod protocol;

use std::fs::File;
use std::io::prelude::*;
use std::io::Result;
use std::path::PathBuf;
use std::str::FromStr;
use syslog::{Facility};
use structopt::StructOpt;
use reqwest::{Client, Url};

#[derive(Debug, StructOpt)]
#[structopt(name=crate_name!(), version=crate_version!(), author=crate_authors!(), about=crate_description!())]
struct Opt {
    /// Set CA certificate
    #[structopt(long,parse(from_os_str),conflicts_with("insecure"),required_unless("insecure"))]
    ca: Option<PathBuf>,

    /// Use plain TCP instead of SSL/TLS
    #[structopt(long,conflicts_with("ca"),required_unless("insecure"))]
    insecure: bool,

    /// For clients, the host name to connect to. For servers ignored
    #[structopt(long,default_value="localhost")]
    host: String,

    /// For clients, the port to connect to. For servers, the port to listen on
    #[structopt(long,default_value="1337")]
    port: u16,

    #[structopt(long)]
    /// Dump client requests and responses to stdout
    dump_json: bool,

    #[structopt(long,possible_values = &["Error","Warn","Info","Debug"],default_value="Info")]
    /// The log level
    loglevel: String,

    #[structopt(subcommand)]
    cmd: OptCommand
}

#[derive(Debug,StructOpt)]
enum OptCommand {
    /// Starts the qmanager daemon
    Daemon {
        /// Stays in foreground, does not detach. Pidfile argument is ignored
        #[structopt(long)]
        foreground: bool,

        /// Certificate file for SSL/TLS operation
        #[structopt(long,parse(from_os_str))]
        cert: Option<PathBuf>,

        /// Key for SSL/TLS certificate
        #[structopt(long,parse(from_os_str))]
        key: Option<PathBuf>,

        /// PID file location
        #[structopt(long,parse(from_os_str))]
        pidfile: Option<PathBuf>,
    },

    /// Requests the queue status
    QueueStatus {

    },

    /// Submits a job to the queue
    Submit {
        /// Run command after program termination
        #[structopt(long)]
        notify_cmd: Option<String>,
        #[structopt(name = "CMDLINE", parse(from_str))]
        cmdline: String
    },

    /// Removes a finished job from the queue
    Remove {
        /// Job ID to remove from the 'finished' queue
        #[structopt(long)]
        job_id: u64,
    },

    /// Asks a running job to terminate
    Kill {
        /// Job ID to terminate
        #[structopt(long)]
        job_id: u64,
    }
}

/// Reads a whole file into a byte vector
fn slurp_file(filename: &PathBuf) -> Result<Vec<u8>> {
    let mut f = File::open(filename)?;
    let mut buf = Vec::new();

    f.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Loads SSL certificates, if any, and sets up corresponding Client and Url objects
fn create_client(insecure: bool, ca: Option<PathBuf>, host: &str, port: u16) -> Result<(Client, Url)> {
    let client = if insecure {
        reqwest::Client::new()
    } else {
        let mut buf = Vec::new();
        File::open(ca.unwrap())?.read_to_end(&mut buf).unwrap();
        let pkcs12 = reqwest::Certificate::from_pem(&buf).unwrap();
        reqwest::Client::builder()
            .add_root_certificate(pkcs12)
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build().unwrap()
    };

    let url = reqwest::Url::parse(&format!("http://{}:{}/", host, port)).unwrap();

    Ok((client, url))
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    syslog::init(Facility::LOG_DAEMON,
                 log::LevelFilter::from_str(&opt.loglevel).expect("Failed to parse log level!"),
                 Some(crate_name!())).expect("Failed to connect to syslog!");

    match opt.cmd {
        OptCommand::Daemon {cert, key, pidfile, foreground}=> {
            let cert = cert.and_then(|s| Some(slurp_file(&s))).transpose()?;
            let key = key.and_then(|s| Some(slurp_file(&s))).transpose()?;

            daemon::handle(opt.port, pidfile, cert, key, foreground, opt.dump_json)

        },

        OptCommand::QueueStatus {} => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_queue_status(&client, url, opt.dump_json)
        },

        OptCommand::Submit {notify_cmd, cmdline} => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_submit(&client, url, &cmdline, notify_cmd, opt.dump_json)
        },

        OptCommand::Remove {job_id} => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_remove(&client, url, job_id, opt.dump_json)
                .and_then(|job| { println!("{:?}", job); Ok(()) } )
        },

        OptCommand::Kill {job_id} => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_kill(&client, url, job_id, opt.dump_json)
                .and_then(|job| { println!("{:?}", job); Ok(()) } )
        }
    }


}
