#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate config;
extern crate daemonize;
extern crate nix;
extern crate reqwest;
extern crate serde;
extern crate serde_json;
extern crate signal_hook;
extern crate simplelog;
extern crate structopt;
extern crate syslog;
extern crate systemd;
extern crate tiny_http;

mod clicommands;
mod cliopts;
mod daemon;
mod job_queue;
mod protocol;
mod state;

use std::fs::File;
use std::io::prelude::*;
use std::io::Result;
use std::path::PathBuf;
use std::str::FromStr;

use cliopts::*;
use job_queue::QueueState;
use state::State;

use reqwest::{Client, Url};
use structopt::StructOpt;
use syslog::Facility;

/// Reads a whole file into a byte vector
fn slurp_file(filename: &PathBuf) -> Result<Vec<u8>> {
    let mut f = File::open(filename)?;
    let mut buf = Vec::new();

    f.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Loads SSL certificates, if any, and sets up corresponding Client and Url objects
fn create_client(
    insecure: bool,
    ca: Option<PathBuf>,
    host: &str,
    port: u16,
) -> Result<(Client, Url)> {
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
            .build()
            .unwrap()
    };

    let url = reqwest::Url::parse(&format!("http://{}:{}/", host, port)).unwrap();

    Ok((client, url))
}

fn main() -> Result<()> {
    // Load command line args add config defaults for those not specified
    let mut opt = Opt::from_args();
    let mut config = config::Config::default();
    config
        .merge(config::File::new(
            opt.config.to_str().unwrap(),
            config::FileFormat::Toml,
        ))
        .expect("Failed to read configuration file!");
    opt.merge_config(config);

    // Check general option usefulness
    opt.verify()?;

    // Do mode-specific checking and set up logging
    if let OptCommand::Daemon { .. } = &opt.cmd {
        // Check if appkey executables are actually existing
        for (k, v) in &opt.appkeys {
            if !v.exists() {
                error!("Appkey '{}' points to non-existent file '{:#?}'", k, v);
            }

            debug!("Registered appkey '{}' => '{:#?}'", k, v);
        }

        // Set up syslog daemon
        syslog::init(
            Facility::LOG_DAEMON,
            log::LevelFilter::from_str(&opt.loglevel).expect("Failed to parse log level!"),
            Some(crate_name!()),
        )
        .expect("Failed to connect to syslog!");
    } else {
        // Set up terminal logging
        simplelog::TermLogger::init(
            simplelog::LevelFilter::from_str(&opt.loglevel).expect("Failed to parse log level!"),
            simplelog::ConfigBuilder::new()
                .add_filter_allow_str(module_path!())
                .build(),
            simplelog::TerminalMode::Stderr,
        )
        .unwrap();
    }

    // Set up program state configuration file
    let state = State::from(opt.state_file.unwrap());

    // Handle subcommands
    match opt.cmd {
        OptCommand::Daemon {
            cert,
            key,
            pidfile,
            foreground,
            notify_url,
        } => {
            let cert = cert.and_then(|s| Some(slurp_file(&s))).transpose()?;
            let key = key.and_then(|s| Some(slurp_file(&s))).transpose()?;

            daemon::handle(
                opt.port,
                pidfile,
                cert,
                key,
                foreground,
                opt.dump_json,
                opt.appkeys,
                notify_url,
                state,
            )
        }

        OptCommand::Stop {} => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_set_queue_status(&client, url, QueueState::Stopping, opt.dump_json)
        }
        OptCommand::Start {} => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_set_queue_status(&client, url, QueueState::Running, opt.dump_json)
        }
        OptCommand::Status {} => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_queue_status(&client, url, opt.dump_json)
        }

        OptCommand::Submit { cmdline } => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_submit(&client, url, &cmdline, opt.dump_json)
        }

        OptCommand::Remove { job_id } => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_remove(&client, url, job_id, opt.dump_json).and_then(|job| {
                println!("{:?}", job);
                Ok(())
            })
        }

        OptCommand::Kill { job_id } => {
            let (client, url) = create_client(opt.insecure, opt.ca, &opt.host, opt.port)?;
            clicommands::handle_kill(&client, url, job_id, opt.dump_json).and_then(|job| {
                println!("{:?}", job);
                Ok(())
            })
        }
    }
}
