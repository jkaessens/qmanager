use std::io::prelude::*;
use std::io::Result;
use std::net::TcpStream;

use clap::ArgMatches;
use native_tls::TlsStream;
use serde_json;

use job_queue::*;
use protocol::{Request, Response};

fn print_jobs(header: &str, jobs: Vec<Job>) {
    println!("{}", header);
    for j in jobs {
        println!("{:?}", j);
    }
}

pub fn handle_queue_status(mut stream: TlsStream<TcpStream>) -> Result<()> {
    eprintln!("TLS Handshake completed! Sending request...");

    let s = serde_json::to_string_pretty(&Request::GetQueuedJobs).unwrap();
    eprintln!("JSON Request: {}", s);
    stream.write(s.as_bytes());
    stream.flush();
    //    serde_json::to_writer(&mut stream, &Request::GetQueuedJobs)?;

    eprintln!("Awaiting response...");

    let response = serde_json::from_reader(&mut stream)?;

    match response {
        Response::GetJobs(jobs) => print_jobs("QUEUED JOBS", jobs),
        Response::Error(s) => {
            eprintln!("Could not get queued jobs: {}", s);
        }
        _ => {
            panic!("Unexpected response: {:?}", response);
        }
    }

    eprintln!("Sending request for finished jobs");

    serde_json::to_writer(&mut stream, &Request::GetFinishedJobs)?;
    stream.flush();

    eprintln!("Awaiting response");

    let response = serde_json::from_reader(&mut stream)?;

    match response {
        Response::GetJobs(jobs) => print_jobs("FINISHED JOBS", jobs),
        Response::Error(s) => {
            eprintln!("Could not get finished jobs: {}", s);
        }
        _ => {
            panic!("Unexpected response: {:?}", response);
        }
    }
    stream.flush();

    Ok(())
}
