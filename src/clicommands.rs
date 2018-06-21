
use std::io::Result;
use std::net::TcpStream;
use std::time::Duration;
use std::error::Error;

use native_tls::TlsStream;
use serde_json;

use job_queue::*;
use protocol::{read_and_decode, encode_and_write, Request, Response};

fn print_jobs(header: &str, jobs: Vec<Job>) {
    println!("{}", header);
    for j in jobs {
        println!("{:?}", j);
    }
}

pub fn handle_submit(mut stream: TlsStream<TcpStream>, cmdline: &str, duration: Option<&str>) -> Result<()> {

    let seconds = match duration.unwrap_or("0").parse::<u64>() {
        Ok(duration) => duration,
        Err(e) => {
            eprintln!("Failed to pase duration: {}, using 0 instead.", e.description());
            0
        }
    };

    encode_and_write(
        &serde_json::to_string_pretty(
            &Request::SubmitJob(
                cmdline.to_string(),
                Some(Duration::from_secs(seconds))
            )
        )?,
        &mut stream)?;

    Ok(())
}

pub fn handle_queue_status(mut stream: TlsStream<TcpStream>) -> Result<()> {
    eprintln!("TLS Handshake completed! Sending request...");

    let s = serde_json::to_string_pretty(&Request::GetQueuedJobs).unwrap();
    encode_and_write(&s, &mut stream)?;

    let response = serde_json::from_str(&read_and_decode(&mut stream)?)?;

    match response {
        Response::GetJobs(jobs) => print_jobs("QUEUED JOBS", jobs),
        Response::Error(s) => {
            eprintln!("Could not get queued jobs: {}", s);
        }
        _ => {
            panic!("Unexpected response: {:?}", response);
        }
    }


    encode_and_write(&serde_json::to_string_pretty(&Request::GetFinishedJobs)?, &mut stream)?;

    let response = serde_json::from_str(&read_and_decode(&mut stream)?)?;

    match response {
        Response::GetJobs(jobs) => print_jobs("FINISHED JOBS", jobs),
        Response::Error(s) => {
            eprintln!("Could not get finished jobs: {}", s);
        }
        _ => {
            panic!("Unexpected response: {:?}", response);
        }
    }

    Ok(())
}
