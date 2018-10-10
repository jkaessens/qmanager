use std::error::Error;
use std::io::Result;
use std::time::Duration;

use serde_json;

use job_queue::*;
use protocol::{encode_and_write, read_and_decode, Request, Response, Stream};

fn print_jobs(header: &str, jobs: Vec<Job>) {
    println!("{}", header);
    for j in jobs {
        println!("{:?}", j);
    }
}

pub fn handle_submit<T: Stream>(
    mut stream: T,
    cmdline: &str,
    duration: Option<&str>,
    email: Option<&str>,
) -> Result<()> {
    let seconds = match duration.unwrap_or("0").parse::<u64>() {
        Ok(duration) => duration,
        Err(e) => {
            eprintln!(
                "Failed to pase duration: {}, using 0 instead.",
                e.description()
            );
            0
        }
    };

    encode_and_write(
        &serde_json::to_string_pretty(&Request::SubmitJob(
            cmdline.to_string(),
            Some(Duration::from_secs(seconds)),
            email.map(|s| s.to_string()),
        ))?,
        &mut stream,
    )?;

    // Daemon returns associated Job ID
    let response = serde_json::from_str(&read_and_decode(&mut stream)?)?;
    match response {
        Response::SubmitJob(id) => println!("Submitted as job #{}", id),
        Response::Error(s) => eprintln!("Could not submit job: {}", s),
        _ => panic!("Unexpected response: {:?}", response),
    }

    Ok(())
}

pub fn handle_reap<T: Stream>(mut stream: T, jobid: &str) -> Result<Job> {
    encode_and_write(
        &serde_json::to_string_pretty(&Request::ReapJob(jobid.parse::<u64>().unwrap()))?,
        &mut stream,
    )?;

    let response = serde_json::from_str(&read_and_decode(&mut stream)?)?;

    match response {
        Response::GetJob(job) => Ok(job),
        Response::Error(s) => {
            eprintln!("Could not reap job: {}", s);
            Err(::std::io::Error::from(::std::io::ErrorKind::Other))
        }
        _ => panic!("Unexpected response: {:?}", response),
    }
}

pub fn handle_queue_status<T: Stream>(mut stream: T) -> Result<()> {
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

    encode_and_write(
        &serde_json::to_string_pretty(&Request::GetFinishedJobs)?,
        &mut stream,
    )?;

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
