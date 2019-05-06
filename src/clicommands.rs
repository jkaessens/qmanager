use std::io::Result;

use serde_json;

use job_queue::*;
use protocol::{encode_and_write, read_and_decode, Request, Response, Stream};

fn print_jobs(header: &str, jobs: Vec<Job>) {
    println!("{}", header);
    for j in jobs {
        println!("{:?}", j);
    }
}

pub fn handle_submit<T: Stream>(mut stream: T,
                                cmdline: &str,
                                notifycmd: Option<&str>,
                                dump_protocol: bool) -> Result<()> {

    let request = &serde_json::to_string_pretty(&Request::SubmitJob(
        cmdline.to_string(),
        notifycmd.map(|s| s.to_string())))?;


    encode_and_write(request,
        &mut stream,
        dump_protocol
    )?;


    // Daemon returns associated Job ID
    let response = serde_json::from_str(&read_and_decode(&mut stream, dump_protocol)?)?;
    match response {
        Response::SubmitJob(id) => println!("Submitted as job #{}", id),
        Response::Error(s) => eprintln!("Could not submit job: {}", s),
        _ => panic!("Unexpected response: {:?}", response),
    }

    Ok(())
}

pub fn handle_remove<T: Stream>(mut stream: T, jobid: &str, dump_protocol: bool) -> Result<Job> {
    encode_and_write(
        &serde_json::to_string_pretty(&Request::RemoveJob(jobid.parse::<u64>().unwrap()))?,
        &mut stream,
        dump_protocol
    )?;

    let response = serde_json::from_str(&read_and_decode(&mut stream, dump_protocol)?)?;

    match response {
        Response::GetJob(job) => Ok(job),
        Response::Error(s) => {
            eprintln!("Could not remove job: {}", s);
            Err(::std::io::Error::from(::std::io::ErrorKind::Other))
        }
        _ => panic!("Unexpected response: {:?}", response),
    }
}

pub fn handle_kill<T: Stream>(mut stream: T, jobid: &str, dump_protocol: bool) -> Result<()> {
    encode_and_write(
        &serde_json::to_string_pretty(&Request::KillJob(jobid.parse::<u64>().unwrap()))?,
        &mut stream,
        dump_protocol
    )?;

    let response = serde_json::from_str(&read_and_decode(&mut stream, dump_protocol)?)?;

    match response {
        Response::Ok => Ok(()),
        Response::Error(s) => {
            eprintln!("Could not kill job: {}", s);
            Err(::std::io::Error::from(::std::io::ErrorKind::Other))
        }
        _ => panic!("Unexpected response: {:?}", response),
    }
}

pub fn handle_queue_status<T: Stream>(mut stream: T, dump_protocol: bool) -> Result<()> {
    let s = serde_json::to_string_pretty(&Request::GetQueuedJobs).unwrap();
    encode_and_write(&s, &mut stream, dump_protocol)?;

    let response = serde_json::from_str(&read_and_decode(&mut stream, dump_protocol)?)?;

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
        dump_protocol
    )?;

    let response = serde_json::from_str(&read_and_decode(&mut stream, dump_protocol)?)?;

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
