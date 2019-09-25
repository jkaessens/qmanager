use std::io::Result;

use serde_json;

use job_queue::*;
use protocol::{Request, Response};

fn print_jobs(header: &str, jobs: Vec<Job>) {
    println!("{}", header);
    for j in jobs {
        println!("{:?}", j);
    }
}

pub fn handle_submit(client: &reqwest::Client,
                                url: reqwest::Url,
                                cmdline: &str,
                                notifycmd: Option<String>,
                                dump_protocol: bool) -> Result<()> {

    let request_s = serde_json::to_string_pretty(&Request::SubmitJob(
        cmdline.to_string(),
        notifycmd.map(|s| s.to_string())))?;

    let mut response_req = client.post(url.clone()).body(request_s.clone()).send().unwrap();
    if dump_protocol {
        println!("Sent: {} ", request_s);
    }

    let response_s = response_req.text().unwrap();
    if dump_protocol {
        println!("Received: {} ", response_s);
    }
    let response = serde_json::from_str(&response_s)?;

    match response {
        Response::SubmitJob(id) => println!("Submitted as job #{}", id),
        Response::Error(s) => eprintln!("Could not submit job: {}", s),
        _ => panic!("Unexpected response: {:?}", response),
    }

    Ok(())
}

pub fn handle_remove(client: &reqwest::Client,
                                url: reqwest::Url,
                                jobid: u64,
                                dump_protocol: bool) -> Result<Job> {

    let request_s = serde_json::to_string_pretty(
        &Request::RemoveJob(jobid))?;
    let mut response_req = client.post(url.clone()).body(request_s.clone()).send().unwrap();
    if dump_protocol {
        println!("Sent: {} ", request_s);
    }

    let response_s = response_req.text().unwrap();
    if dump_protocol {
        println!("Received: {} ", response_s);
    }
    let response = serde_json::from_str(&response_s)?;


    match response {
        Response::GetJob(job) => Ok(job),
        Response::Error(s) => {
            eprintln!("Could not remove job: {}", s);
            Err(::std::io::Error::from(::std::io::ErrorKind::Other))
        }
        _ => panic!("Unexpected response: {:?}", response),
    }
}

pub fn handle_kill(client: &reqwest::Client,
                              url: reqwest::Url,
                              jobid: u64,
                              dump_protocol: bool) -> Result<()> {
    let request_s = serde_json::to_string_pretty(
        &Request::KillJob(jobid))?;

    let mut response_req = client.post(url.clone()).body(request_s.clone()).send().unwrap();
    if dump_protocol {
        println!("Sent: {} ", request_s);
    }

    let response_s = response_req.text().unwrap();
    if dump_protocol {
        println!("Received: {} ", response_s);
    }
    let response = serde_json::from_str(&response_s)?;

    match response {
        Response::Ok => Ok(()),
        Response::Error(s) => {
            eprintln!("Could not kill job: {}", s);
            Err(::std::io::Error::from(::std::io::ErrorKind::Other))
        }
        _ => panic!("Unexpected response: {:?}", response),
    }
}

pub fn handle_queue_status(client: &reqwest::Client,
                                      url: reqwest::Url,
                                      dump_protocol: bool) -> Result<()> {
    let mut request_s = serde_json::to_string_pretty(&Request::GetQueuedJobs).unwrap();

    let mut response_req = client.post(url.clone()).body(request_s.clone()).send().unwrap();
    if dump_protocol {
        println!("Sent: {} ", request_s);
    }

    let response_s = response_req.text().unwrap();
    if dump_protocol {
        println!("Received: {} ", response_s);
    }
    let mut response = serde_json::from_str(&response_s)?;

    match response {
        Response::GetJobs(jobs) => print_jobs("QUEUED JOBS", jobs),
        Response::Error(s) => {
            eprintln!("Could not get queued jobs: {}", s);
        }
        _ => {
            panic!("Unexpected response: {:?}", response);
        }
    }


    request_s = serde_json::to_string_pretty(&Request::GetFinishedJobs).unwrap();
    response_req = client.post(url.clone()).body(request_s.clone()).send().unwrap();
    if dump_protocol {
        println!("Sent: {} ", request_s);
    }

    let response_s = response_req.text().unwrap();
    if dump_protocol {
        println!("Received: {} ", response_s);
    }
    response = serde_json::from_str(&response_s)?;
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
