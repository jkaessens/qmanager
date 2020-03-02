/**
 * clicommands.rs
 *
 * Contains various functions that create JSON requests out of CLI arguments,
 * parse the JSON response and provide a human-readable(-ish) console output.
 **/
use std::io::Result;

use serde_json;

use job_queue::*;
use protocol::{Request, Response};

/// Dumps a job vector to the console
fn print_jobs(header: &str, jobs: Vec<Job>) {
    println!("{}", header);
    for j in jobs {
        println!("{:?}", j);
    }
}

/// Sends a job submission request to the server and processes its result
///
/// # Arguments
///
/// * `client` - a HTTP(S) client object to be used for the connection
/// * `url` - the absolute URL that the client should use for posting the request
/// * `cmdline`- command line to be submitted for execution
/// * `dump_protocol` - a flag indicating that the JSON requests and responses are to be dumped
pub fn handle_submit(
    client: &reqwest::Client,
    url: reqwest::Url,
    cmdline: &str,
    dump_protocol: bool,
) -> Result<()> {
    // serialize the request into a JSON object
    let request_s = serde_json::to_string_pretty(&Request::SubmitJob(cmdline.to_string()))?;

    // write it to the server
    let mut response_req = client
        .post(url.clone())
        .body(request_s.clone())
        .send()
        .unwrap();
    if dump_protocol {
        println!("Sent: {} ", request_s);
    }

    // block for the server's response...
    let response_s = response_req.text().unwrap();
    if dump_protocol {
        println!("Received: {} ", response_s);
    }

    // ...and deserialize the response object from JSON
    let response = serde_json::from_str(&response_s)?;

    match response {
        Response::SubmitJob(id) => println!("Submitted as job #{}", id),
        Response::Error(s) => eprintln!("Could not submit job: {}", s),
        _ => panic!("Unexpected response: {:?}", response),
    }

    Ok(())
}

/// Requests a job to be removed from the queue
pub fn handle_remove(
    client: &reqwest::Client,
    url: reqwest::Url,
    jobid: u64,
    dump_protocol: bool,
) -> Result<Job> {
    let request_s = serde_json::to_string_pretty(&Request::RemoveJob(jobid))?;
    let mut response_req = client
        .post(url.clone())
        .body(request_s.clone())
        .send()
        .unwrap();
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

/// Requests a running job to be terminated
pub fn handle_kill(
    client: &reqwest::Client,
    url: reqwest::Url,
    jobid: u64,
    dump_protocol: bool,
) -> Result<()> {
    let request_s = serde_json::to_string_pretty(&Request::KillJob(jobid))?;

    let mut response_req = client
        .post(url.clone())
        .body(request_s.clone())
        .send()
        .unwrap();
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

/// Sets the current state of the queue.
/// Note that 'Stopped' cannot be set manually and will yield errors. You will have
/// to set 'Stopping' and let the queue itself to decide to go into 'Stopped' mode.
pub fn handle_set_queue_status(
    client: &reqwest::Client,
    url: reqwest::Url,
    new_state: QueueState,
    dump_protocol: bool,
) -> Result<()> {
    let request_s = serde_json::to_string_pretty(&Request::SetQueueState(new_state)).unwrap();
    let mut response_req = client
        .post(url.clone())
        .body(request_s.clone())
        .send()
        .unwrap();
    if dump_protocol {
        println!("Sent: {} ", request_s);
    }
    let response_s = response_req.text().unwrap();
    if dump_protocol {
        println!("Received: {} ", response_s);
    }
    let response = serde_json::from_str(&response_s)?;
    match response {
        Response::QueueState(s) => println!("Current queue status: {:?}", s),
        Response::Error(s) => eprintln!("Could not get queue status: {}", s),
        _ => panic!("Unexpected response: {:?}", response),
    };
    Ok(())
}

/// Requests the job queue state, the list of queued, running and finished jobs respectively
pub fn handle_queue_status(
    client: &reqwest::Client,
    url: reqwest::Url,
    dump_protocol: bool,
) -> Result<()> {
    // Request general queue state
    let mut request_s = serde_json::to_string_pretty(&Request::GetQueueState).unwrap();
    let mut response_req = client
        .post(url.clone())
        .body(request_s.clone())
        .send()
        .unwrap();
    if dump_protocol {
        println!("Sent: {} ", request_s);
    }
    let response_s = response_req.text().unwrap();
    if dump_protocol {
        println!("Received: {} ", response_s);
    }
    let response = serde_json::from_str(&response_s)?;
    match response {
        Response::QueueState(s) => println!("Current queue status: {:?}", s),
        Response::Error(s) => eprintln!("Could not get queue status: {}", s),
        _ => panic!("Unexpected response: {:?}", response),
    };

    // Request list of queued jobs (including running)
    request_s = serde_json::to_string_pretty(&Request::GetQueuedJobs).unwrap();
    response_req = client
        .post(url.clone())
        .body(request_s.clone())
        .send()
        .unwrap();
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

    // Request list of finished jobs
    request_s = serde_json::to_string_pretty(&Request::GetFinishedJobs).unwrap();
    response_req = client
        .post(url.clone())
        .body(request_s.clone())
        .send()
        .unwrap();
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
