use std::io::prelude::*;

use job_queue::{Job,QueueState};

pub trait Stream: Read + Write {}
impl<T: Read + Write> Stream for T {}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    SubmitJob(String),
    RemoveJob(u64),
    KillJob(u64),
    GetQueuedJobs,
    GetFinishedJobs,
    SetQueueState(QueueState),
    GetQueueState,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    SubmitJob(u64),
    GetJobs(Vec<Job>),
    GetJob(Job),
    Error(String),
    QueueState(QueueState),
    Ok,
}
