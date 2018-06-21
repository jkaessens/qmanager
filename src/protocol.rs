use job_queue::Job;
use std::time::Duration;

use std::mem;

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    SubmitJob(String, Option<Duration>),
    ReapJob(u64),
    GetQueuedJobs,
    GetFinishedJobs,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    SubmitJob(u64),
    GetJobs(Vec<Job>),
    Error(String),
    Ok,
}

pub fn encode(source: String, dest: &mut Vec<u8>) {
    let len: u32 = source.len() as u32;

    let len_byte_buf: [u8; mem::size_of::<u32>()] = unsafe { mem::transmute(len) };

    dest.extend_from_slice(&len_byte_buf);
    dest.extend_from_slice(source.as_bytes());
}

pub fn decode(source: &Vec<u8>, dest: &mut String) {
    if source.len() < mem::size_of::<u32>() {
        panic!("Incomplete packet!");
    }

    let len: u32 = unsafe { mem::transmute::<[u8; 4], u32>(source[0..mem::size_of::<u32>()]) };

    let len: u32 = unsafe { mem::transmute(&source[0..mem::size_of::<u32>() - 1]) };
}
