
use std::time::Duration;
use std::io::Result;
use std::io::prelude::*;


use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use job_queue::Job;

pub trait Stream: Read + Write {}
impl<T: Read + Write> Stream for T {}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    SubmitJob(String, Option<Duration>, Option<String>),
    ReapJob(u64),
    GetQueuedJobs,
    GetFinishedJobs,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    SubmitJob(u64),
    GetJobs(Vec<Job>),
    GetJob(Job),
    Error(String),
    Ok,
}

pub fn encode_and_write(source: &str, target: &mut Stream) -> Result<()> {
    let len = source.len();

    target.write_u32::<LittleEndian>(len as u32)?;
    target.write_all(source.as_bytes())?;

    Ok(())
}

pub fn read_and_decode(source: &mut Stream) -> Result<String> {

    let len: u32 = source.read_u32::<LittleEndian>()?;

    let mut buf = vec![0; len as usize];
    source.read_exact(&mut buf)?;

    Ok(String::from_utf8(buf).unwrap())
}
