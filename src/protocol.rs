
use std::time::Duration;
use std::io::Result;
use std::io::prelude::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use job_queue::Job;

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

pub fn encode_and_write(source: &str, target: &mut Write) -> Result<()> {
    let len = source.len();
    //    let len_byte_buf: [u8; mem::size_of::<u32>()] = unsafe { mem::transmute(len) };

    target.write_u32::<LittleEndian>(len as u32)?;
    target.write_all(source.as_bytes())?;

    Ok(())
}

pub fn read_and_decode(source: &mut Read) -> Result<String> {

    let len: u32 = source.read_u32::<LittleEndian>()?;

    let mut buf = vec![0; len as usize];
    source.read_exact(&mut buf)?;

    Ok(String::from_utf8(buf).unwrap())
}
