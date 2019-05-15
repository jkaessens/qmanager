use std::io::prelude::*;

use job_queue::Job;


pub trait Stream: Read + Write {}
impl<T: Read + Write> Stream for T {}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    SubmitJob(String, Option<String>),
    RemoveJob(u64),
    KillJob(u64),
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

/*
pub fn encode_and_write(source: &str, target: &mut Stream, dump: bool) -> Result<()> {
    if dump {
        println!("Sending:");
        println!("{}", source);
    }

    let len = source.len();

    target.write_u32::<LittleEndian>(len as u32)?;
    target.write_all(source.as_bytes())?;

    Ok(())
}

pub fn read_and_decode(source: &mut Request, dump: bool) -> Result<String> {

    let len: u32 = source.read_u32::<LittleEndian>()?;

    let mut buf = vec![0; len as usize];

    let tls_handshake = len & 0xFF;
    let tls_ver_major = (len >> 8) & 0xFF;
    let tls_ver_minor = (len >> 16) & 0xFF;

    source.read_exact(&mut buf[0..1])?;

    if tls_handshake == 22
        && tls_ver_major == 3
        && tls_ver_minor > 0
        && buf[0] != 0x7B
        && buf[0] != 0x22
    {
        // We got a TLS handshake on an insecure socket
        Err(::std::io::Error::from(::std::io::ErrorKind::InvalidData))
    } else {
        source.read_exact(&mut buf[1..len as usize])?;

        let s = String::from_utf8(buf).unwrap();
        if dump {
            println!("Receiving:");
            println!("{}", &s);
        }
        Ok(s)
    }
}
*/