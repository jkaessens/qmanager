use std::collections::VecDeque;

use std::time::SystemTime;
use std::process::Command;
use std::io::{Error,ErrorKind};


#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum JobState {
    Queued,
    Running,
    Terminated(i32),
    Killed(i32),
    Failed(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    pub id: u64,
    pub cmdline: String,
    pub scheduled: SystemTime,
    pub started: Option<SystemTime>,
    pub finished: Option<SystemTime>,
    pub stderr: String,
    pub stdout: String,
    pub state: JobState,
    pub pid: Option<u32>,
    pub notify_cmd: Option<String>,
}

pub struct JobQueue {
    last_id: u64,
    queue: VecDeque<Job>,
    finished: Vec<Job>,
}

impl JobQueue {
    pub fn new() -> Self {
        JobQueue {
            last_id: 0,
            queue: VecDeque::new(),
            finished: Vec::new(),
        }
    }

    pub fn iter_queued(&self) -> impl Iterator<Item = &Job> {
        self.queue.iter()
    }

    pub fn iter_finished(&self) -> impl Iterator<Item = &Job> {
        self.finished.iter()
    }

    pub fn submit(&mut self, cmdline: String, notify_cmd: Option<String>) -> u64 {
        let job = Job {
            id: self.last_id + 1,
            cmdline,
            scheduled: SystemTime::now(),
            started: None,
            finished: None,
            stderr: String::from(""),
            stdout: String::from(""),
            state: JobState::Queued,
            pid: None,
            notify_cmd,
        };

        self.last_id += 1;
        self.queue.push_back(job);
        self.last_id
    }

    pub fn schedule(&mut self) -> Option<Job> {
        self.queue.front_mut().map(|j| {
            j.started = Some(SystemTime::now());
            j.state = JobState::Running;
            j.clone()
        })
    }

    pub fn send_sigterm(&mut self, jobid: u64) -> Result<(), Error> {
        eprintln!("[job queue] Trying to kill job {}", jobid);
        match self.queue.iter().find(|j| j.state == JobState::Running && j.id == jobid) {
            Some(job) => {
                // It is okay to panic here, as failure to execute /bin/kill is a serious bug
                let status = Command::new("/bin/kill")
                    .arg("-SIGTERM")
                    .arg(job.pid.unwrap().to_string())
                    .status()
                    .expect("Failed to execute kill command");

                if let Some(code) = status.code() {
                    if code == 0 {
                        Ok(())
                    } else {
                        eprintln!("Could not kill job. Exit code: {}", code);
                        Err(Error::from_raw_os_error(code))
                    }
                } else {
                    panic!("kill didn't leave an exit code");
                }
            },

            None => {
                eprintln!("Could not find job");
                Err(Error::from(ErrorKind::InvalidInput))
            }
        }
    }

    pub fn assign_pid(&mut self, jobid: u64, pid: u32) {
        if let Some(ref mut job) = self.queue.iter_mut().find(|job| job.state == JobState::Running && job.id == jobid) {
            job.pid = Some(pid);
        }
    }

    pub fn finish(&mut self, new_state: JobState, stdout: String, stderr: String) -> Option<Job> {
        if let Some(mut j) = self.queue.pop_front() {
            if j.state != JobState::Running {
                panic!("Trying to finish a job that is not running: {:?}", j);
            }

            j.finished = Some(SystemTime::now());
            j.state = new_state;
            j.stdout = stdout;
            j.stderr = stderr;
            self.finished.push(j.clone());
            Some(j)
        } else {
            None
        }
    }

    pub fn remove_finished(&mut self, id: u64) -> Result<Job, ()> {
        let mut index: Option<usize> = None;

        for (current, job) in self.finished.iter().enumerate() {
            if job.id == id {
                index = Some(current);
                break;
            }
        }

        if let Some(i) = index {
            Ok(self.finished.remove(i))
        } else {
            Err(())
        }
    }
}
