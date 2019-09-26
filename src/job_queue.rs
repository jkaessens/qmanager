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
}

pub struct JobQueue {
    last_id: u64,
    queue: Vec<Job>,
    finished: Vec<Job>,
}

impl JobQueue {
    pub fn new() -> Self {
        JobQueue {
            last_id: 0,
            queue: Vec::new(),
            finished: Vec::new(),
        }
    }

    pub fn iter_queued(&self) -> impl Iterator<Item = &Job> {
        self.queue.iter()
    }

    pub fn iter_finished(&self) -> impl Iterator<Item = &Job> {
        self.finished.iter()
    }

    pub fn submit(&mut self, cmdline: String) -> u64 {
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
        };

        self.last_id += 1;
        self.queue.push(job);
        self.last_id
    }

    pub fn schedule(&mut self) -> Option<Job> {
        self.queue.first_mut().map(|j| {
            j.started = Some(SystemTime::now());
            j.state = JobState::Running;
            j.clone()
        })
    }

    pub fn send_sigterm(&mut self, jobid: u64) -> Result<(), Error> {
        debug!("[job queue] Trying to kill job {}", jobid);

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
                        error!("Could not kill job. Exit code: {}", code);
                        Err(Error::from_raw_os_error(code))
                    }
                } else {
                    error!("kill didn't leave an exit code");
                    Err(Error::from(ErrorKind::Other))
                }
            },

            None => {
                warn!("Could not find job");
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
        if self.queue.len() > 0 {
            let mut j = self.queue.remove(0);

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

    pub fn remove(&mut self, id: u64) -> Result<Job, ()> {
        let mut item_index :Option<usize> = None;

        // scan finished jobs
        for (current, job) in self.finished.iter().enumerate() {
            if job.id == id {
                item_index = Some(current);
                break;
            }
        }

        if let Some(index) = item_index {
            return Ok(self.finished.remove(index));
        }


        // scan queued jobs (that are not running)
        for (current, job) in self.queue.iter().enumerate() {
            if job.id == id {
                if job.state == JobState::Running {
                    return Err(());
                } else {
                    item_index = Some(current);
                    break;
                }
            }
        }

        if let Some(index) = item_index {
            return Ok(self.queue.remove(index));
        }

        Err(())
    }
}
