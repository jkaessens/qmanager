use std::io::{Error, ErrorKind};
use std::process::Command;
use std::time::SystemTime;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum JobState {
    Queued,
    Running,
    Terminated(i32),
    Killed(i32),
    Failed(String),
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
pub enum QueueState {
    Running,
    Stopping,
    Stopped,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobQueue {
    last_id: u64,
    state: QueueState,
    queue: Vec<Job>,
    finished: Vec<Job>,
}

impl JobQueue {
    pub fn new(last_id: u64) -> Self {
        JobQueue {
            last_id,
            state: QueueState::Running,
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

    pub fn get_state(&self) -> QueueState {
        self.state
    }

    pub fn set_state(&mut self, mut new_state: QueueState) {
        debug!(
            "Trying to set state {:?} to {:?}, queue size is {}",
            self.state,
            new_state,
            self.queue.len()
        );
        if new_state == QueueState::Stopping && self.queue.is_empty() {
            new_state = QueueState::Stopped;
        }
        self.state = new_state;
    }

    pub fn reset_first_job(&mut self, new_state: JobState) {
        debug!("Setting status of first job in queue to {:?}", new_state);

        // Only reset if there is a job
        if let Some(j) = self.queue.first() {
            // Only reset if the current job is not properly queued (i.e. Running)
            if j.state != JobState::Queued {
                match new_state {
                    JobState::Running | JobState::Killed(_) | JobState::Terminated(_) => {
                        panic!("Cannot manually set a job to Running, Terminated or Killed state")
                    }
                    JobState::Queued => {
                        self.queue.first_mut().map(|j| {
                            j.started = None;
                            j.state = JobState::Queued;
                            j.pid = None;
                            j.stderr = String::from("");
                            j.stdout = String::from("");
                        });
                    }
                    JobState::Failed(s) => {
                        self.finish(JobState::Failed(s), "".to_owned(), "".to_owned());
                    }
                }
            }
        }
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
        if self.state == QueueState::Running {
            self.queue.first_mut().map(|j| {
                j.started = Some(SystemTime::now());
                j.state = JobState::Running;
                j.clone()
            })
        } else {
            None
        }
    }

    pub fn send_sigterm(&mut self, jobid: u64) -> Result<(), Error> {
        debug!("[job queue] Trying to kill job {}", jobid);

        match self
            .queue
            .iter()
            .find(|j| j.state == JobState::Running && j.id == jobid)
        {
            Some(job) => {
                // It is okay to panic here, as failure to execute /bin/kill is a serious bug
                let status = Command::new("/usr/bin/pkill")
                    .arg("-SIGTERM")
                    .arg("-P")
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
            }

            None => {
                warn!("Could not find job");
                Err(Error::from(ErrorKind::InvalidInput))
            }
        }
    }

    pub fn assign_pid(&mut self, jobid: u64, pid: u32) {
        if let Some(ref mut job) = self
            .queue
            .iter_mut()
            .find(|job| job.state == JobState::Running && job.id == jobid)
        {
            job.pid = Some(pid);
        }
    }

    pub fn finish(&mut self, new_state: JobState, stdout: String, stderr: String) -> Option<Job> {
        if !self.queue.is_empty() {
            let mut j = self.queue.remove(0);
            debug!(
                "Queue finish: job {} old state {:?} new state {:?}",
                j.id, j.state, new_state
            );
            if j.state != JobState::Running {
                panic!("Trying to finish a job that is not running: {:?}", j);
            }

            j.finished = Some(SystemTime::now());
            j.state = new_state;
            j.stdout = stdout;
            j.stderr = stderr;
            self.finished.push(j.clone());
            if self.state == QueueState::Stopping {
                self.state = QueueState::Stopped;
            }
            Some(j)
        } else {
            error!("Queue finish: no job?");
            None
        }
    }

    pub fn remove(&mut self, id: u64) -> Result<Job, ()> {
        let mut item_index: Option<usize> = None;

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
