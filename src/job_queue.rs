use std::io::{Error, ErrorKind};
use std::process::Command;
use std::time::SystemTime;

/// The current state of a single job
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum JobState {
    /// queued and waiting for execution
    Queued,

    /// currently running (top of 'queued' queue)
    Running,

    /// the process has exited with the given value
    Terminated(i32),

    /// the process has been killed by the given signal
    Killed(i32),

    /// the process could not be launched or was aborted by us
    Failed(String),
}

/// The reason a job-specific command could not be processed
pub enum FailReason {
    /// The job is in the wrong state (i.e. removing a running job)
    WrongJobState,

    /// There is no such job
    NoSuchJob,
}

/// The state of the job queue
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
pub enum QueueState {
    /// queue is idle or executing a job
    Running,

    /// the last process is executed before the queue is stopped
    Stopping,

    /// the queue is stopped and is not precessing jobs
    Stopped,
}

/// The Job
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    /// The unique job ID
    pub id: u64,

    /// Command to be executed
    pub cmdline: String,

    /// Timestamp of queue insertion
    pub scheduled: SystemTime,

    /// Timestamp of execution start
    pub started: Option<SystemTime>,

    /// Timestamp of execution end
    pub finished: Option<SystemTime>,

    /// stderr content
    pub stderr: String,

    /// stdout content
    pub stdout: String,

    /// current job state
    pub state: JobState,

    /// PID of the process (only if running or finished)
    pub pid: Option<u32>,
}

/// The Job Queue itself
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobQueue {
    /// The last ID assigned to a job
    last_id: u64,

    /// Queue state
    state: QueueState,

    /// The list of queued jobs, including the currently running one
    queue: Vec<Job>,

    /// List of finished jobs
    finished: Vec<Job>,
}

impl JobQueue {
    /// Creates a new JobQueue with the given last ID. The first ID
    /// to be assigned will be last_id+1.
    pub fn new(last_id: u64) -> Self {
        JobQueue {
            last_id,
            state: QueueState::Running,
            queue: Vec::new(),
            finished: Vec::new(),
        }
    }

    /// Provides an iterator over the currently queued jobs, including the running
    pub fn iter_queued(&self) -> impl Iterator<Item = &Job> {
        self.queue.iter()
    }

    /// Provides an iterater over the finished jobs
    pub fn iter_finished(&self) -> impl Iterator<Item = &Job> {
        self.finished.iter()
    }

    /// Returns the state of the queue
    pub fn get_state(&self) -> QueueState {
        self.state
    }

    /// Sets the state of the queue
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

    /// Reset the first job of the queue if it was some variation of Running
    /// during start/resume.
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

    /// Submits a new job to the queue and returns the assigned ID
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

    /// Returns the topmost job of the "queued" queue if available.
    /// The job is expected to be executed.
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

    /// Sends SIGTERM to the associated pid of the given job ID
    pub fn send_sigterm(&mut self, jobid: u64) -> Result<(), Error> {
        debug!("[job queue] Trying to kill job {}", jobid);

        // find the currently running job
        match self
            .queue
            .iter()
            .find(|j| j.state == JobState::Running && j.id == jobid)
        {
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
            }

            None => {
                warn!("Could not find job");
                Err(Error::from(ErrorKind::InvalidInput))
            }
        }
    }

    /// Assigns a pid to the currently running job
    pub fn assign_pid(&mut self, jobid: u64, pid: u32) {
        if let Some(ref mut job) = self
            .queue
            .iter_mut()
            .find(|job| job.state == JobState::Running && job.id == jobid)
        {
            job.pid = Some(pid);
        }
    }

    /// Sets a job to the "Finished" state and moves it to the appropriate queue.
    /// Time stamps are updated.
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

    /// Removes the job identified by the given ID.
    /// Only queued or finished jobs can be removed. Trying to remove a running
    /// job will fail.
    pub fn remove(&mut self, id: u64) -> Result<Job, FailReason> {
        let mut item_index: Option<usize> = None;

        // scan finished jobs
        for (current, job) in self.finished.iter().enumerate() {
            if job.id == id {
                item_index = Some(current);
                break;
            }
        }

        // remove if found
        if let Some(index) = item_index {
            return Ok(self.finished.remove(index));
        }

        // scan queued jobs (that are not running)
        for (current, job) in self.queue.iter().enumerate() {
            if job.id == id {
                if job.state == JobState::Running {
                    return Err(FailReason::WrongJobState);
                } else {
                    item_index = Some(current);
                    break;
                }
            }
        }

        if let Some(index) = item_index {
            return Ok(self.queue.remove(index));
        }

        Err(FailReason::NoSuchJob)
    }
}
