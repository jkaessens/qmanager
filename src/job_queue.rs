use std::collections::VecDeque;

use std::time::{Duration, SystemTime};
//use std::os::unix::process::ExitStatusExt;

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
    pub expected_duration: Duration,
    pub stderr: String,
    pub stdout: String,
    pub state: JobState,
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

    pub fn submit(
        &mut self,
        cmdline: String,
        expected_duration: Option<Duration>,
        notify_cmd: Option<String>,
    ) -> u64 {
        let job = Job {
            id: self.last_id + 1,
            cmdline,
            scheduled: SystemTime::now(),
            started: None,
            finished: None,
            expected_duration: expected_duration.unwrap_or_default(),
            stderr: String::from(""),
            stdout: String::from(""),
            state: JobState::Queued,
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
