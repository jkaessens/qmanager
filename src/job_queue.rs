use std::collections::VecDeque;
use std::time::{Duration, SystemTime};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum JobState {
    Queued,
    Running,
    Terminated,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    id: u64,
    cmdline: String,
    scheduled: SystemTime,
    started: Option<SystemTime>,
    finished: Option<SystemTime>,
    expected_duration: Duration,
    stderr: Option<String>,
    stdout: Option<String>,
    exit_code: Option<u32>,
    state: JobState,
}

pub struct JobQueue {
    last_id: u64,
    queue: VecDeque<Job>,
    finished: VecDeque<Job>,
}

impl JobQueue {
    pub fn new() -> Self {
        JobQueue {
            last_id: 0,
            queue: VecDeque::new(),
            finished: VecDeque::new(),
        }
    }

    pub fn iter_queued(&self) -> impl Iterator<Item = &Job> {
        self.queue.iter()
    }

    pub fn iter_finished(&self) -> impl Iterator<Item = &Job> {
        self.finished.iter()
    }

    pub fn submit(&mut self, cmdline: String, expected_duration: Option<Duration>) -> u64 {
        let job = Job {
            id: self.last_id + 1,
            cmdline,
            scheduled: SystemTime::now(),
            started: None,
            finished: None,
            expected_duration: expected_duration.unwrap_or_default(),
            stderr: None,
            stdout: None,
            exit_code: None,
            state: JobState::Queued,
        };

        self.last_id += 1;
        self.queue.push_back(job);
        self.last_id
    }

    pub fn schedule(&mut self) -> Option<String> {
        self.queue.front_mut().map(|j| {
            j.started = Some(SystemTime::now());
            j.state = JobState::Running;
            j.cmdline.clone()
        })
    }

    pub fn finish(&mut self, exit_code: u32, new_state: JobState) {
        if let Some(mut j) = self.queue.pop_front() {
            if j.state != JobState::Running {
                panic!("Trying to finish a job that is not running: {:?}", j);
            }

            j.finished = Some(SystemTime::now());
            j.state = new_state;
            j.exit_code = Some(exit_code);
            self.finished.push_back(j);
        }
    }

    pub fn remove_finished(&mut self, id: u64) -> Result<(), ()> {
        let mut index: Option<usize> = None;

        for (current, job) in self.finished.iter().enumerate() {
            if job.id == id {
                index = Some(current);
                break;
            }
        }

        if let Some(i) = index {
            self.finished
                .remove(i)
                .expect("Failed to remove element from job queue");
            Ok(())
        } else {
            Err(())
        }
    }
}
