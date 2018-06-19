use std::collections::VecDeque;
use std::slice::Iter;
use std::thread;
use std::time::{Duration, SystemTime};

pub enum JobState {
    Queued,
    Running,
    Terminated,
}

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

    pub fn iter_queued(&self) -> Iter<Job> {
        self.queue.iter()
    }

    pub fn iter_finished(&self) -> Iter<Job> {
        self.finished.iter()
    }

    pub fn submit(&mut self, cmdline: String) {
        let job = Job {
            id: self.last_id + 1,
            cmdline: cmdline,
            scheduled: SystemTime::now(),
            started: None,
            finished: None,
            expected_duration: Duration::new(0, 0),
            stderr: None,
            stdout: None,
            exit_code: None,
            state: JobState::Queued,
        };

        self.last_id += 1;
        self.queue.push_back(job);
    }

    pub fn run_once(&mut self) {
        if let Some(j) = self.queue.pop_front() {
            j.started = Some(SystemTime::now());
            // ... run actual job
            j.finished = Some(SystemTime::now());
            j.state = JobState::Terminated;
            j.exit_code = Some(0);
            self.finished.push_back(j);
        }
    }
}
