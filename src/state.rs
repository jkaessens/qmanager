use std::fs::{self, File};
use std::io::Result;
use std::path::PathBuf;

use job_queue::*;

const DEFAULT_STATE_LAST_ID: u64 = 0;

pub struct State {
    state_file: PathBuf,
}

impl State {
    fn load(p: PathBuf) -> State {
        /*
        let mut config = config::Config::default();
        config.merge(config::File::new(p.to_str().unwrap(), config::FileFormat::Toml)).unwrap();

        State {
        state_file: p,
        queue: serde_json::from_str(config.)
        }
        */

        //let queue_s = fs::read_to_string(&p).expect("Could not read file");

        State { state_file: p }
    }

    pub fn from(p: PathBuf) -> State {
        if !p.exists() {
            warn!(
                "Cannot open state file {}. Using defaults.",
                p.to_str().unwrap()
            );
            State { state_file: p }
        } else {
            debug!("Loading program state from {}", p.to_str().unwrap());
            State::load(p)
        }
    }

    pub fn load_queue(&self) -> JobQueue {
        let s = fs::read_to_string(&self.state_file).unwrap_or("".to_owned());
        let o = serde_json::from_str(&s);
        if let Ok(q) = o {
            q
        } else {
            warn!("Could not parse JobQueue from state file, returning default queue");
            JobQueue::new(DEFAULT_STATE_LAST_ID)
        }
    }

    pub fn save(&self, q: &JobQueue) -> Result<()> {
        let f = File::create(&self.state_file);
        if let Err(e) = f {
            error!(
                "Cannot create or open state file {}: {:?}",
                self.state_file.to_str().unwrap(),
                e
            );
            Err(e)
        } else {
            let mut f = f.unwrap();
            serde_json::to_writer_pretty(&mut f, q)?;
            debug!("State file {} updated.", self.state_file.to_str().unwrap());
            Ok(())
        }
    }
}
