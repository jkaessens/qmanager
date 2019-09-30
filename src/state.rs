use std::path::PathBuf;
use std::io::Write;
use std::io::Result;
use std::fs::{File, OpenOptions};

const DEFAULT_STATE_LAST_ID :u64 = 0;

pub struct State {
    pub last_id: u64,
    state_file: PathBuf,
}

impl State {

    fn load(p: PathBuf) -> State {
        let mut config = config::Config::default();
        config.merge(config::File::new(p.to_str().unwrap(), config::FileFormat::Toml)).unwrap();

        State {
            last_id: config.get::<u64>("last_id").expect("State file exists but does not contain 'last_id' option"),
            state_file: p
        }
    }

    pub fn from(p: PathBuf) -> State {
        if !p.exists() {
            warn!("Cannot open state file {}. Using defaults.", p.to_str().unwrap());
            State {
                last_id: DEFAULT_STATE_LAST_ID,
                state_file: p
            }
        } else {
            debug!("Loading program state from {}", p.to_str().unwrap());
            State::load(p)
        }
    }

    pub fn save(&self) -> Result<()> {
        let f = File::create(&self.state_file);
        if let Err(e) = f {
            error!("Cannot create or open state file {}: {:?}", self.state_file.to_str().unwrap(), e);
            Err(e)
        } else {
            let mut f = f.unwrap();
            f.write_fmt(format_args!("last_id = {}", self.last_id))?;
            debug!("State file {} updated.", self.state_file.to_str().unwrap());
            Ok(())
        }
    }
}