pub mod api;
pub mod gpu;
pub mod model;
pub mod process;
pub mod results;
pub mod scheduler;
pub mod store;
pub mod wake;

use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}
pub fn id() -> Result<String> {
    Ok(fs::read_to_string("/proc/sys/kernel/random/uuid")?
        .trim()
        .to_owned())
}
pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_reader(File::open(path).with_context(|| path.display().to_string())?)
        .with_context(|| format!("invalid JSON in {}", path.display()))
}
pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temp = path.with_extension(format!("{}.tmp", id()?));
    let mut file = File::create_new(&temp)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    File::open(path.parent().context("missing parent")?)?.sync_all()?;
    Ok(())
}
pub fn data_dir(path: Option<PathBuf>) -> Result<PathBuf> {
    match path {
        Some(path) => Ok(std::path::absolute(path)?),
        None => Ok(
            PathBuf::from(std::env::var_os("HOME").context("set --data-dir or HOME")?)
                .join(".local/state/ghs"),
        ),
    }
}
