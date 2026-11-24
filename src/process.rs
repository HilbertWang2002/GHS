use crate::{
    model::{Attempt, Exit},
    now, read_json, write_json,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

#[derive(Debug)]
pub struct Stat {
    pub parent: u32,
    pub session: u32,
    pub start: u64,
    pub state: char,
    pub name: String,
}
pub fn stat(pid: u32) -> Result<Stat> {
    let path = format!("/proc/{pid}/stat");
    let s = fs::read_to_string(&path).with_context(|| path.clone())?;
    let (left, tail) = s.rsplit_once(") ").context("invalid proc stat")?;
    let name = left
        .split_once('(')
        .context("invalid proc name")?
        .1
        .to_owned();
    let fields: Vec<_> = tail.split_whitespace().collect();
    ensure!(fields.len() >= 20, "truncated {path}");
    Ok(Stat {
        parent: fields[1].parse()?,
        session: fields[3].parse()?,
        start: fields[19].parse()?,
        state: fields[0].chars().next().context("missing state")?,
        name,
    })
}
pub fn boot_id() -> Result<String> {
    Ok(fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned())
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub version: u32,
    pub attempt_id: String,
    pub boot_id: String,
    pub pid: u32,
    pub start: u64,
}
impl Identity {
    pub fn verify(&self, attempt_id: &str, boot: &str) -> Result<()> {
        ensure!(
            self.version == 1 && self.attempt_id == attempt_id && self.boot_id == boot,
            "runner record or boot ID mismatch"
        );
        let s = stat(self.pid)?;
        ensure!(
            s.start == self.start && s.session == self.pid && s.state != 'Z' && s.state != 'X',
            "runner {} identity changed or exited",
            self.pid
        );
        Ok(())
    }
}
fn kill_children() -> Result<()> {
    // Direct children cannot recycle a PID until this single-threaded subreaper waits.
    // Once killed, their living descendants are reparented here and drained in turn.
    let pid = std::process::id();
    let children = fs::read_to_string(format!("/proc/self/task/{pid}/children"))?;
    for child in children.split_whitespace() {
        if unsafe { libc::kill(child.parse()?, libc::SIGKILL) } == -1 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::ESRCH) {
                return Err(e.into());
            }
        }
    }
    Ok(())
}
fn wait_tree(main: i32, dir: &Path) -> Result<(i32, bool)> {
    let mut root_status = None;
    let mut cancelled = false;
    loop {
        if dir.join("cancel").try_exists()? {
            cancelled = true;
            kill_children()?;
        }
        let mut status = 0;
        let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if pid < 0 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() == Some(libc::ECHILD) {
                return Ok((
                    root_status.context("missing command wait status")?,
                    cancelled,
                ));
            }
            if e.kind() != io::ErrorKind::Interrupted {
                return Err(e.into());
            }
        } else if pid == main {
            root_status = Some(status);
        } else if pid == 0 {
            thread::sleep(Duration::from_millis(40));
        }
    }
}
pub fn run(dir: &Path) -> Result<()> {
    let a: Attempt = read_json(&dir.join("attempt.json"))?;
    ensure!(a.version == 1, "unsupported attempt version");
    let pid = std::process::id();
    ensure!(
        unsafe { libc::getsid(0) } == pid as i32,
        "runner requires its own session"
    );
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let identity = Identity {
        version: 1,
        attempt_id: a.attempt_id.clone(),
        boot_id: boot_id()?,
        pid,
        start: stat(pid)?.start,
    };
    write_json(&dir.join("identity.json"), &identity)?;
    let output = dir.join("artifacts");
    let log = File::create_new(dir.join("output.log"))?;
    let mut command = Command::new(&a.stage.command[0]);
    command
        .args(&a.stage.command[1..])
        .envs(&a.stage.env)
        .env("CUDA_VISIBLE_DEVICES", a.gpus.join(","))
        .env("GHS_OUTPUT_DIR", &output)
        .env("GHS_ATTEMPT_ID", &a.attempt_id)
        .env("GHS_RUN_ID", &a.run_id)
        .env("GHS_STAGE_ID", &a.stage_id)
        .env("GHS_CPUS", a.stage.resources.cpus.to_string())
        .env("GHS_PURPOSE", a.purpose.name())
        .env("GHS_EVENTS", dir.join("events.jsonl"))
        .env("GHS_RESULTS", dir.join("results.jsonl"))
        .env(
            "GHS_RUN_DIR",
            dir.parent()
                .and_then(Path::parent)
                .context("invalid attempt layout")?,
        )
        .current_dir(a.stage.cwd.as_ref().unwrap_or(&output))
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    let mut exit = Exit {
        version: 1,
        attempt_id: a.attempt_id,
        code: None,
        signal: None,
        cancelled: false,
        finished_at: 0,
        missing_artifacts: vec![],
        error: None,
    };
    match command.spawn() {
        Err(e) => exit.error = Some(format!("launch {}: {e}", a.stage.command[0])),
        Ok(child) => {
            let (status, cancelled) = wait_tree(child.id() as i32, dir)?;
            if libc::WIFEXITED(status) {
                exit.code = Some(libc::WEXITSTATUS(status));
            }
            if libc::WIFSIGNALED(status) {
                exit.signal = Some(libc::WTERMSIG(status));
            }
            exit.cancelled = cancelled;
            for path in &a.stage.artifacts {
                match fs::metadata(output.join(path)) {
                    Ok(m) if m.is_file() => (),
                    Ok(_) => exit.missing_artifacts.push(path.clone()),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        exit.missing_artifacts.push(path.clone())
                    }
                    Err(e) => {
                        exit.error = Some(format!("inspect artifact {}: {e}", path.display()));
                        break;
                    }
                }
            }
        }
    }
    exit.finished_at = now();
    write_json(&dir.join("exit.json"), &exit)
}
