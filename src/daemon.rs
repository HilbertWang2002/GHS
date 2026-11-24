use anyhow::{Context, Result, ensure};
use clap::Args;
use ghs::{
    api::{Address, Reply, Request},
    data_dir,
    model::Config,
    store::Store,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    os::unix::{
        fs::FileTypeExt,
        net::{UnixListener, UnixStream},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[derive(Args)]
pub struct Options {
    #[arg(long, env = "GHS_CONFIG")]
    config: Option<PathBuf>,
    /// Start in a new session and return once the socket is ready.
    #[arg(long)]
    detach: bool,
}
type Job = (Request, mpsc::Sender<Reply>);
fn lock(path: &Path) -> Result<File> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    f.try_lock()
        .with_context(|| format!("daemon lock {}", path.display()))?;
    Ok(f)
}
fn handle(mut stream: UnixStream, tx: mpsc::Sender<Job>) -> Result<()> {
    let request = (|| -> Result<Reply> {
        let mut line = String::new();
        BufReader::new(&stream).read_line(&mut line)?;
        let req = serde_json::from_str(&line)?;
        let (reply, rx) = mpsc::channel();
        tx.send((req, reply))?;
        Ok(rx.recv()?)
    })();
    let response = request.unwrap_or_else(|e| Reply::Error(format!("{e:#}")));
    serde_json::to_writer(&mut stream, &response)?;
    stream.write_all(b"\n")?;
    Ok(())
}
fn detach(root: &Path, socket: &Path, config: Option<&Path>) -> Result<()> {
    let log = root.join("daemon.log");
    let mut cmd = Command::new("/proc/self/exe");
    cmd.arg("daemon")
        .arg("--data-dir")
        .arg(root)
        .arg("--socket")
        .arg(socket);
    if let Some(config) = config {
        cmd.arg("--config").arg(config);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(File::options().append(true).create(true).open(&log)?);
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd.spawn()?;
    let mut ready = String::new();
    BufReader::new(child.stdout.take().context("missing startup pipe")?).read_line(&mut ready)?;
    if ready.is_empty() {
        anyhow::bail!(
            "daemon exited with {}, see {}",
            child.wait()?,
            log.display()
        );
    }
    print!("{ready}");
    Ok(())
}
pub fn run(address: Address, options: Options) -> Result<()> {
    unsafe {
        libc::umask(0o077);
    }
    let socket = address.socket()?;
    let root = data_dir(address.data_dir)?;
    fs::create_dir_all(&root)?;
    let root = root.canonicalize()?;
    if options.detach {
        return detach(&root, &socket, options.config.as_deref());
    }
    let _data_lock = lock(&root.join("daemon.lock"))?;
    if let Some(p) = socket.parent() {
        fs::create_dir_all(p)?;
    }
    let mut sock_lock = socket.as_os_str().to_os_string();
    sock_lock.push(".lock");
    let _sock_lock = lock(Path::new(&sock_lock))?;
    match fs::symlink_metadata(&socket) {
        Ok(m) => {
            ensure!(
                m.file_type().is_socket(),
                "socket path already exists: {}",
                socket.display()
            );
            match UnixStream::connect(&socket) {
                Ok(_) => anyhow::bail!("socket is in use"),
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    fs::remove_file(&socket)?
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => return Err(e.into()),
    }
    let mut store = Store::open(root)?;
    let mut poll_interval_seconds = Config::load(options.config.as_deref())?.poll_interval_seconds;
    store.reconcile()?;
    let listener =
        UnixListener::bind(&socket).with_context(|| format!("bind {}", socket.display()))?;
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let sender = tx.clone();
                    thread::spawn(move || {
                        if let Err(e) = handle(s, sender) {
                            eprintln!("client: {e:#}");
                        }
                    });
                }
                Err(e) => {
                    eprintln!("accept: {e}");
                    break;
                }
            }
        }
    });
    eprintln!("GHS listening on {}", socket.display());
    println!(
        "{}",
        serde_json::json!({"pid":std::process::id(),"socket":socket})
    );
    std::io::stdout().flush()?;
    let mut next = Instant::now();
    loop {
        if Instant::now() >= next {
            // Reap executor children without delivering signals to running attempts on shutdown.
            while unsafe { libc::waitpid(-1, std::ptr::null_mut(), libc::WNOHANG) } > 0 {}
            store.reconcile()?;
            store.dispatch_wakes()?;
            match Config::load(options.config.as_deref()) {
                Ok(config) => {
                    poll_interval_seconds = config.poll_interval_seconds;
                    store.schedule(&config)?;
                    // Runner launch failures can enqueue a failure notification in this pass.
                    store.dispatch_wakes()?;
                }
                Err(e) => eprintln!("scheduling paused: {e:#}"),
            }
            next = Instant::now() + Duration::from_secs(poll_interval_seconds.into());
        }
        match rx.recv_timeout(next.saturating_duration_since(Instant::now())) {
            Ok((req, sender)) => {
                let reply = match store.dispatch(req, options.config.as_deref()) {
                    Ok(v) => Reply::Result(v),
                    Err(e) => Reply::Error(format!("{e:#}")),
                };
                if sender.send(reply).is_err() {
                    eprintln!("client disconnected");
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            Err(e) => return Err(e.into()),
        }
    }
}
