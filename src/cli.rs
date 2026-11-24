mod output;

use anyhow::{Context, Result};
use clap::Subcommand;
use ghs::{
    api::{self, Request},
    model::{Purpose, WakeRuntime, WakeTarget},
    read_json,
};
use std::{
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

#[derive(Subcommand)]
pub enum Action {
    /// 查看运行数、排队数、GPU 占用及进度
    Status {
        #[arg(long)]
        processes: bool,
        #[arg(long)]
        uuid: Option<String>,
        #[arg(long)]
        watch: bool,
    },
    /// 按清单提交一批运行，相同请求 ID 可重复提交
    #[command(alias = "submit-batch")]
    Submit {
        manifest: PathBuf,
        #[arg(long)]
        request_id: String,
        /// T3 Code thread ID to notify on each run's failure or success
        #[arg(long, requires = "wake_instance")]
        wake_session: Option<String>,
        /// Named [t3code.NAME] instance in the daemon configuration
        #[arg(long, requires = "wake_session")]
        wake_instance: Option<String>,
    },
    /// 按运行查看队列和进度
    #[command(alias = "list-runs")]
    List {
        /// 额外显示完整的运行 UUID
        #[arg(long)]
        ids: bool,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        purpose: Option<Purpose>,
        #[arg(long)]
        batch_id: Option<String>,
        #[arg(long)]
        state: Option<String>,
    },
    /// 用短编号（如 @1）或 UUID 查看实验详情
    #[command(alias = "get-run")]
    Get { run_id: String },
    #[command(alias = "get-events")]
    Events {
        attempt_id: String,
        #[arg(long, default_value_t = 0)]
        after: u64,
    },
    #[command(alias = "read-log")]
    Log {
        attempt_id: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long,default_value_t=api::page())]
        length: u64,
    },
    #[command(alias = "compare-runs")]
    Compare {
        run_ids: Vec<String>,
        #[arg(long)]
        batch_id: Option<String>,
        #[arg(long)]
        purpose: Option<Purpose>,
        #[arg(long = "metric")]
        metrics: Vec<String>,
        #[arg(long)]
        group: Option<String>,
        #[arg(long)]
        baseline_id: Option<String>,
    },
    #[command(alias = "list-artifacts")]
    Artifacts {
        run_id: String,
        #[arg(long)]
        attempt_id: Option<String>,
    },
    #[command(alias = "cancel-attempt")]
    Cancel { attempt_id: String },
    #[command(alias = "retry-stage")]
    Retry { stage_id: String },
    /// 清理完成后重试失败的整个 run，保留成功阶段
    RetryRun { run_id: String },
    /// 手动重新投递失败的唤醒通知
    RetryWake {
        event_id: String,
        /// 已核查 T3；允许结果不明的投递可能生成重复消息
        #[arg(long)]
        allow_duplicate: bool,
    },
    #[command(alias = "import-run")]
    Import {
        path: PathBuf,
        #[arg(long)]
        purpose: Option<Purpose>,
        #[arg(long)]
        purpose_source: Option<String>,
    },
    Reindex {
        #[arg(long)]
        run_id: Option<String>,
    },
}
pub fn run(action: Action, socket: &Path, json: bool) -> Result<()> {
    use base64::Engine;
    let show_ids = matches!(action, Action::List { ids: true, .. });
    let req = match action {
        Action::Status {
            processes,
            uuid,
            watch,
        } => {
            let req = Request::GetGpuStatus { processes, uuid };
            loop {
                let v = api::call(socket, &req)?;
                if watch && !json && io::stdout().is_terminal() {
                    print!("\x1b[2J\x1b[H");
                }
                if json {
                    println!("{}", serde_json::to_string(&v)?);
                } else {
                    println!("{}", output::status(&v)?);
                }
                if !watch {
                    return Ok(());
                }
                let interval = v["poll_interval_seconds"]
                    .as_u64()
                    .context("daemon reply missing poll_interval_seconds")?;
                thread::sleep(Duration::from_secs(interval));
            }
        }
        Action::Submit {
            manifest,
            request_id,
            wake_session,
            wake_instance,
        } => Request::SubmitBatch {
            request_id,
            manifest: read_json(&manifest)?,
            wake: wake_session
                .map(|session_id| {
                    Ok::<_, anyhow::Error>(WakeTarget {
                        runtime: WakeRuntime::T3code,
                        session_id,
                        instance: wake_instance.context("--wake-instance is required")?,
                    })
                })
                .transpose()?,
        },
        Action::List {
            project,
            purpose,
            batch_id,
            state,
            ..
        } => Request::ListRuns {
            gpu_labels: !json,
            project,
            purpose,
            batch_id,
            state,
        },
        Action::Get { run_id } => Request::GetRun { run_id },
        Action::Events { attempt_id, after } => Request::GetEvents { attempt_id, after },
        Action::Log {
            attempt_id,
            offset,
            length,
        } => {
            let v = api::call(
                socket,
                &Request::ReadLog {
                    attempt_id,
                    offset,
                    length,
                },
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else {
                io::stdout().write_all(
                    &base64::engine::general_purpose::STANDARD.decode(
                        v["base64"]
                            .as_str()
                            .context("daemon reply missing log bytes")?,
                    )?,
                )?;
            }
            return Ok(());
        }
        Action::Compare {
            run_ids,
            batch_id,
            purpose,
            metrics,
            group,
            baseline_id,
        } => Request::CompareRuns {
            run_ids,
            batch_id,
            purpose,
            metrics,
            group,
            baseline_id,
        },
        Action::Artifacts { run_id, attempt_id } => Request::ListArtifacts { run_id, attempt_id },
        Action::Cancel { attempt_id } => Request::CancelAttempt { attempt_id },
        Action::Retry { stage_id } => Request::RetryStage { stage_id },
        Action::RetryRun { run_id } => Request::RetryRun { run_id },
        Action::RetryWake {
            event_id,
            allow_duplicate,
        } => Request::RetryWake {
            event_id,
            allow_duplicate,
        },
        Action::Import {
            path,
            purpose,
            purpose_source,
        } => Request::ImportRun {
            path: path.canonicalize()?,
            purpose,
            purpose_source,
        },
        Action::Reindex { run_id } => Request::Reindex { run_id },
    };
    let reply = api::call(socket, &req)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&reply)?);
    } else {
        println!("{}", output::render(&req, &reply, show_ids)?);
    }
    Ok(())
}
