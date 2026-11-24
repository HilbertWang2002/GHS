use crate::{
    gpu,
    model::{Config, Purpose, WakeTarget},
    results,
    store::Store,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "method",
    content = "arguments",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Request {
    GetGpuStatus {
        #[serde(default)]
        processes: bool,
        uuid: Option<String>,
    },
    SubmitBatch {
        request_id: String,
        manifest: Value,
        wake: Option<WakeTarget>,
    },
    ListRuns {
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        gpu_labels: bool,
        project: Option<String>,
        purpose: Option<Purpose>,
        batch_id: Option<String>,
        state: Option<String>,
    },
    GetRun {
        run_id: String,
    },
    GetEvents {
        attempt_id: String,
        #[serde(default)]
        after: u64,
    },
    ReadLog {
        attempt_id: String,
        #[serde(default)]
        offset: u64,
        #[serde(default = "page")]
        length: u64,
    },
    CompareRuns {
        #[serde(default)]
        run_ids: Vec<String>,
        batch_id: Option<String>,
        purpose: Option<Purpose>,
        #[serde(default)]
        metrics: Vec<String>,
        group: Option<String>,
        baseline_id: Option<String>,
    },
    ListArtifacts {
        run_id: String,
        attempt_id: Option<String>,
    },
    CancelAttempt {
        attempt_id: String,
    },
    RetryStage {
        stage_id: String,
    },
    RetryRun {
        run_id: String,
    },
    RetryWake {
        event_id: String,
        #[serde(default)]
        allow_duplicate: bool,
    },
    ImportRun {
        path: PathBuf,
        purpose: Option<Purpose>,
        purpose_source: Option<String>,
    },
    Reindex {
        run_id: Option<String>,
    },
}
pub fn page() -> u64 {
    65536
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reply {
    Result(Value),
    Error(String),
}
impl Store {
    pub fn dispatch(&mut self, req: Request, config: Option<&Path>) -> Result<Value> {
        match req {
            Request::GetGpuStatus { processes, uuid } => {
                let cfg = Config::load(config)?;
                let reservations = self.reservations()?;
                let mut snap = gpu::snapshot(&cfg, &reservations, processes)?;
                if let Some(uuid) = &uuid {
                    snap.devices.retain(|d| d.uuid.as_ref() == Some(uuid));
                    if snap.devices.is_empty() {
                        bail!("unknown GPU UUID {uuid}");
                    }
                }
                let mut active = serde_json::Map::new();
                for r in reservations {
                    let id = &r.owner.attempt_id;
                    if let Some(uuid) = &uuid
                        && !r.gpus.contains(uuid)
                        && !snap
                            .devices
                            .iter()
                            .flat_map(|d| d.processes.iter().flatten())
                            .any(|p| p.owner.as_ref().is_some_and(|o| o.attempt_id == *id))
                    {
                        continue;
                    }
                    let (a, dir) = self.attempt(id)?;
                    let state: String =
                        self.db
                            .query_row("SELECT state FROM attempts WHERE id=?", [id], |r| {
                                r.get(0)
                            })?;
                    let (progress, eta, error) = match results::live_progress(&dir, &a) {
                        Ok((event, eta)) => (event, eta, None),
                        Err(e) => (None, None, Some(format!("{e:#}"))),
                    };
                    active.insert(id.clone(), json!({"run_id":a.run_id, "local_id":self.local_id(&a.run_id)?,
                        "name":self.meta(&a.run_id)?.config.name, "stage":a.stage.name, "state":state, "gpus":a.gpus,
                        "progress":progress,"eta_seconds":eta,"progress_error":error}));
                }
                let mut value = serde_json::to_value(snap)?;
                value["active_attempts"] = active.into();
                value["run_counts"] = self.run_counts()?;
                Ok(value)
            }
            Request::SubmitBatch {
                request_id,
                manifest,
                wake,
            } => self.submit(&request_id, &manifest, wake.as_ref(), config),
            Request::ListRuns {
                gpu_labels,
                project,
                purpose,
                batch_id,
                state,
            } => {
                let runs = self.list_runs(
                    project.as_deref(),
                    purpose,
                    batch_id.as_deref(),
                    state.as_deref(),
                )?;
                let mut reply = json!({});
                if gpu_labels
                    && runs
                        .iter()
                        .flat_map(|r| r["stages"].as_array().into_iter().flatten())
                        .any(|s| s["resources"]["gpus"].as_u64().is_some_and(|n| n > 0))
                {
                    let pool = Config::load(config)?.pool;
                    // A CPU-only host can list waiting jobs without querying NVML.
                    let need_indices = pool.is_some()
                        || runs
                            .iter()
                            .flat_map(|r| r["stages"].as_array().into_iter().flatten())
                            .any(|s| {
                                s["resources"]["allowed_gpus"]
                                    .as_array()
                                    .is_some_and(|g| !g.is_empty())
                                    || s["attempts"].as_array().into_iter().flatten().any(|a| {
                                        a["gpus"].as_array().is_some_and(|g| !g.is_empty())
                                    })
                            });
                    if need_indices {
                        reply["gpu_indices"] = json!(gpu::indices()?);
                    }
                    reply["gpu_pool"] = json!(pool);
                }
                reply["runs"] = runs.into();
                Ok(reply)
            }
            Request::GetRun { run_id } => self.get_run(&self.resolve_run(&run_id)?),
            Request::GetEvents { attempt_id, after } => {
                let (a, dir) = self.attempt(&attempt_id)?;
                let events = results::events(&dir, &a, after, dir.join("exit.json").try_exists()?)?;
                let next = events
                    .last()
                    .and_then(|e| e["seq"].as_u64())
                    .unwrap_or(after);
                Ok(
                    json!({"attempt_id":attempt_id,"events":events,"path":dir.join("events.jsonl"),"next_cursor":next}),
                )
            }
            Request::ReadLog {
                attempt_id,
                offset,
                length,
            } => results::read_log(&self.attempt(&attempt_id)?.1, offset, length),
            Request::CompareRuns {
                run_ids,
                batch_id,
                purpose,
                metrics,
                group,
                baseline_id,
            } => self.compare(
                &run_ids
                    .iter()
                    .map(|id| self.resolve_run(id))
                    .collect::<Result<Vec<_>>>()?,
                batch_id.as_deref(),
                purpose.unwrap_or(Purpose::Experiment),
                &metrics,
                group.as_deref(),
                baseline_id
                    .as_deref()
                    .map(|id| self.resolve_run(id))
                    .transpose()?
                    .as_deref(),
            ),
            Request::ListArtifacts { run_id, attempt_id } => {
                self.artifacts(&self.resolve_run(&run_id)?, attempt_id.as_deref())
            }
            Request::CancelAttempt { attempt_id } => self.cancel(&attempt_id),
            Request::RetryStage { stage_id } => self.retry(&stage_id),
            Request::RetryRun { run_id } => self.retry_run(&self.resolve_run(&run_id)?),
            Request::RetryWake {
                event_id,
                allow_duplicate,
            } => self.retry_wake(&event_id, allow_duplicate),
            Request::ImportRun {
                path,
                purpose,
                purpose_source,
            } => self.import(&path, purpose, purpose_source.as_deref()),
            Request::Reindex { run_id } => self.reindex(
                run_id
                    .as_deref()
                    .map(|id| self.resolve_run(id))
                    .transpose()?
                    .as_deref(),
            ),
        }
    }
}
pub fn call(socket: &Path, req: &Request) -> Result<Value> {
    let mut stream =
        UnixStream::connect(socket).with_context(|| format!("connect to {}", socket.display()))?;
    serde_json::to_writer(&mut stream, req)?;
    stream.write_all(b"\n")?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    match serde_json::from_str(&line).context("invalid daemon reply")? {
        Reply::Result(v) => Ok(v),
        Reply::Error(e) => bail!("{e}"),
    }
}
#[derive(clap::Args, Debug)]
pub struct Address {
    #[arg(long, env = "GHS_DATA_DIR", global = true)]
    pub data_dir: Option<PathBuf>,
    #[arg(long, env = "GHS_SOCKET", global = true)]
    pub socket: Option<PathBuf>,
}
impl Address {
    pub fn socket(&self) -> Result<PathBuf> {
        match &self.socket {
            Some(p) => Ok(std::path::absolute(p)?),
            None => Ok(crate::data_dir(self.data_dir.clone())?.join("ghs.sock")),
        }
    }
}
fn manifest_schema() -> Value {
    let strings = json!({"type":"array","items":{"type":"string"}});
    let purpose = json!({"type":"string","enum":["experiment","validation"]});
    let stage = json!({"type":"object","required":["name","command"],"additionalProperties":false,
        "properties":{"name":{"type":"string"},"command":{"type":"array","minItems":1,"items":{"type":"string"}},
        "cwd":{"type":"string","description":"Absolute working directory; defaults to GHS_OUTPUT_DIR"},
        "env":{"type":"object","additionalProperties":{"type":"string"}},"depends_on":strings,
        "artifacts":{"type":"array","items":{"type":"string"},"description":"Required files relative to GHS_OUTPUT_DIR"},
        "resources":{"type":"object","additionalProperties":false,"properties":{"cpus":{"type":"integer","minimum":1},"gpus":{"type":"integer","minimum":0},"allowed_gpus":strings}}}});
    let run = json!({"type":"object","required":["name","stages"],"additionalProperties":false,"properties":{
        "name":{"type":"string"},"purpose":purpose,"priority":{"type":"integer"},"code_ref":{"type":"string"},
        "parameters":{"type":"object"},"stages":{"type":"array","minItems":1,"items":stage}}});
    json!({"type":"object","required":["version","project","runs"],"additionalProperties":false,"properties":{
        "version":{"type":"integer","const":1},"project":{"type":"string"},"description":{"type":"string"},
        "purpose":purpose,"baseline":{"type":"string","description":"Name of the reference run"},
        "runs":{"type":"array","minItems":1,"items":run}}})
}
pub fn tool_list() -> Value {
    fn tool(name: &str, desc: &str, fields: &[(&str, &str)], required: &[&str]) -> Value {
        let mut props = serde_json::Map::new();
        for (key, ty) in fields {
            let schema = match *ty {
                "manifest" => manifest_schema(),
                "wake" => {
                    json!({"type":"object","additionalProperties":false,"required":["runtime","instance","session_id"],
                    "properties":{"runtime":{"type":"string","enum":["t3code"]},"instance":{"type":"string"},
                    "session_id":{"type":"string","description":"T3 Code thread ID, not the underlying provider session ID"}}})
                }
                "purpose" => json!({"enum":["validation","experiment"],"type":"string"}),
                "strings" => json!({"type":"array","items":{"type":"string"}}),
                "integer" => json!({"type":"integer","minimum":0}),
                _ => json!({"type":ty}),
            };
            props.insert(key.to_string(), schema);
        }
        let read_only = matches!(
            name,
            "get_gpu_status"
                | "list_runs"
                | "get_run"
                | "get_events"
                | "read_log"
                | "compare_runs"
                | "list_artifacts"
        );
        json!({
            "name": name, "description": desc, "annotations": {"readOnlyHint": read_only},
            "inputSchema": {"type":"object","properties":props,"required":required,"additionalProperties":false}
        })
    }
    let tools = [
        tool(
            "get_gpu_status",
            "Sample physical GPUs, running and queued run counts, active progress and stage ETA.",
            &[("processes", "boolean"), ("uuid", "string")],
            &[],
        ),
        tool(
            "submit_batch",
            "Submit a version 1 JSON manifest. Each run stops on its first failed stage. Optional wake targets a configured T3 instance and thread: notify immediately on run failure or once all its stages succeed. Reuse request_id with identical manifest and wake for idempotency.",
            &[
                ("request_id", "string"),
                ("manifest", "manifest"),
                ("wake", "wake"),
            ],
            &["request_id", "manifest"],
        ),
        tool(
            "list_runs",
            "List both purposes, optionally filtering runs.",
            &[
                ("project", "string"),
                ("purpose", "purpose"),
                ("batch_id", "string"),
                ("state", "string"),
            ],
            &[],
        ),
        tool(
            "get_run",
            "Inspect one run by UUID or local ID such as @1, including its stages, attempts and progress.",
            &[("run_id", "string")],
            &["run_id"],
        ),
        tool(
            "get_events",
            "Read progress after a sequence cursor (initially 0).",
            &[("attempt_id", "string"), ("after", "integer")],
            &["attempt_id"],
        ),
        tool(
            "read_log",
            "Read raw combined stdout/stderr by byte offset; length defaults to 65536.",
            &[
                ("attempt_id", "string"),
                ("offset", "integer"),
                ("length", "integer"),
            ],
            &["attempt_id"],
        ),
        tool(
            "compare_runs",
            "Compare selected observations against baseline_id or the manifest baseline. Defaults to experiment.",
            &[
                ("run_ids", "strings"),
                ("batch_id", "string"),
                ("purpose", "purpose"),
                ("metrics", "strings"),
                ("group", "string"),
                ("baseline_id", "string"),
            ],
            &[],
        ),
        tool(
            "list_artifacts",
            "List output file locations.",
            &[("run_id", "string"), ("attempt_id", "string")],
            &["run_id"],
        ),
        tool(
            "cancel_attempt",
            "Cancel a live attempt and its descendants.",
            &[("attempt_id", "string")],
            &["attempt_id"],
        ),
        tool(
            "retry_stage",
            "Queue another attempt for a cancelled stage. Runs stopped by failure require retry_run.",
            &[("stage_id", "string")],
            &["stage_id"],
        ),
        tool(
            "retry_run",
            "Explicitly retry a failed local run after cleanup. Preserve succeeded stages and logs; requeue other stages in a new generation.",
            &[("run_id", "string")],
            &["run_id"],
        ),
        tool(
            "retry_wake",
            "Retry a failed T3 notification. Unknown delivery requires allow_duplicate=true after inspecting T3; may create another turn.",
            &[("event_id", "string"), ("allow_duplicate", "boolean")],
            &["event_id"],
        ),
        tool(
            "import_run",
            "Register a portable run directory read-only; supply purpose and source if absent.",
            &[
                ("path", "string"),
                ("purpose", "purpose"),
                ("purpose_source", "string"),
            ],
            &["path"],
        ),
        tool(
            "reindex",
            "Rebuild metric and artifact indexes from finished attempt directories.",
            &[("run_id", "string")],
            &[],
        ),
    ];
    json!({"tools": tools})
}
