use crate::{gpu::Reservation, id, model::*, process::Identity, read_json, write_json};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, Params, params};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

pub struct Store {
    pub db: Connection,
    pub root: PathBuf,
}
pub fn column(db: &Connection, sql: &str, args: impl Params) -> Result<Vec<String>> {
    Ok(db
        .prepare(sql)?
        .query_map(args, |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}
pub const ACTIVE: &str = "('running','starting','needs_review','cancelling')";
impl Store {
    pub fn open(root: PathBuf) -> Result<Self> {
        let db = Connection::open(root.join("scheduler.sqlite3"))?;
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY,request TEXT UNIQUE NOT NULL,manifest TEXT NOT NULL,receipt TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY,meta TEXT NOT NULL,path TEXT UNIQUE NOT NULL,imported INTEGER NOT NULL,purpose_source TEXT NOT NULL);
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS run_numbers(number INTEGER PRIMARY KEY AUTOINCREMENT, run TEXT NOT NULL UNIQUE REFERENCES runs(id), batch TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS numbered_batches ON run_numbers(batch,number);
        INSERT INTO run_numbers(run,batch) SELECT id,json_extract(meta,'$.batch_id') FROM runs
            WHERE NOT EXISTS (SELECT 1 FROM run_numbers WHERE run=runs.id) ORDER BY runs.rowid;
        CREATE TRIGGER IF NOT EXISTS number_new_run AFTER INSERT ON runs BEGIN
            INSERT INTO run_numbers(run,batch) VALUES(NEW.id,json_extract(NEW.meta,'$.batch_id'));
        END;
        COMMIT;
        CREATE TABLE IF NOT EXISTS stages(id TEXT PRIMARY KEY,run TEXT NOT NULL REFERENCES runs(id),name TEXT NOT NULL,priority INTEGER NOT NULL,state TEXT NOT NULL,reason TEXT, UNIQUE(run,name));
        CREATE TABLE IF NOT EXISTS deps(stage TEXT REFERENCES stages(id),requires TEXT REFERENCES stages(id),PRIMARY KEY(stage,requires));
        CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY,stage TEXT NOT NULL REFERENCES stages(id),number INTEGER NOT NULL,spec TEXT NOT NULL,state TEXT NOT NULL,exit TEXT,index_error TEXT, UNIQUE(stage,number));
        CREATE UNIQUE INDEX IF NOT EXISTS one_active ON attempts(stage) WHERE state IN ('starting','running','cancelling','needs_review');
        CREATE TABLE IF NOT EXISTS metrics(attempt TEXT REFERENCES attempts(id),line INTEGER NOT NULL,record TEXT NOT NULL,PRIMARY KEY(attempt,line));
        CREATE TABLE IF NOT EXISTS artifacts(attempt TEXT REFERENCES attempts(id),path TEXT NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(attempt,path));")?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS run_control(
                run TEXT PRIMARY KEY REFERENCES runs(id), generation INTEGER NOT NULL DEFAULT 1,
                failure_attempt TEXT REFERENCES attempts(id), failure_stage TEXT REFERENCES stages(id), failure_reason TEXT);
             INSERT INTO run_control(run) SELECT id FROM runs WHERE imported=0
                AND NOT EXISTS(SELECT 1 FROM run_control WHERE run=runs.id);
             CREATE TABLE IF NOT EXISTS batch_wake(
                batch TEXT PRIMARY KEY REFERENCES batches(id), target TEXT NOT NULL, instance TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS wake_outbox(
                id TEXT PRIMARY KEY, run TEXT NOT NULL REFERENCES runs(id), generation INTEGER NOT NULL,
                event TEXT NOT NULL, payload TEXT NOT NULL, target TEXT NOT NULL, instance TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending', delivery INTEGER NOT NULL DEFAULT 0,
                error TEXT, receipt TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                UNIQUE(run,generation));"
        )?;
        Ok(Self { db, root })
    }
    pub fn submit(
        &mut self,
        request: &str,
        raw: &Value,
        wake: Option<&WakeTarget>,
        config: Option<&Path>,
    ) -> Result<Value> {
        ensure!(!request.trim().is_empty(), "empty request_id");
        let encoded = serde_json::to_string(raw)?;
        let old: Option<(String, String)> = self
            .db
            .query_row(
                "SELECT manifest,receipt FROM batches WHERE request=?",
                [request],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((previous, receipt)) = old {
            ensure!(
                encoded == previous,
                "conflict: request_id already used with a different manifest"
            );
            let receipt: Value = serde_json::from_str(&receipt)?;
            let saved: Option<String> = self
                .db
                .query_row(
                    "SELECT target FROM batch_wake WHERE batch=?",
                    [receipt["batch_id"].as_str().context("missing batch ID")?],
                    |r| r.get(0),
                )
                .optional()?;
            let saved = saved
                .map(|s| serde_json::from_str::<WakeTarget>(&s))
                .transpose()?;
            ensure!(
                saved.as_ref() == wake,
                "conflict: request_id already used with a different wake target"
            );
            return self.numbered_receipt(receipt);
        }
        let manifest: Manifest = serde_json::from_value(raw.clone())?;
        manifest.validate()?;
        let instance = wake
            .map(|w| w.resolve(&Config::load(config)?))
            .transpose()?;
        let batch = id()?;
        let batch_path = PathBuf::from("batches").join(&batch);
        let dir = self.root.join(&batch_path);
        fs::create_dir_all(dir.join("runs"))?;
        write_json(&dir.join("manifest.json"), raw)?;
        let ids = manifest
            .runs
            .iter()
            .map(|r| Ok((r.name.clone(), id()?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let baseline = manifest.baseline.as_ref().map(|n| ids[n].clone());
        let tx = self.db.transaction()?;
        let mut expanded = vec![];
        for run in manifest.runs {
            let purpose = run
                .purpose
                .or(manifest.purpose)
                .context("missing purpose")?;
            let source = if run.purpose.is_some() {
                "run manifest"
            } else {
                "batch manifest"
            };
            let uuid = ids[&run.name].clone();
            let path = batch_path.join("runs").join(&uuid);
            let meta = RunRecord {
                version: 1,
                run_id: uuid,
                batch_id: batch.clone(),
                project: manifest.project.clone(),
                purpose: Some(purpose),
                baseline_id: baseline.clone(),
                stage_ids: run
                    .stages
                    .iter()
                    .map(|s| Ok((s.name.clone(), id()?)))
                    .collect::<Result<_>>()?,
                config: run,
            };
            fs::create_dir_all(self.root.join(&path).join("attempts"))?;
            write_json(&self.root.join(&path).join("run.json"), &meta)?;
            insert_run(&tx, &meta, &path, false, source)?;
            tx.execute("INSERT INTO run_control(run) VALUES(?)", [&meta.run_id])?;
            expanded.push(json!({"run_id":meta.run_id,"name":meta.config.name,"purpose":purpose,"stages":meta.stage_ids}));
        }
        let receipt = json!({"version":1,"batch_id":batch,"baseline_id":baseline,"runs":expanded,"wake":wake});
        write_json(&dir.join("runs.json"), &receipt)?;
        tx.execute(
            "INSERT INTO batches VALUES(?,?,?,?)",
            params![batch, request, encoded, receipt.to_string()],
        )?;
        if let (Some(target), Some(instance)) = (wake, instance) {
            tx.execute(
                "INSERT INTO batch_wake VALUES(?,?,?)",
                params![
                    batch,
                    serde_json::to_string(target)?,
                    serde_json::to_string(&instance)?
                ],
            )?;
        }
        tx.commit()?;
        self.numbered_receipt(receipt)
    }
    fn numbered_receipt(&self, mut receipt: Value) -> Result<Value> {
        for run in receipt["runs"]
            .as_array_mut()
            .context("invalid stored receipt")?
        {
            run["local_id"] = self
                .local_id(run["run_id"].as_str().context("missing receipt run ID")?)?
                .into();
        }
        Ok(receipt)
    }
    pub fn local_id(&self, run: &str) -> Result<String> {
        Ok(self.db.query_row(
            "SELECT '@'||number FROM run_numbers WHERE run=?",
            [run],
            |r| r.get(0),
        )?)
    }
    pub fn resolve_run(&self, id: &str) -> Result<String> {
        let Some(number) = id.strip_prefix('@') else {
            return Ok(id.to_owned());
        };
        let number: i64 = number
            .parse()
            .with_context(|| format!("invalid experiment ID {id}"))?;
        self.db
            .query_row(
                "SELECT run FROM run_numbers WHERE number=?",
                [number],
                |r| r.get(0),
            )
            .with_context(|| format!("unknown experiment {id}"))
    }
    pub fn run_counts(&self) -> Result<Value> {
        // Group by run before counting so parallel stages and multi-GPU jobs count once.
        let (running, queued): (i64, i64) = self.db.query_row(
            "SELECT count(*) FILTER(WHERE active), count(*) FILTER(WHERE NOT active AND waiting AND NOT stopped)
             FROM (SELECT max(s.state IN ('starting','running','cancelling')) AS active,
                          max(s.state='queued') AS waiting,
                          max(s.state IN ('failed','cancelled','needs_review')) AS stopped
                   FROM stages s JOIN runs r ON s.run=r.id WHERE r.imported=0 GROUP BY s.run)",
            [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(json!({"running":running,"queued":queued}))
    }
    pub fn meta(&self, run: &str) -> Result<RunRecord> {
        let raw: String = self
            .db
            .query_row("SELECT meta FROM runs WHERE id=?", [run], |r| r.get(0))
            .with_context(|| format!("run {run}"))?;
        Ok(serde_json::from_str(&raw)?)
    }
    pub fn run_path(&self, run: &str) -> Result<PathBuf> {
        let path: String = self
            .db
            .query_row("SELECT path FROM runs WHERE id=?", [run], |r| r.get(0))
            .with_context(|| format!("run {run}"))?;
        Ok(self.root.join(path))
    }
    pub fn attempt(&self, aid: &str) -> Result<(Attempt, PathBuf)> {
        let raw: String = self
            .db
            .query_row("SELECT spec FROM attempts WHERE id=?", [aid], |r| r.get(0))
            .with_context(|| format!("attempt {aid}"))?;
        let a: Attempt = serde_json::from_str(&raw)?;
        let path = self.run_path(&a.run_id)?.join("attempts").join(aid);
        Ok((a, path))
    }
    pub fn active(&self) -> Result<Vec<String>> {
        column(
            &self.db,
            &format!("SELECT id FROM attempts WHERE state IN {ACTIVE}"),
            [],
        )
    }
    pub fn reservations(&self) -> Result<Vec<Reservation>> {
        self.active()?
            .iter()
            .map(|aid| {
                let (a, dir) = self.attempt(aid)?;
                Ok(Reservation::new(
                    a,
                    read_json::<Identity>(&dir.join("identity.json")),
                ))
            })
            .collect()
    }
    pub fn get_run(&self, run: &str) -> Result<Value> {
        let m = self.meta(run)?;
        let (source, imported): (String, bool) = self.db.query_row(
            "SELECT purpose_source,imported FROM runs WHERE id=?",
            [run],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut stages = vec![];
        let mut states = vec![];
        for s in &m.config.stages {
            let sid = &m.stage_ids[&s.name];
            let (state, reason): (String, Option<String>) =
                self.db
                    .query_row("SELECT state,reason FROM stages WHERE id=?", [sid], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
            let mut attempts = vec![];
            for aid in column(
                &self.db,
                "SELECT id FROM attempts WHERE stage=? ORDER BY number",
                [sid],
            )? {
                let (a, dir) = self.attempt(&aid)?;
                let (status, exit, error): (String, Option<String>, Option<String>) =
                    self.db.query_row(
                        "SELECT state,exit,index_error FROM attempts WHERE id=?",
                        [&aid],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )?;
                let exit = exit
                    .map(|v| serde_json::from_str::<Value>(&v))
                    .transpose()?;
                let (progress, progress_error) =
                    match crate::results::latest_event(&dir, &a, exit.is_some()) {
                        Ok(event) => (event, None),
                        Err(e) => (None, Some(format!("{e:#}"))),
                    };
                attempts.push(json!({
                    "attempt_id":aid, "number":a.number, "state":status, "created_at":a.created_at,
                    "resources":a.stage.resources, "gpus":a.gpus, "exit":exit, "index_error":error,
                    "progress":progress, "progress_error":progress_error, "directory":dir
                }));
            }
            states.push(state.clone());
            stages.push(json!({"stage_id":sid,"name":s.name,"state":state,"reason":reason,"depends_on":s.depends_on,"resources":s.resources,"attempts":attempts}));
        }
        let mut state = [
            "needs_review",
            "cancelling",
            "running",
            "starting",
            "failed",
            "cancelled",
            "queued",
            "blocked",
            "skipped",
            "succeeded",
        ]
        .into_iter()
        .find(|s| states.iter().any(|v| v == *s))
        .context("unrecognized run state")?;
        let failure = self.run_failure(run)?;
        if failure.is_some() {
            state = if states.iter().any(|s| {
                matches!(
                    s.as_str(),
                    "starting" | "running" | "cancelling" | "needs_review"
                )
            }) {
                "failing"
            } else {
                "failed"
            };
        }
        let wake = self.wake_status(run)?;
        let (group_id, description): (String, Option<String>) = self.db.query_row(
            "SELECT 'B'||min(number), (SELECT json_extract(manifest,'$.description') FROM batches WHERE id=?1)
             FROM run_numbers WHERE batch=?1",
            [&m.batch_id],
            |r| Ok((r.get(0)?,r.get(1)?)),
        )?;
        Ok(json!({
            "version":1, "run_id":run, "local_id":self.local_id(run)?, "batch_id":m.batch_id,
            "group_id":group_id, "batch_description":description, "name":m.config.name, "project":m.project,
            "purpose":m.purpose, "purpose_source":source, "baseline_id":m.baseline_id,
            "priority":m.config.priority, "parameters":m.config.parameters, "code_ref":m.config.code_ref,
            "imported":imported, "directory":self.run_path(run)?, "state":state, "stages":stages,
            "failure":failure, "wake":wake
        }))
    }
    pub fn list_runs(
        &self,
        project: Option<&str>,
        purpose: Option<Purpose>,
        batch: Option<&str>,
        state: Option<&str>,
    ) -> Result<Vec<Value>> {
        let ids = column(
            &self.db,
            "SELECT id FROM runs WHERE (?1 IS NULL OR json_extract(meta,'$.project')=?1) AND (?2 IS NULL OR json_extract(meta,'$.purpose')=?2) AND (?3 IS NULL OR json_extract(meta,'$.batch_id')=?3) ORDER BY rowid",
            params![project, purpose.map(Purpose::name), batch],
        )?;
        let mut out = vec![];
        for id in ids {
            let r = self.get_run(&id)?;
            if state.is_none_or(|s| r["state"] == s) {
                out.push(r);
            }
        }
        Ok(out)
    }
}
pub fn insert_run(
    db: &Connection,
    m: &RunRecord,
    path: &Path,
    imported: bool,
    source: &str,
) -> Result<()> {
    db.execute(
        "INSERT INTO runs VALUES(?,?,?,?,?)",
        params![
            m.run_id,
            serde_json::to_string(m)?,
            path.to_str().context("non-UTF8 run path")?,
            imported,
            source
        ],
    )?;
    for s in &m.config.stages {
        let Some(sid) = m.stage_ids.get(&s.name) else {
            bail!("missing ID for stage {}", s.name)
        };
        db.execute(
            "INSERT INTO stages VALUES(?,?,?,?,?,?)",
            params![
                sid,
                m.run_id,
                s.name,
                m.config.priority,
                if imported { "blocked" } else { "queued" },
                if imported {
                    Some("historical stage has no completed attempt")
                } else {
                    None
                }
            ],
        )?;
    }
    for s in &m.config.stages {
        for d in &s.depends_on {
            db.execute(
                "INSERT INTO deps VALUES(?,?)",
                params![m.stage_ids[&s.name], m.stage_ids[d]],
            )?;
        }
    }
    Ok(())
}
