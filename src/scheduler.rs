use crate::{
    gpu, id,
    model::*,
    now,
    process::{Identity, boot_id},
    read_json, results,
    store::{ACTIVE, Store, column},
    write_json,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    fs::{self, File},
    io,
    os::unix::process::CommandExt,
    path::Path,
    process::{Command, Stdio},
};

impl Store {
    fn stage_state(&self, sid: &str, state: &str, reason: Option<&str>) -> Result<()> {
        self.db.execute(
            "UPDATE stages SET state=?2,reason=?3 WHERE id=?1 AND (state!=?2 OR reason IS NOT ?3)",
            params![sid, state, reason],
        )?;
        Ok(())
    }
    fn attempt_state(
        &mut self,
        aid: &str,
        sid: &str,
        state: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "UPDATE attempts SET state=? WHERE id=? AND state!=?",
            params![state, aid, state],
        )?;
        tx.execute(
            "UPDATE stages SET state=?2,reason=?3 WHERE id=?1 AND (state!=?2 OR reason IS NOT ?3)",
            params![sid, state, reason],
        )?;
        tx.commit()?;
        Ok(())
    }
    fn finish(&mut self, a: &Attempt, dir: &Path, exit: &Exit) -> Result<()> {
        ensure!(
            exit.version == 1 && exit.attempt_id == a.attempt_id,
            "exit record identity mismatch"
        );
        let mut tx = self.db.transaction()?;
        let state = exit.state();
        let reason = if state == "failed" {
            Some(format!(
                "code {:?}, signal {:?}, missing {:?}, error {:?}",
                exit.code, exit.signal, exit.missing_artifacts, exit.error
            ))
        } else {
            None
        };
        tx.execute(
            "UPDATE attempts SET state=?,exit=? WHERE id=?",
            params![state, serde_json::to_string(exit)?, a.attempt_id],
        )?;
        tx.execute(
            "UPDATE stages SET state=?,reason=? WHERE id=?",
            params![state, reason, a.stage_id],
        )?;
        // Invalid project data is recorded separately from process completion.
        results::refresh_index(&mut tx, a, dir)?;
        if state == "failed" {
            fail_run(
                &tx,
                &self.root,
                a,
                reason.as_deref().context("missing failure reason")?,
            )?;
        } else if state == "succeeded" {
            let complete: bool = tx.query_row(
                "SELECT NOT EXISTS(SELECT 1 FROM stages WHERE run=?1 AND state!='succeeded')
                 AND NOT EXISTS(SELECT 1 FROM run_control WHERE run=?1 AND failure_attempt IS NOT NULL)",
                [&a.run_id], |r| r.get(0))?;
            if complete {
                crate::wake::enqueue(&tx, &self.root, &a.run_id, "run.succeeded", json!({}))?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    fn reconcile_one(&mut self, a: &Attempt, dir: &Path, boot: &str) -> Result<()> {
        let exit = dir.join("exit.json");
        if exit.try_exists()? {
            return self.finish(a, dir, &read_json(&exit)?);
        }
        let i: Identity = read_json(&dir.join("identity.json"))?;
        i.verify(&a.attempt_id, boot)?;
        let state = if dir.join("cancel").try_exists()? {
            "cancelling"
        } else {
            "running"
        };
        self.attempt_state(&a.attempt_id, &a.stage_id, state, None)
    }
    pub fn reconcile(&mut self) -> Result<()> {
        let boot = boot_id()?;
        for aid in self.active()? {
            let (a, dir) = self.attempt(&aid)?;
            if let Err(e) = self.reconcile_one(&a, &dir, &boot) {
                self.attempt_state(&aid, &a.stage_id, "needs_review", Some(&format!("{e:#}")))?;
            }
        }
        // Adopt failures recorded by older versions as well as failures from this pass.
        let failures = column(
            &self.db,
            "SELECT a.id FROM attempts a JOIN stages s ON s.id=a.stage
             JOIN run_control c ON c.run=s.run WHERE s.state='failed' AND a.state='failed'
             AND c.failure_attempt IS NULL
             AND a.number=(SELECT max(number) FROM attempts WHERE stage=a.stage)
             ORDER BY json_extract(a.exit,'$.finished_at'),a.rowid",
            [],
        )?;
        for aid in failures {
            let (a, _) = self.attempt(&aid)?;
            let reason: String =
                self.db
                    .query_row("SELECT reason FROM stages WHERE id=?", [&a.stage_id], |r| {
                        r.get(0)
                    })?;
            let tx = self.db.transaction()?;
            fail_run(&tx, &self.root, &a, &reason)?;
            tx.commit()?;
        }
        self.abort_failed_runs(&boot)
    }
    fn abort_failed_runs(&mut self, boot: &str) -> Result<()> {
        let abort = column(&self.db, &format!(
            "SELECT a.id FROM attempts a JOIN stages s ON s.id=a.stage
             JOIN run_control c ON c.run=s.run WHERE c.failure_attempt IS NOT NULL AND a.state IN {ACTIVE}"), [])?;
        for aid in abort {
            let (a, dir) = self.attempt(&aid)?;
            if let Err(e) = self.request_cancel(&a, &dir, boot) {
                self.attempt_state(
                    &aid,
                    &a.stage_id,
                    "needs_review",
                    Some(&format!("fail-fast cleanup: {e:#}")),
                )?;
            }
        }
        Ok(())
    }
    pub fn schedule(&mut self, config: &Config) -> Result<()> {
        let queue = column(
            &self.db,
            "SELECT s.id FROM stages s JOIN runs r ON r.id=s.run WHERE r.imported=0 AND s.state IN ('queued','blocked') ORDER BY s.priority DESC,s.rowid",
            [],
        )?;
        let mut reservations = self.reservations()?;
        // Hardware observations are shared for this pass. Reservations are updated after every start.
        let mut sample = None;
        for sid in queue {
            // The queue is a snapshot. A launch failure earlier in this pass may have stopped this run.
            let eligible: bool = self.db.query_row(
                "SELECT s.state IN ('queued','blocked') AND c.failure_attempt IS NULL
                 FROM stages s JOIN run_control c ON c.run=s.run WHERE s.id=?",
                [&sid],
                |r| r.get(0),
            )?;
            if !eligible {
                continue;
            }
            let unmet = column(
                &self.db,
                "SELECT s.name||' ('||s.state||')' FROM deps d JOIN stages s ON s.id=d.requires WHERE d.stage=? AND s.state!='succeeded'",
                [&sid],
            )?;
            if !unmet.is_empty() {
                self.stage_state(
                    &sid,
                    "blocked",
                    Some(&format!("waiting for {}", unmet.join(", "))),
                )?;
                continue;
            }
            let (rid, name): (String, String) =
                self.db
                    .query_row("SELECT run,name FROM stages WHERE id=?", [&sid], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
            let m = self.meta(&rid)?;
            let s = m
                .config
                .stages
                .into_iter()
                .find(|s| s.name == name)
                .context("missing stage in run")?;
            let used: u64 = reservations.iter().map(|r| u64::from(r.cpus)).sum();
            if used + u64::from(s.resources.cpus) > u64::from(config.cpus) {
                self.stage_state(
                    &sid,
                    "queued",
                    Some(&format!(
                        "needs {} CPU slots, {used}/{} reserved",
                        s.resources.cpus, config.cpus
                    )),
                )?;
                continue;
            }
            let mut gpus = vec![];
            if s.resources.gpus > 0 {
                let reserved = gpu::reserved_uuids(&reservations);
                if let Some(reason) =
                    gpu::gpu_limit_reason(config.max_gpus, reserved.len(), s.resources.gpus)
                {
                    self.stage_state(&sid, "queued", Some(&reason))?;
                    continue;
                }
                let snap = match sample
                    .get_or_insert_with(|| gpu::snapshot(config, &reservations, false))
                {
                    Ok(v) => v,
                    Err(e) => {
                        self.stage_state(&sid, "queued", Some(&format!("GPU sampling: {e:#}")))?;
                        continue;
                    }
                };
                gpus = select_gpus(&s.resources, snap, &reserved);
                if gpus.len() < s.resources.gpus {
                    let details = snap
                        .devices
                        .iter()
                        .map(|d| {
                            format!(
                                "{} {} {}",
                                d.uuid.as_deref().unwrap_or("?"),
                                allocation_state(d, &reserved),
                                d.reasons.join(", ")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("; ");
                    self.stage_state(
                        &sid,
                        "queued",
                        Some(&format!(
                            "needs {} GPU(s), {} eligible under allowed_gpus {:?}: {details}",
                            s.resources.gpus,
                            gpus.len(),
                            s.resources.allowed_gpus
                        )),
                    )?;
                    continue;
                }
            }
            let number = self.db.query_row(
                "SELECT coalesce(max(number),0)+1 FROM attempts WHERE stage=?",
                [&sid],
                |r| r.get(0),
            )?;
            let a = Attempt {
                version: 1,
                attempt_id: id()?,
                run_id: rid,
                stage_id: sid,
                purpose: m.purpose.context("missing purpose")?,
                number,
                stage: s,
                gpus,
                created_at: now(),
            };
            self.start(&a)?;
            if self.run_failure(&a.run_id)?.is_some() {
                self.abort_failed_runs(&boot_id()?)?;
            }
            reservations = self.reservations()?;
        }
        Ok(())
    }
    fn start(&mut self, a: &Attempt) -> Result<()> {
        let dir = self
            .run_path(&a.run_id)?
            .join("attempts")
            .join(&a.attempt_id);
        fs::create_dir(&dir)?;
        fs::create_dir(dir.join("artifacts"))?;
        write_json(&dir.join("attempt.json"), a)?;
        let log = File::create_new(dir.join("runner.log"))?;
        let stderr = log.try_clone()?;
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO attempts(id,stage,number,spec,state) VALUES(?,?,?,?,'starting')",
            params![
                a.attempt_id,
                a.stage_id,
                a.number,
                serde_json::to_string(a)?
            ],
        )?;
        tx.execute(
            "UPDATE stages SET state='starting',reason=NULL WHERE id=?",
            [&a.stage_id],
        )?;
        tx.commit()?;
        // Use the running image even if the installed binary is replaced during an upgrade.
        let mut cmd = Command::new("/proc/self/exe");
        cmd.arg("runner")
            .arg(&dir)
            .stdin(Stdio::null())
            .stdout(log)
            .stderr(stderr);
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        if let Err(e) = cmd.spawn() {
            let exit = Exit {
                version: 1,
                attempt_id: a.attempt_id.clone(),
                finished_at: now(),
                code: None,
                signal: None,
                cancelled: false,
                missing_artifacts: vec![],
                error: Some(format!("launch ghs runner: {e}")),
            };
            write_json(&dir.join("exit.json"), &exit)?;
            self.finish(a, &dir, &exit)?;
        }
        Ok(())
    }
    pub fn cancel(&mut self, aid: &str) -> Result<Value> {
        let (a, dir) = self.attempt(aid)?;
        let imported: bool =
            self.db
                .query_row("SELECT imported FROM runs WHERE id=?", [&a.run_id], |r| {
                    r.get(0)
                })?;
        ensure!(!imported, "historical run is read-only");
        self.reconcile()?;
        let state: String =
            self.db
                .query_row("SELECT state FROM attempts WHERE id=?", [aid], |r| r.get(0))?;
        ensure!(
            matches!(state.as_str(), "running" | "cancelling"),
            "cannot cancel attempt in state {state}"
        );
        self.request_cancel(&a, &dir, &boot_id()?)?;
        Ok(json!({"attempt_id":aid,"state":"cancelling"}))
    }
    fn request_cancel(&mut self, a: &Attempt, dir: &Path, boot: &str) -> Result<()> {
        let identity: Identity = read_json(&dir.join("identity.json"))?;
        identity.verify(&a.attempt_id, boot)?;
        File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("cancel"))?
            .sync_all()?;
        self.attempt_state(&a.attempt_id, &a.stage_id, "cancelling", None)
    }
    pub fn recover(&mut self, aid: &str, confirm_stopped: bool) -> Result<Value> {
        ensure!(
            confirm_stopped,
            "recovery requires confirm_stopped=true after verifying the runner and all descendants have exited"
        );
        let (a, _) = self.attempt(aid)?;
        let imported: bool =
            self.db
                .query_row("SELECT imported FROM runs WHERE id=?", [&a.run_id], |r| {
                    r.get(0)
                })?;
        ensure!(!imported, "historical run is read-only");
        // Consume newly available exit evidence before accepting a manual resolution.
        self.reconcile()?;
        let state: String =
            self.db
                .query_row("SELECT state FROM attempts WHERE id=?", [aid], |r| r.get(0))?;
        ensure!(
            state == "needs_review",
            "recover requires a needs_review attempt (state {state})"
        );
        self.attempt_state(
            aid,
            &a.stage_id,
            "cancelled",
            Some("manually recovered: operator confirmed runner and all descendants have exited"),
        )?;
        Ok(json!({"attempt_id":aid,"stage_id":a.stage_id,"state":"cancelled"}))
    }

    pub fn retry(&mut self, sid: &str) -> Result<Value> {
        let (state, imported): (String, bool) = self
            .db
            .query_row(
                "SELECT s.state,r.imported FROM stages s JOIN runs r ON s.run=r.id WHERE s.id=?",
                [sid],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .context("stage not found")?;
        ensure!(
            !imported && matches!(state.as_str(), "failed" | "cancelled"),
            "retry requires a failed or cancelled local stage (state {state})"
        );
        let stopped: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_control c JOIN stages s ON s.run=c.run
             WHERE s.id=? AND c.failure_attempt IS NOT NULL)",
            [sid],
            |r| r.get(0),
        )?;
        ensure!(
            !stopped,
            "run stopped after a stage failure; use retry-run after cleanup finishes"
        );
        self.stage_state(sid, "queued", None)?;
        Ok(json!({"stage_id":sid,"state":"queued"}))
    }

    pub fn retry_run(&mut self, run: &str) -> Result<Value> {
        self.reconcile()?;
        ensure!(
            self.run_failure(run)?.is_some(),
            "retry-run requires a failed local run"
        );
        let active: bool = self.db.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM attempts a JOIN stages s ON s.id=a.stage
             WHERE s.run=? AND a.state IN {ACTIVE})"
            ),
            [run],
            |r| r.get(0),
        )?;
        ensure!(
            !active,
            "run cleanup is not finished; inspect active attempts before retry-run"
        );
        let tx = self.db.transaction()?;
        tx.execute("UPDATE run_control SET generation=generation+1,failure_attempt=NULL,failure_stage=NULL,failure_reason=NULL WHERE run=?", [run])?;
        let count = tx.execute(
            "UPDATE stages SET state='queued',reason=NULL WHERE run=? AND state!='succeeded'",
            [run],
        )?;
        tx.commit()?;
        Ok(json!({"run_id":run,"state":"queued","requeued_stages":count}))
    }

    pub fn run_failure(&self, run: &str) -> Result<Option<Value>> {
        Ok(self.db.query_row(
            "SELECT c.generation,c.failure_attempt,c.failure_stage,c.failure_reason,s.name
             FROM run_control c JOIN stages s ON s.id=c.failure_stage WHERE c.run=?",
            [run], |r| Ok(json!({"generation":r.get::<_,i64>(0)?,"attempt_id":r.get::<_,String>(1)?,
                "stage_id":r.get::<_,String>(2)?,"reason":r.get::<_,String>(3)?,"stage":r.get::<_,String>(4)?}))
        ).optional()?)
    }
}

fn fail_run(db: &rusqlite::Connection, root: &Path, a: &Attempt, reason: &str) -> Result<()> {
    let first = db.execute(
        "UPDATE run_control SET failure_attempt=?2,failure_stage=?3,failure_reason=?4
         WHERE run=?1 AND failure_attempt IS NULL",
        params![a.run_id, a.attempt_id, a.stage_id, reason],
    )?;
    if first == 0 {
        return Ok(());
    }
    db.execute("UPDATE stages SET state='skipped',reason=?2 WHERE run=?1 AND state IN ('queued','blocked')",
        params![a.run_id,format!("run stopped after stage {} failed: {reason}",a.stage.name)])?;
    let cleanup: bool = db.query_row(&format!(
        "SELECT EXISTS(SELECT 1 FROM attempts a JOIN stages s ON s.id=a.stage WHERE s.run=? AND a.state IN {ACTIVE})"),
        [&a.run_id], |r| r.get(0))?;
    crate::wake::enqueue(
        db,
        root,
        &a.run_id,
        "run.failed",
        json!({
            "stage_id":a.stage_id,"stage":a.stage.name,"attempt_id":a.attempt_id,"reason":reason,
            "cleanup_pending":cleanup
        }),
    )
}
fn allocation_state<'a>(device: &'a gpu::Device, reserved: &HashSet<&str>) -> &'a str {
    if device.state == "available"
        && device
            .uuid
            .as_deref()
            .is_some_and(|id| reserved.contains(id))
    {
        "reserved"
    } else {
        &device.state
    }
}
pub fn select_gpus(
    resources: &Resources,
    snap: &gpu::Snapshot,
    reserved: &HashSet<&str>,
) -> Vec<String> {
    snap.devices
        .iter()
        .filter(|d| allocation_state(d, reserved) == "available")
        .filter_map(|d| d.uuid.as_ref())
        .filter(|id| {
            resources
                .allowed_gpus
                .as_ref()
                .is_none_or(|pool| pool.contains(id))
        })
        .take(resources.gpus)
        .cloned()
        .collect()
}
