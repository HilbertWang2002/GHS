use crate::{
    model::*,
    read_json,
    store::{Store, column, insert_run},
};
use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

fn visit_records<T: DeserializeOwned>(
    file: &Path,
    finished: bool,
    mut visit: impl FnMut(T, i64) -> Result<()>,
) -> Result<()> {
    let f = match File::open(file) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let mut reader = BufReader::new(f);
    let mut buf = Vec::new();
    let mut line = 0;
    loop {
        buf.clear();
        if reader.read_until(b'\n', &mut buf)? == 0 {
            break;
        }
        if !finished && !buf.ends_with(b"\n") {
            break;
        }
        line += 1;
        let record = serde_json::from_slice(&buf)
            .with_context(|| format!("{} line {line}", file.display()))?;
        visit(record, line)?;
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct Event {
    version: u32,
    seq: u64,
    time: u64,
    attempt_id: String,
    stage_id: String,
    completed: f64,
    total: f64,
    unit: String,
}
impl Event {
    fn validate(&self, a: &Attempt, prev: u64, dir: &Path) -> Result<()> {
        ensure!(
            self.version == 1
                && self.attempt_id == a.attempt_id
                && self.stage_id == a.stage_id
                && self.seq > prev
                && self.completed >= 0.
                && self.total >= self.completed
                && !self.unit.is_empty(),
            "invalid progress event {} in {}",
            self.seq,
            dir.display()
        );
        Ok(())
    }
    fn remaining_seconds(&self, previous: Option<&Self>) -> Option<u64> {
        if self.completed == self.total && self.total > 0. {
            return Some(0);
        }
        let prev = previous?;
        if prev.unit != self.unit
            || prev.total != self.total
            || self.time <= prev.time
            || self.completed <= prev.completed
        {
            return None;
        }
        let seconds = ((self.total - self.completed) / (self.completed - prev.completed))
            * ((self.time - prev.time) as f64 / 1000.);
        (seconds < u64::MAX as f64).then(|| seconds.ceil() as u64)
    }
}
fn visit_events(
    dir: &Path,
    a: &Attempt,
    finished: bool,
    mut visit: impl FnMut(Event) -> Result<()>,
) -> Result<()> {
    let mut prev = 0;
    visit_records(&dir.join("events.jsonl"), finished, |e: Event, _| {
        e.validate(a, prev, dir)?;
        prev = e.seq;
        visit(e)
    })
}
// Status only needs the last two published records. Read backwards from a fixed
// end offset so a concurrent partial append is never mistaken for a sample.
pub fn live_progress(dir: &Path, a: &Attempt) -> Result<(Option<Value>, Option<u64>)> {
    let path = dir.join("events.jsonl");
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((None, None)),
        Err(e) => return Err(e).with_context(|| path.display().to_string()),
    };
    let mut pos = file.metadata()?.len();
    let mut buffer = [0; 8192];
    let mut breaks = Vec::with_capacity(3);
    while pos > 0 && breaks.len() < 3 {
        let n = pos.min(buffer.len() as u64) as usize;
        pos -= n as u64;
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut buffer[..n])?;
        breaks.extend(
            buffer[..n]
                .iter()
                .enumerate()
                .rev()
                .filter(|(_, c)| **c == b'\n')
                .map(|(i, _)| pos + i as u64)
                .take(3 - breaks.len()),
        );
    }
    let Some(&end) = breaks.first() else {
        return Ok((None, None));
    };
    let start = breaks.get(2).map_or(0, |pos| pos + 1);
    file.seek(SeekFrom::Start(start))?;
    let mut previous: Option<Event> = None;
    let mut eta = None;
    for record in BufReader::new(file.take(end + 1 - start)).split(b'\n') {
        let e: Event = serde_json::from_slice(&record?)
            .with_context(|| format!("invalid progress in {}", path.display()))?;
        e.validate(a, previous.as_ref().map_or(0, |p| p.seq), dir)?;
        eta = e.remaining_seconds(previous.as_ref());
        previous = Some(e);
    }
    Ok((previous.map(serde_json::to_value).transpose()?, eta))
}
pub fn latest_event(dir: &Path, a: &Attempt, finished: bool) -> Result<Option<Value>> {
    let mut last = None;
    visit_events(dir, a, finished, |e| {
        last = Some(e);
        Ok(())
    })?;
    Ok(last.map(serde_json::to_value).transpose()?)
}
pub fn events(dir: &Path, a: &Attempt, after: u64, finished: bool) -> Result<Vec<Value>> {
    let mut out = vec![];
    visit_events(dir, a, finished, |e| {
        if e.seq > after {
            out.push(serde_json::to_value(e)?);
        }
        Ok(())
    })?;
    Ok(out)
}
pub fn read_log(dir: &Path, offset: u64, length: u64) -> Result<Value> {
    let mut f = File::open(dir.join("output.log"))
        .with_context(|| format!("read log at {}", dir.display()))?;
    ensure!(offset <= f.metadata()?.len(), "offset past end of log");
    f.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![];
    f.take(length).read_to_end(&mut bytes)?;
    Ok(
        json!({"path":dir.join("output.log"),"offset":offset,"next_offset":offset+bytes.len() as u64,"text":std::str::from_utf8(&bytes).ok(),"base64":STANDARD.encode(bytes)}),
    )
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Metric {
    version: u32,
    metric: String,
    value: f64,
    source: String,
    unit: Option<String>,
    direction: Option<String>,
    location: Option<String>,
    group: Option<String>,
    protocol: Option<String>,
}
impl Metric {
    fn differences(&self, other: &Self) -> Vec<String> {
        [
            ("unit", &self.unit, &other.unit),
            ("location", &self.location, &other.location),
            ("protocol", &self.protocol, &other.protocol),
            ("direction", &self.direction, &other.direction),
        ]
        .into_iter()
        .filter(|(_, x, y)| x != y)
        .map(|(label, x, y)| format!("{label} mismatch: {x:?} vs {y:?}"))
        .collect()
    }
}
fn collect_files(
    root: &Path,
    relative: &Path,
    out: &mut Vec<(PathBuf, &'static str)>,
) -> Result<()> {
    for ent in fs::read_dir(root.join(relative))? {
        let ent = ent?;
        let path = relative.join(ent.file_name());
        let ty = ent.file_type()?;
        if ty.is_dir() {
            collect_files(root, &path, out)?;
        } else if ty.is_file() || ty.is_symlink() {
            out.push((path, "artifact"));
        }
    }
    Ok(())
}
fn attempt_files(dir: &Path) -> Result<Vec<(PathBuf, &'static str)>> {
    let mut files = vec![];
    for (path, kind) in [
        ("output.log", "log"),
        ("runner.log", "log"),
        ("results.jsonl", "results"),
        ("events.jsonl", "events"),
        ("exit.json", "exit"),
    ] {
        if dir.join(path).try_exists()? {
            files.push((PathBuf::from(path), kind));
        }
    }
    collect_files(dir, Path::new("artifacts"), &mut files)?;
    Ok(files)
}
fn index(db: &Connection, a: &Attempt, dir: &Path) -> Result<()> {
    db.execute("DELETE FROM metrics WHERE attempt=?", [&a.attempt_id])?;
    visit_records(&dir.join("results.jsonl"), true, |m: Metric, line| {
        ensure!(
            m.version == 1
                && !m.metric.is_empty()
                && !m.source.is_empty()
                && m.direction
                    .as_deref()
                    .is_none_or(|d| d == "min" || d == "max"),
            "invalid result in {} line {}",
            dir.display(),
            line
        );
        db.execute(
            "INSERT INTO metrics VALUES(?,?,?)",
            params![a.attempt_id, line, serde_json::to_string(&m)?],
        )?;
        Ok(())
    })?;
    db.execute("DELETE FROM artifacts WHERE attempt=?", [&a.attempt_id])?;
    for (path, kind) in attempt_files(dir)? {
        db.execute(
            "INSERT INTO artifacts VALUES(?,?,?)",
            params![
                a.attempt_id,
                path.to_str().context("non-UTF8 artifact")?,
                kind
            ],
        )?;
    }
    db.execute(
        "UPDATE attempts SET index_error=NULL WHERE id=?",
        [&a.attempt_id],
    )?;
    Ok(())
}

/// Refresh one attempt atomically. A failed rebuild invalidates its old observations.
pub fn refresh_index(
    tx: &mut rusqlite::Transaction<'_>,
    a: &Attempt,
    dir: &Path,
) -> Result<Option<String>> {
    let savepoint = tx.savepoint()?;
    match index(&savepoint, a, dir) {
        Ok(()) => {
            savepoint.commit()?;
            Ok(None)
        }
        Err(e) => {
            savepoint.finish()?;
            let error = format!("attempt {}: {e:#}", a.attempt_id);
            tx.execute(
                "UPDATE attempts SET index_error=? WHERE id=?",
                params![error, a.attempt_id],
            )?;
            Ok(Some(error))
        }
    }
}

impl Store {
    pub fn import(
        &mut self,
        path: &Path,
        purpose: Option<Purpose>,
        source: Option<&str>,
    ) -> Result<Value> {
        let path = path.canonicalize()?;
        let mut m: RunRecord = read_json(&path.join("run.json"))?;
        ensure!(m.version == 1, "unsupported run version");
        component(&m.run_id)?;
        component(&m.batch_id)?;
        let purpose_source = match (m.purpose, purpose) {
            (Some(p), q) => {
                ensure!(q.is_none_or(|q| q == p), "purpose conflicts with run.json");
                "run.json"
            }
            (None, Some(p)) => {
                m.purpose = Some(p);
                source
                    .filter(|s| !s.is_empty())
                    .context("purpose_source required for supplied purpose")?
            }
            (None, None) => bail!("supply historical purpose and purpose_source"),
        };
        ensure!(
            m.config.purpose.is_none_or(|p| Some(p) == m.purpose),
            "conflicting historical purposes"
        );
        Manifest {
            version: 1,
            project: m.project.clone(),
            description: String::new(),
            purpose: m.purpose,
            baseline: None,
            runs: vec![m.config.clone()],
        }
        .validate()?;
        ensure!(
            m.stage_ids.len() == m.config.stages.len(),
            "invalid historical stage IDs"
        );
        for stage in m.stage_ids.values() {
            component(stage)?;
        }
        let tx = self.db.transaction()?;
        let old: Option<(bool, String)> = tx
            .query_row(
                "SELECT imported,meta FROM runs WHERE id=?",
                [&m.run_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((imported, raw)) = old {
            ensure!(
                imported && serde_json::to_value(&m)? == serde_json::from_str::<Value>(&raw)?,
                "run ID conflict"
            );
            tx.execute("DELETE FROM metrics WHERE attempt IN (SELECT a.id FROM attempts a JOIN stages s ON s.id=a.stage WHERE s.run=?)",[&m.run_id])?;
            tx.execute("DELETE FROM artifacts WHERE attempt IN (SELECT a.id FROM attempts a JOIN stages s ON s.id=a.stage WHERE s.run=?)",[&m.run_id])?;
            tx.execute(
                "DELETE FROM attempts WHERE stage IN (SELECT id FROM stages WHERE run=?)",
                [&m.run_id],
            )?;
            tx.execute("UPDATE stages SET state='blocked',reason='historical stage has no completed attempt' WHERE run=?",[&m.run_id])?;
            tx.execute(
                "UPDATE runs SET path=?,purpose_source=? WHERE id=?",
                params![
                    path.to_str().context("non-UTF8 path")?,
                    purpose_source,
                    m.run_id
                ],
            )?;
        } else {
            insert_run(&tx, &m, &path, true, purpose_source)?;
        }
        let mut attempts = vec![];
        for ent in fs::read_dir(path.join("attempts"))? {
            let ent = ent?;
            let dir = ent.path();
            let a: Attempt = read_json(&dir.join("attempt.json"))?;
            ensure!(
                a.version == 1
                    && a.number > 0
                    && a.run_id == m.run_id
                    && Some(a.purpose) == m.purpose
                    && m.stage_ids.get(&a.stage.name) == Some(&a.stage_id)
                    && ent.file_name().to_str() == Some(&a.attempt_id),
                "invalid attempt identity at {}",
                dir.display()
            );
            attempts.push((a, dir));
        }
        attempts.sort_by_key(|(a, _)| a.number);
        for (a, dir) in attempts {
            let exit = if dir.join("exit.json").try_exists()? {
                Some(read_json::<Exit>(&dir.join("exit.json"))?)
            } else {
                None
            };
            ensure!(
                exit.as_ref()
                    .is_none_or(|e| e.version == 1 && e.attempt_id == a.attempt_id),
                "exit identity mismatch"
            );
            let state = exit.as_ref().map_or("blocked", Exit::state);
            tx.execute(
                "INSERT INTO attempts(id,stage,number,spec,state,exit) VALUES(?,?,?,?,?,?)",
                params![
                    a.attempt_id,
                    a.stage_id,
                    a.number,
                    serde_json::to_string(&a)?,
                    state,
                    exit.as_ref().map(serde_json::to_string).transpose()?
                ],
            )?;
            tx.execute(
                "UPDATE stages SET state=?,reason=? WHERE id=?",
                params![
                    state,
                    if exit.is_none() {
                        Some("historical attempt missing exit record")
                    } else {
                        None
                    },
                    a.stage_id
                ],
            )?;
            index(&tx, &a, &dir)?;
        }
        tx.commit()?;
        self.get_run(&m.run_id)
    }
    pub fn reindex(&mut self, rid: Option<&str>) -> Result<Value> {
        if let Some(run) = rid {
            self.meta(run)?;
        }
        let ids = column(
            &self.db,
            "SELECT a.id FROM attempts a JOIN stages s ON s.id=a.stage JOIN runs r ON r.id=s.run WHERE (a.exit IS NOT NULL OR r.imported=1) AND (?1 IS NULL OR s.run=?1)",
            [rid],
        )?;
        let entries = ids
            .iter()
            .map(|id| self.attempt(id))
            .collect::<Result<Vec<_>>>()?;
        let mut tx = self.db.transaction()?;
        let mut errors = vec![];
        for (a, dir) in entries {
            if let Some(error) = refresh_index(&mut tx, &a, &dir)? {
                errors.push(error);
            }
        }
        tx.commit()?;
        ensure!(
            errors.is_empty(),
            "index rebuild errors: {}",
            errors.join("\n")
        );
        Ok(json!({"indexed_attempt_ids":ids}))
    }
    pub fn artifacts(&self, rid: &str, aid: Option<&str>) -> Result<Value> {
        self.meta(rid)?;
        if let Some(id) = aid {
            ensure!(
                self.attempt(id)?.0.run_id == rid,
                "attempt belongs to a different run"
            );
        }
        let ids = column(
            &self.db,
            "SELECT a.id FROM attempts a JOIN stages s ON a.stage=s.id WHERE s.run=?1 AND (?2 IS NULL OR a.id=?2) ORDER BY a.rowid",
            params![rid, aid],
        )?;
        let mut out = vec![];
        let mut errors = vec![];
        for id in ids {
            let (_, dir) = self.attempt(&id)?;
            let (exit, error): (Option<String>, Option<String>) = self.db.query_row(
                "SELECT exit,index_error FROM attempts WHERE id=?",
                [&id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if let Some(e) = &error {
                errors.push(json!({"attempt_id":id,"error":e}));
            }
            let files = if exit.is_none() {
                attempt_files(&dir)?
                    .into_iter()
                    .map(|(p, k)| (p, k.to_owned()))
                    .collect()
            } else {
                let mut q = self
                    .db
                    .prepare("SELECT path,kind FROM artifacts WHERE attempt=? ORDER BY path")?;
                q.query_map([&id], |r| {
                    Ok((
                        PathBuf::from(r.get::<_, String>(0)?),
                        r.get::<_, String>(1)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
            };
            for (p, k) in files {
                out.push(json!({"run_id":rid,"attempt_id":id,"kind":k,"relative_path":p,"path":dir.join(p)}));
            }
        }
        Ok(json!({"artifacts":out,"index_errors":errors}))
    }
}
#[derive(Clone)]
struct Observation {
    stage: String,
    m: Metric,
    source: Value,
}
struct Report {
    run: String,
    purpose: Purpose,
    baseline: Option<String>,
    rows: Vec<Observation>,
    issues: Vec<String>,
    eligible: bool,
}
impl Store {
    fn report(&self, rid: &str, purpose: Purpose) -> Result<Report> {
        let m = self.meta(rid)?;
        let mut report = Report {
            run: rid.into(),
            purpose: m.purpose.context("missing purpose")?,
            baseline: m.baseline_id,
            rows: vec![],
            issues: vec![],
            eligible: true,
        };
        if report.purpose != purpose {
            report.eligible = false;
            report.issues.push(format!(
                "purpose is {}, selected {}",
                report.purpose.name(),
                purpose.name()
            ));
        }
        for (name, sid) in m.stage_ids {
            let state: String =
                self.db
                    .query_row("SELECT state FROM stages WHERE id=?", [&sid], |r| r.get(0))?;
            if state != "succeeded" {
                report.eligible = false;
                report.issues.push(format!("stage {name} is {state}"));
                continue;
            }
            let (aid, error): (String, Option<String>) = self.db.query_row(
                "SELECT id,index_error FROM attempts WHERE stage=? ORDER BY number DESC LIMIT 1",
                [&sid],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if let Some(error) = error {
                report.eligible = false;
                report.issues.push(error);
                continue;
            }
            let mut q = self
                .db
                .prepare("SELECT line,record FROM metrics WHERE attempt=? ORDER BY line")?;
            let dir = self.attempt(&aid)?.1;
            for row in q.query_map([&aid], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })? {
                let (line, raw) = row?;
                let m: Metric = serde_json::from_str(&raw)?;
                let source = json!({"run_id":rid,"stage_id":sid,"attempt_id":aid,"path":dir.join("results.jsonl"),"line":line,"source":m.source});
                report.rows.push(Observation {
                    stage: name.clone(),
                    m,
                    source,
                });
            }
        }
        Ok(report)
    }
    pub fn compare(
        &self,
        ids: &[String],
        batch: Option<&str>,
        purpose: Purpose,
        metrics: &[String],
        group: Option<&str>,
        baseline: Option<&str>,
    ) -> Result<Value> {
        let selected = if ids.is_empty() {
            column(
                &self.db,
                "SELECT id FROM runs WHERE json_extract(meta,'$.purpose')=?1 AND (?2 IS NULL OR json_extract(meta,'$.batch_id')=?2) ORDER BY rowid",
                params![purpose.name(), batch],
            )?
        } else {
            ids.to_vec()
        };
        let mut reports = BTreeMap::new();
        let mut keys = BTreeSet::new();
        for rid in &selected {
            let r = self.report(rid, purpose)?;
            if let Some(b) = batch {
                ensure!(self.meta(rid)?.batch_id == b, "run {rid} outside batch");
            }
            let base = baseline.map(str::to_owned).or(r.baseline.clone());
            reports.insert(rid.clone(), r);
            if let Some(id) = base
                && !reports.contains_key(&id)
            {
                let exists: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM runs WHERE id=?)",
                    [&id],
                    |row| row.get(0),
                )?;
                if exists {
                    reports.insert(id.clone(), self.report(&id, purpose)?);
                }
            }
        }
        for r in reports.values() {
            for o in &r.rows {
                if (metrics.is_empty() || metrics.contains(&o.m.metric))
                    && group.is_none_or(|g| o.m.group.as_deref() == Some(g))
                {
                    keys.insert((o.m.metric.clone(), o.m.group.clone()));
                }
            }
        }
        for m in metrics {
            if !keys.iter().any(|(k, _)| k == m) {
                keys.insert((m.clone(), group.map(str::to_owned)));
            }
        }
        let mut rows = vec![];
        let mut runs = vec![];
        let scope = &selected;
        for id in &selected {
            let r = &reports[id];
            let mut issues = r.issues.clone();
            let base_id = baseline.or(r.baseline.as_deref());
            let base = base_id.and_then(|b| reports.get(b));
            let mut included = false;
            for (metric, g) in &keys {
                let matches = |o: &&Observation| o.m.metric == *metric && o.m.group == *g;
                let observations: Vec<_> = r.rows.iter().filter(matches).collect();
                if observations.is_empty() {
                    let reason = if r.issues.is_empty() {
                        "missing result".into()
                    } else {
                        r.issues.join(", ")
                    };
                    rows.push(json!({
                        "run_id": id, "purpose": r.purpose, "metric": metric, "group": g,
                        "value":null, "delta":null, "included":false, "reason":reason
                    }));
                    continue;
                }
                for o in observations {
                    let mut reasons = r.issues.clone();
                    if let Some(id) = base_id
                        && base.is_none()
                    {
                        reasons.push(format!("baseline run {id} is not registered"));
                    }
                    if base_id.is_none() {
                        for other_id in scope {
                            let other = &reports[other_id];
                            if other.eligible {
                                for other_row in other
                                    .rows
                                    .iter()
                                    .filter(matches)
                                    .filter(|x| x.stage == o.stage)
                                {
                                    for diff in o.m.differences(&other_row.m) {
                                        let reason = format!("run {other_id}: {diff}");
                                        if !reasons.contains(&reason) {
                                            reasons.push(reason);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let mut delta = None;
                    let mut base_source = None;
                    let mut base_value = None;
                    if let Some(b) = base {
                        let candidates: Vec<_> = b
                            .rows
                            .iter()
                            .filter(matches)
                            .filter(|x| x.stage == o.stage)
                            .collect();
                        if !b.eligible {
                            reasons.push(format!("baseline: {}", b.issues.join(", ")));
                        }
                        match candidates.as_slice() {
                            [other] => {
                                base_source = Some(&other.source);
                                base_value = Some(other.m.value);
                                reasons.extend(o.m.differences(&other.m));
                                if reasons.is_empty() {
                                    let d = o.m.value - other.m.value;
                                    ensure!(d.is_finite(), "metric delta overflow");
                                    delta = Some(d);
                                }
                            }
                            [] => reasons.push("missing baseline observation".into()),
                            _ => reasons.push("ambiguous baseline observations".into()),
                        }
                    }
                    included |= reasons.is_empty();
                    for reason in &reasons {
                        if !issues.contains(reason) {
                            issues.push(reason.clone());
                        }
                    }
                    rows.push(json!({
                        "run_id": id, "purpose": r.purpose, "stage": o.stage, "metric": metric,
                        "group": g, "value": o.m.value, "unit": o.m.unit, "location": o.m.location,
                        "protocol": o.m.protocol, "direction": o.m.direction, "source": o.source,
                        "baseline_id": base_id, "baseline_value": base_value, "baseline_source": base_source,
                        "delta": delta, "included": reasons.is_empty(), "reasons": reasons
                    }));
                }
            }
            if !r
                .rows
                .iter()
                .any(|o| keys.contains(&(o.m.metric.clone(), o.m.group.clone())))
            {
                issues.push("missing selected results".into());
            }
            runs.push(
                json!({"run_id":r.run,"local_id":self.local_id(&r.run)?,"purpose":r.purpose,"included":included,"reasons":issues}),
            );
        }
        Ok(json!({"purpose":purpose,"runs":runs,"observations":rows}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn status_estimates_from_complete_matching_samples_at_the_tail() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("ghs-progress-{}", crate::id()?));
        fs::create_dir(&dir)?;
        let a: Attempt = serde_json::from_value(
            json!({"version":1,"attempt_id":"a","stage_id":"s",
            "run_id":"r","purpose":"experiment","number":1,"stage":{"name":"train","command":["true"]},"gpus":[],"created_at":0}),
        )?;
        assert_eq!(live_progress(&dir, &a)?, (None, None));
        let mut file = File::create(dir.join("events.jsonl"))?;
        let sample = |seq, time, completed, total| {
            json!({"version":1,"attempt_id":"a","stage_id":"s",
            "seq":seq,"time":time,"completed":completed,"total":total,"unit":"steps"})
        };
        writeln!(file, "{}", sample(1, 2000, 10, 100))?;
        assert_eq!(live_progress(&dir, &a)?.1, None);
        writeln!(file, "{}", sample(2, 4000, 20, 100))?;
        assert_eq!(live_progress(&dir, &a)?.1, Some(16));
        // A long unfinished last record may cross multiple read blocks.
        let mut third = sample(3, 6000, 20, 100);
        third["detail"] = "x".repeat(20000).into();
        write!(file, "{third}")?;
        let (last, eta) = live_progress(&dir, &a)?;
        assert_eq!(last.unwrap()["seq"], 2);
        assert_eq!(eta, Some(16));
        writeln!(file)?;
        assert_eq!(live_progress(&dir, &a)?.1, None);
        writeln!(file, "{}", sample(4, 8000, 25, 200))?;
        assert_eq!(live_progress(&dir, &a)?.1, None);
        writeln!(file, "{}", sample(5, 10000, 200, 200))?;
        assert_eq!(live_progress(&dir, &a)?.1, Some(0));
        writeln!(file, "invalid")?;
        assert!(live_progress(&dir, &a).is_err());
        fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
