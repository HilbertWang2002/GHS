use anyhow::{Context, Result, bail};
use comfy_table::{CellAlignment, Table, presets::NOTHING};
use ghs::api::Request;
use serde_json::Value;
use std::collections::BTreeSet;
use time::{OffsetDateTime, UtcOffset};

fn table(headers: &[&str]) -> Table {
    let mut table = Table::new();
    table.load_preset(NOTHING).set_header(headers.to_vec());
    table
}

fn right_align(table: &mut Table, indices: &[usize]) {
    for &i in indices {
        table
            .column_mut(i)
            .expect("table header column")
            .set_cell_alignment(CellAlignment::Right);
    }
}

fn rows<'a>(v: &'a Value, field: &str) -> Result<&'a [Value]> {
    v[field]
        .as_array()
        .map(Vec::as_slice)
        .with_context(|| format!("daemon reply missing array {field}"))
}

fn cell(v: &Value) -> String {
    match v {
        Value::String(s) => {
            let mut out = String::with_capacity(s.len());
            for c in s.chars() {
                if c.is_control() {
                    out.extend(c.escape_default());
                } else {
                    out.push(c);
                }
            }
            out
        }
        Value::Null => "—".into(),
        Value::Number(n) if n.is_f64() => n.to_string().trim_end_matches(".0").to_owned(),
        _ => v.to_string(),
    }
}

fn state(v: &Value) -> String {
    match v.as_str() {
        Some("needs_review") => "待核查",
        Some("running") => "运行中",
        Some("queued") => "排队中",
        Some("blocked") => "已阻塞",
        Some("starting") => "启动中",
        Some("cancelling") => "取消中",
        Some("succeeded") => "已完成",
        Some("failed") => "失败",
        Some("failing") => "失败清理中",
        Some("skipped") => "已跳过",
        Some("cancelled") => "已取消",
        _ => return cell(v),
    }
    .to_owned()
}

fn purpose(v: &Value) -> String {
    match v.as_str() {
        Some("experiment") => "实验".into(),
        Some("validation") => "验证".into(),
        _ => cell(v),
    }
}

fn local_datetime(millis: u64) -> Result<OffsetDateTime> {
    let dt = OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)?;
    Ok(dt.to_offset(UtcOffset::local_offset_at(dt)?))
}

fn clock(ms: &Value) -> Result<String> {
    let dt = local_datetime(ms.as_u64().context("missing timestamp")?)?;
    Ok(format!(
        "{:02}:{:02}:{:02}",
        dt.hour(),
        dt.minute(),
        dt.second()
    ))
}

fn duration(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m {:02}s", seconds / 60, seconds % 60),
        3600..86400 => format!("{}h {:02}m", seconds / 3600, seconds % 3600 / 60),
        _ => format!("{}d {:02}h", seconds / 86400, seconds % 86400 / 3600),
    }
}

fn gib(v: &Value) -> String {
    match v.as_u64() {
        None => "—".into(),
        Some(0) => "0".into(),
        Some(n) => {
            let g = n as f64 / (1u64 << 30) as f64;
            if g < 0.01 {
                "<0.01".into()
            } else if g < 1.0 {
                format!("{g:.2}")
            } else {
                format!("{g:.1}")
            }
        }
    }
}

fn percent(v: &Value) -> String {
    v.as_f64()
        .map_or_else(|| "—".into(), |n| format!("{n:.0}%"))
}

fn work(v: &Value) -> String {
    if v.is_null() {
        return "—".into();
    }
    let unit = match v["unit"].as_str() {
        Some("step" | "steps" | "iteration" | "iterations" | "iter" | "iters") => "步".into(),
        _ => cell(&v["unit"]),
    };
    format!("{}/{} {unit}", cell(&v["completed"]), cell(&v["total"]))
}

fn attempt_work(a: &Value) -> String {
    if a["progress_error"].is_string() {
        "进度错误".into()
    } else {
        work(&a["progress"])
    }
}

fn eta(a: &Value) -> String {
    a["eta_seconds"]
        .as_u64()
        .map_or_else(|| "—".into(), duration)
}

fn gpu_state(device: &Value, attempts: &[&Value]) -> Result<String> {
    let state = device["state"].as_str().context("missing GPU state")?;
    if state == "needs_review" || attempts.iter().any(|a| a["state"] == "needs_review") {
        return Ok("CHECK".into());
    }
    if !attempts.is_empty() {
        if attempts.iter().any(|a| a["state"] == "running") {
            return Ok("RUNNING".into());
        }
        return Ok(attempts
            .iter()
            .map(|a| cell(&a["state"]).to_ascii_uppercase())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join("/"));
    }
    Ok(match state {
        "available" | "quota_blocked" => "IDLE",
        "outside_pool" => "OUTSIDE",
        "external_blocked" => "EXTERNAL",
        other => anyhow::bail!("unexpected GPU state {other}"),
    }
    .to_owned())
}

pub fn status(v: &Value) -> Result<String> {
    let devices = rows(v, "devices")?;
    let counts = &v["run_counts"];
    let mut lines = vec![format!(
        "运行中 {} 个实验   排队中 {} 个实验   {}",
        counts["running"]
            .as_u64()
            .context("missing running count")?,
        counts["queued"].as_u64().context("missing queued count")?,
        clock(&v["sampled_at"])?
    )];
    let active = v["active_attempts"]
        .as_object()
        .context("missing active attempts")?;
    let owner_attempt = |owner: &Value| -> Result<&Value> {
        let id = owner["attempt_id"]
            .as_str()
            .context("missing owner attempt ID")?;
        active
            .get(id)
            .with_context(|| format!("missing active attempt {id}"))
    };
    let shared_model = devices
        .first()
        .filter(|d| devices.iter().all(|other| other["name"] == d["name"]));
    let mut headers = vec![
        "GPU",
        "STATE",
        "利用率",
        "显存(G) 用/总",
        "外部%",
        "实验",
        "进度",
        "预计剩余",
    ];
    if let Some(d) = shared_model {
        lines.push(cell(&d["name"]));
    } else if !devices.is_empty() {
        headers.push("型号");
    }
    let mut gpu_table = table(&headers);
    right_align(&mut gpu_table, &[0, 2, 3, 4, 6, 7]);
    let mut processes = table(&["GPU", "PID", "程序", "显存(G)", "归属"]);
    right_align(&mut processes, &[0, 1, 3]);
    let mut has_processes = false;
    for d in devices {
        let gpu = cell(&d["index"]);
        let mut attempts = Vec::new();
        let mut seen = BTreeSet::new();
        let owners = rows(d, "reservations")?.iter().chain(
            d["processes"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|p| &p["owner"])
                .filter(|o| !o.is_null()),
        );
        for owner in owners {
            if seen.insert(owner["attempt_id"].as_str().context("missing attempt ID")?) {
                attempts.push(owner_attempt(owner)?);
            }
        }
        let values = |f: fn(&Value) -> String| {
            if attempts.is_empty() {
                "—".into()
            } else {
                attempts.iter().map(|a| f(a)).collect::<Vec<_>>().join(", ")
            }
        };
        let mut row = vec![
            gpu.clone(),
            gpu_state(d, &attempts)?,
            percent(&d["utilization_percent"]),
            format!("{} / {}", gib(&d["used_bytes"]), gib(&d["total_bytes"])),
            percent(&d["external_percent"]),
            values(|a| cell(&a["local_id"])),
            values(attempt_work),
            values(eta),
        ];
        if shared_model.is_none() {
            row.push(cell(&d["name"]));
        }
        gpu_table.add_row(row);
        if let Some(ps) = d["processes"].as_array() {
            for p in ps {
                let owner = if p["ownership"] == "managed" {
                    let a = owner_attempt(&p["owner"])?;
                    format!("{} {}", cell(&a["local_id"]), cell(&a["stage"]))
                } else {
                    cell(&p["ownership"]).to_ascii_uppercase()
                };
                processes.add_row([
                    gpu.clone(),
                    cell(&p["pid"]),
                    cell(&p["name"]),
                    gib(&p["memory_bytes"]),
                    owner,
                ]);
                has_processes = true;
            }
        }
    }
    if devices.is_empty() {
        lines.push("未发现 GPU。".into());
    } else {
        lines.push(gpu_table.to_string());
    }
    let mut cpu = table(&["实验", "CPU 阶段", "状态", "进度", "预计剩余"]);
    right_align(&mut cpu, &[3, 4]);
    for a in active
        .values()
        .filter(|a| a["gpus"].as_array().is_some_and(Vec::is_empty))
    {
        cpu.add_row([
            cell(&a["local_id"]),
            cell(&a["stage"]),
            state(&a["state"]),
            attempt_work(a),
            eta(a),
        ]);
    }
    if cpu.row_count() > 0 {
        lines.push(String::new());
        lines.push(cpu.to_string());
    }
    if has_processes {
        lines.push(String::new());
        lines.push(processes.to_string());
    } else if devices.iter().any(|d| d["processes"].is_array()) {
        lines.push("当前没有可见的 GPU 进程。".into());
    }
    Ok(lines.join("\n"))
}

// Scheduler diagnostics retain per-device evidence in JSON. Show the admission decision in the queue.
fn wait_reason(reason: &str) -> String {
    if let Some(req) = reason.strip_prefix("needs ") {
        if let Some((count, tail)) = req.split_once(" GPU(s), ") {
            if let Some((eligible, _)) = tail.split_once(" eligible under allowed_gpus ") {
                return format!("需要 {count} 张 GPU，当前可分配 {eligible} 张");
            }
            if let Some((used, max)) = tail.split_once(" reserved, scheduler max_gpus = ") {
                return format!("需要 {count} 张 GPU，已预留 {used}/{max} 张");
            }
        }
        if let Some((count, used)) = req.split_once(" CPU slots, ")
            && let Some(slots) = used.strip_suffix(" reserved")
        {
            return format!("需要 {count} 个 CPU 槽，已预留 {slots}");
        }
    }
    if let Some(deps) = reason.strip_prefix("waiting for ") {
        return format!("等待前置阶段 {deps}");
    }
    reason.to_owned()
}

fn progress(v: &Value) -> String {
    format!(
        "{} / {} {}",
        cell(&v["completed"]),
        cell(&v["total"]),
        cell(&v["unit"])
    )
}

fn exit_note(exit: &Value) -> Option<String> {
    let mut parts = vec![];
    if let Some(e) = exit["error"].as_str() {
        parts.push(e.to_owned());
    }
    if let Some(sig) = exit["signal"].as_i64() {
        parts.push(format!("终止信号 {sig}"));
    }
    if let Some(code) = exit["code"].as_i64()
        && code != 0
    {
        parts.push(format!("退出码 {code}"));
    }
    if let Some(paths) = exit["missing_artifacts"].as_array()
        && !paths.is_empty()
    {
        parts.push(format!(
            "缺少产物 {}",
            paths.iter().map(cell).collect::<Vec<_>>().join("、")
        ));
    }
    (!parts.is_empty()).then(|| parts.join("；"))
}

fn stage_note(stage: &Value) -> Result<String> {
    let latest = rows(stage, "attempts")?.last();
    if stage["state"] == "failed"
        && let Some(msg) = latest.and_then(|a| exit_note(&a["exit"]))
    {
        return Ok(msg);
    }
    if let Some(reason) = stage["reason"].as_str() {
        return Ok(wait_reason(reason));
    }
    if let Some(a) = latest
        && a["state"] == stage["state"]
        && !a["progress"].is_null()
    {
        return Ok(progress(&a["progress"]));
    }
    Ok(String::new())
}

fn active_state(v: &Value) -> bool {
    matches!(
        v.as_str(),
        Some("starting" | "running" | "cancelling" | "needs_review")
    )
}

fn label_gpus(uuids: &BTreeSet<&str>, labels: &Value) -> String {
    let mut indices = BTreeSet::new();
    let mut names = Vec::new();
    for &id in uuids {
        if let Some(i) = labels[id].as_u64() {
            indices.insert(i);
        } else {
            names.push(id.to_owned());
        }
    }
    let labels: Vec<_> = indices
        .into_iter()
        .map(|i| i.to_string())
        .chain(names)
        .collect();
    if labels.is_empty() {
        "无".into()
    } else {
        labels.join(",")
    }
}

fn allowed_gpus(stages: &[&Value], v: &Value) -> Result<String> {
    let gpu_stages: Vec<_> = stages
        .iter()
        .map(|s| &s["resources"])
        .filter(|r| r["gpus"].as_u64().is_some_and(|n| n > 0))
        .collect();
    if gpu_stages.is_empty() {
        return Ok("CPU".into());
    }
    let pool = v["gpu_pool"].as_array();
    let mut allowed = BTreeSet::new();
    for r in gpu_stages {
        match (r["allowed_gpus"].as_array(), pool) {
            (None, None) => return Ok("池内任意".into()),
            (Some(ids), _) => {
                for id in ids.iter().filter(|id| pool.is_none_or(|p| p.contains(id))) {
                    allowed.insert(id.as_str().context("invalid GPU UUID")?);
                }
            }
            (None, Some(ids)) => {
                for id in ids {
                    allowed.insert(id.as_str().context("invalid pool GPU UUID")?);
                }
            }
        }
    }
    Ok(label_gpus(&allowed, &v["gpu_indices"]))
}

fn timing(attempts: &[&Value], now: u64) -> Result<(String, String)> {
    let mut intervals = Vec::new();
    let mut first = None;
    let mut incomplete = false;
    for a in attempts {
        let start = a["created_at"]
            .as_u64()
            .context("missing attempt creation time")?;
        first = Some(first.map_or(start, |old: u64| old.min(start)));
        if let Some(end) = a["exit"]["finished_at"].as_u64() {
            intervals.push((start, end));
        } else if active_state(&a["state"]) {
            intervals.push((start, now));
        } else {
            incomplete = true;
        }
    }
    let Some(first) = first else {
        return Ok(("—".into(), "—".into()));
    };
    let date = local_datetime(first)?;
    let start = if date.date() == local_datetime(now)?.date() {
        format!(
            "{:02}:{:02}:{:02}",
            date.hour(),
            date.minute(),
            date.second()
        )
    } else {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}",
            date.year(),
            date.month() as u8,
            date.day(),
            date.hour(),
            date.minute()
        )
    };
    if incomplete {
        return Ok((start, "—".into()));
    }
    intervals.sort_unstable();
    let mut time = 0;
    let mut previous_end = 0;
    for (start, end) in intervals {
        anyhow::ensure!(end >= start, "attempt end precedes start");
        time += end.saturating_sub(start.max(previous_end));
        previous_end = previous_end.max(end);
    }
    Ok((start, duration(time / 1000)))
}

fn run_list(v: &Value, ids: bool) -> Result<String> {
    let runs = rows(v, "runs")?;
    if runs.is_empty() {
        return Ok("没有符合条件的实验。".into());
    }
    let common_project = runs.iter().all(|r| r["project"] == runs[0]["project"]);
    let mut headers = vec![
        "ID",
        "组别",
        "状态",
        "进度",
        "开始时间",
        "已用时",
        "优先级",
        "允许 GPU",
        "实际 GPU",
    ];
    if ids {
        headers.push("UUID");
    }
    let mut t = table(&headers);
    for column in t.column_iter_mut() {
        column.set_padding((0, 1));
    }
    right_align(&mut t, &[3, 5, 6]);
    let now = ghs::now();
    for r in runs {
        let stages = rows(r, "stages")?;
        let attempts: Vec<_> = stages
            .iter()
            .map(|s| rows(s, "attempts"))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        let active: Vec<_> = attempts
            .iter()
            .copied()
            .filter(|a| active_state(&a["state"]))
            .collect();
        let waiting = stages.iter().any(|s| s["state"] == "queued");
        let relevant: Vec<_> = stages
            .iter()
            .filter(|s| {
                if !active.is_empty() {
                    active_state(&s["state"])
                } else if waiting {
                    s["state"] == "queued"
                } else {
                    true
                }
            })
            .collect();
        let assigned: &[&Value] = if !active.is_empty() {
            &active
        } else if waiting {
            &[]
        } else {
            &attempts
        };
        let mut gpus = BTreeSet::new();
        for a in assigned {
            for gpu in rows(a, "gpus")? {
                gpus.insert(gpu.as_str().context("invalid assigned GPU")?);
            }
        }
        let actual = if assigned.is_empty() {
            "—".into()
        } else if gpus.is_empty() {
            "CPU".into()
        } else {
            label_gpus(&gpus, &v["gpu_indices"])
        };
        let (start, elapsed) = timing(&attempts, now)?;
        let done = stages.iter().filter(|s| s["state"] == "succeeded").count();
        let latest = match active.as_slice() {
            [one] => Some(*one),
            [] if stages.len() == 1 && !waiting => attempts.last().copied(),
            _ => None,
        };
        let progress = match latest {
            Some(a) if !a["progress"].is_null() || a["progress_error"].is_string() => {
                attempt_work(a)
            }
            _ if stages.len() > 1 || done > 0 => format!("{done}/{} 阶段", stages.len()),
            _ => "—".into(),
        };
        let mut group = if common_project {
            cell(&r["group_id"])
        } else {
            format!("{}/{}", cell(&r["project"]), cell(&r["group_id"]))
        };
        if let Some((title, _)) = r["batch_description"]
            .as_str()
            .and_then(|s| s.split_once([':', '：']))
            && !title.trim().is_empty()
        {
            group.push(' ');
            group.push_str(&cell(&Value::from(title.trim())));
        }
        let mut row = vec![
            cell(&r["local_id"]),
            group,
            state(&r["state"]),
            progress,
            start,
            elapsed,
            cell(&r["priority"]),
            allowed_gpus(&relevant, v)?,
            actual,
        ];
        if ids {
            row.push(cell(&r["run_id"]));
        }
        t.add_row(row);
    }
    let group = if common_project {
        format!("{} · ", cell(&runs[0]["project"]))
    } else {
        String::new()
    };
    Ok(format!("{group}{} 个实验\n{t}", runs.len()))
}

fn run_detail(r: &Value) -> Result<String> {
    let mut lines = vec![
        format!(
            "{}  {} / {}    {} · {}",
            cell(&r["local_id"]),
            cell(&r["project"]),
            cell(&r["name"]),
            state(&r["state"]),
            purpose(&r["purpose"])
        ),
        format!(
            "运行 ID  {}\n输出目录  {}",
            cell(&r["run_id"]),
            cell(&r["directory"])
        ),
    ];
    if let Some(failure) = r.get("failure").filter(|v| !v.is_null()) {
        lines.push(format!(
            "失败阶段  {}\n失败原因  {}",
            cell(&failure["stage"]),
            cell(&failure["reason"])
        ));
        if r["state"] == "failed" {
            lines.push(format!(
                "重试整个实验  ghs retry-run {}",
                cell(&r["local_id"])
            ));
        }
    }
    if let Some(wake) = r.get("wake") {
        if !wake["target"].is_null() {
            lines.push(format!(
                "唤醒目标  T3 {} / {}",
                cell(&wake["target"]["instance"]),
                cell(&wake["target"]["session_id"])
            ));
        }
        for event in rows(wake, "events")? {
            lines.push(format!(
                "唤醒事件  {}  {}  {}\n  投递记录  {}",
                cell(&event["event_id"]),
                cell(&event["event"]),
                cell(&event["state"]),
                cell(&event["directory"])
            ));
            if !event["error"].is_null() {
                lines.push(format!("  错误  {}", cell(&event["error"])));
            }
            if !event["receipt"].is_null() {
                lines.push(format!("  T3 回执  {}", cell(&event["receipt"])));
            }
        }
    }
    for s in rows(r, "stages")? {
        let res = &s["resources"];
        lines.push(format!(
            "\n{}  {}   {} CPU · {} GPU",
            cell(&s["name"]),
            state(&s["state"]),
            cell(&res["cpus"]),
            cell(&res["gpus"])
        ));
        let note = stage_note(s)?;
        if !note.is_empty() {
            lines.push(format!("  {note}"));
        }
        let attempts = rows(s, "attempts")?;
        if let Some(a) = attempts.last() {
            lines.push(format!(
                "  尝试 #{}  {}  {}",
                cell(&a["number"]),
                state(&a["state"]),
                cell(&a["attempt_id"])
            ));
            for field in ["progress_error", "index_error"] {
                if let Some(e) = a[field].as_str() {
                    lines.push(format!("  {e}"));
                }
            }
        }
        if matches!(s["state"].as_str(), Some("failed" | "cancelled"))
            && r["imported"] != true
            && r["failure"].is_null()
        {
            lines.push(format!("  重试  ghs retry {}", cell(&s["stage_id"])));
        }
    }
    Ok(lines.join("\n"))
}

fn comparison(v: &Value) -> Result<String> {
    let runs = rows(v, "runs")?;
    if runs.is_empty() {
        return Ok("没有符合条件的运行可供比较。".into());
    }
    let observations = rows(v, "observations")?;
    let mut lines = vec![format!(
        "{}比较 · {} 个运行",
        purpose(&v["purpose"]),
        runs.len()
    )];
    for run in runs {
        lines.push(format!("\n运行 {}", cell(&run["local_id"])));
        let mut t = table(&["阶段", "指标", "分组", "原值", "单位", "与对照差值", "说明"]);
        let mut count = 0;
        for o in observations.iter().filter(|o| o["run_id"] == run["run_id"]) {
            let note = if let Some(reason) = o["reason"].as_str() {
                reason.to_owned()
            } else {
                rows(o, "reasons")?
                    .iter()
                    .map(cell)
                    .collect::<Vec<_>>()
                    .join("；")
            };
            t.add_row([
                cell(&o["stage"]),
                cell(&o["metric"]),
                cell(&o["group"]),
                cell(&o["value"]),
                cell(&o["unit"]),
                cell(&o["delta"]),
                note,
            ]);
            count += 1;
        }
        if count > 0 {
            lines.push(t.to_string());
        } else {
            lines.push(
                rows(run, "reasons")?
                    .iter()
                    .map(cell)
                    .collect::<Vec<_>>()
                    .join("；"),
            );
        }
    }
    Ok(lines.join("\n"))
}

pub fn render(req: &Request, v: &Value, ids: bool) -> Result<String> {
    match req {
        Request::SubmitBatch { .. } => {
            let runs = rows(v, "runs")?;
            let mut t = table(&["ID", "运行", "用途"]);
            for r in runs {
                t.add_row([
                    cell(&r["local_id"]),
                    cell(&r["name"]),
                    purpose(&r["purpose"]),
                ]);
            }
            Ok(format!(
                "批次 {} · {} 个运行\n{t}",
                cell(&v["batch_id"]),
                runs.len()
            ))
        }
        Request::ListRuns { .. } => run_list(v, ids),
        Request::GetRun { .. } | Request::ImportRun { .. } => run_detail(v),
        Request::GetEvents { .. } => {
            let events = rows(v, "events")?;
            if events.is_empty() {
                return Ok("没有新的进度事件。".into());
            }
            let mut t = table(&["序号", "本地时间", "完成量 / 总量"]);
            for e in events {
                t.add_row([cell(&e["seq"]), clock(&e["time"])?, progress(e)]);
            }
            Ok(format!(
                "{t}\n下次读取可用 --after {}",
                cell(&v["next_cursor"])
            ))
        }
        Request::CompareRuns { .. } => comparison(v),
        Request::ListArtifacts { .. } => {
            let files = rows(v, "artifacts")?;
            let mut lines = Vec::new();
            if files.is_empty() {
                lines.push("没有已登记的文件。".into());
            } else {
                let mut t = table(&["类型", "文件路径"]);
                for file in files {
                    t.add_row([cell(&file["kind"]), cell(&file["path"])]);
                }
                lines.push(t.to_string());
            }
            for e in rows(v, "index_errors")? {
                lines.push(format!(
                    "索引错误 · {}  {}",
                    cell(&e["attempt_id"]),
                    cell(&e["error"])
                ));
            }
            Ok(lines.join("\n"))
        }
        Request::CancelAttempt { .. } => Ok(format!("已请求取消尝试 {}", cell(&v["attempt_id"]))),
        Request::RecoverAttempt { .. } => Ok(format!(
            "尝试 {} 已取消，资源预留已释放",
            cell(&v["attempt_id"])
        )),
        Request::RetryStage { .. } => Ok(format!("阶段 {} 已重新排队", cell(&v["stage_id"]))),
        Request::RetryRun { .. } => Ok(format!(
            "运行 {} 的 {} 个未成功阶段已重新排队",
            cell(&v["run_id"]),
            cell(&v["requeued_stages"])
        )),
        Request::RetryWake { .. } => Ok(format!("唤醒事件 {} 已重新排队", cell(&v["event_id"]))),
        Request::Reindex { .. } => Ok(format!(
            "已重建 {} 次执行尝试的索引。",
            rows(v, "indexed_attempt_ids")?.len()
        )),
        Request::GetGpuStatus { .. } | Request::ReadLog { .. } => {
            bail!("status and log must use their streaming output")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use unicode_width::UnicodeWidthStr;

    #[test]
    fn status_columns_align_for_wide_headers_and_report_gpu_work() {
        let idle = json!({"index":0,"name":"测试卡","state":"available","reservations":[],
            "used_bytes":536870912,"total_bytes":8589934592u64,"utilization_percent":0,"external_percent":0});
        let mut running = idle.clone();
        running["index"] = 10.into();
        running["state"] = "reserved".into();
        running["reservations"] = json!([{"attempt_id":"active"}]);
        running["utilization_percent"] = 100.into();
        let mut external = idle.clone();
        external["index"] = 11.into();
        external["state"] = "external_blocked".into();
        let report=status(&json!({"devices":[idle,running,external],"run_counts":{"running":1,"queued":3},
            "sampled_at":1000,"active_attempts":{"active":{"local_id":"@17","state":"running","gpus":["gpu"],"progress":{"completed":25,"total":100,"unit":"steps"},"eta_seconds":150}}})).unwrap();
        let lines: Vec<_> = report.lines().collect();
        assert!(lines[0].contains("运行中 1 个实验   排队中 3 个实验"));
        assert_eq!(lines.len(), 6);
        let column = |line: &str, term: &str| line[..line.find(term).unwrap()].width();
        assert_eq!(column(lines[2], "STATE"), column(lines[3], "IDLE"));
        assert_eq!(column(lines[2], "STATE"), column(lines[4], "RUNNING"));
        assert_eq!(column(lines[2], "STATE"), column(lines[5], "EXTERNAL"));
        assert_eq!(column(lines[2], "实验"), column(lines[4], "@17"));
        assert_eq!(
            column(lines[2], "利用率") + "利用率".width(),
            column(lines[4], "100%") + 4
        );
        assert!(lines[4].contains("0.50 / 8.0"));
        assert!(lines[4].contains("25/100 步"));
        assert!(lines[4].contains("2m 30s"));
        assert!(!report.contains("REASON") && !report.contains("reserved"));
    }

    #[test]
    fn list_counts_elapsed_intervals_once_and_intersects_allowed_cards() {
        let a = json!({"created_at":1000,"exit":{"finished_at":15000}});
        let b = json!({"created_at":10000,"exit":{"finished_at":21000}});
        let c = json!({"created_at":30000,"state":"running"});
        assert_eq!(timing(&[&a, &b, &c], 35000).unwrap().1, "25s");
        let stage = json!({"resources":{"gpus":1,"allowed_gpus":["gpu2","gpu3","gpu4"]}});
        let inventory = json!({"gpu_pool":["gpu4","gpu2"],"gpu_indices":{"gpu2":12,"gpu4":3}});
        assert_eq!(allowed_gpus(&[&stage], &inventory).unwrap(), "3,12");
    }
}
