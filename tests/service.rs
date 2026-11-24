use ghs::{id, read_json, write_json};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Service {
    root: PathBuf,
    child: Option<Child>,
}
impl Service {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("ghs-test-{}", id().unwrap()));
        fs::create_dir(&root).unwrap();
        let mut s = Self { root, child: None };
        s.configure(1, None, 5);
        s.start();
        s
    }
    fn configure(&self, cpus: u32, gpu: Option<&str>, threshold: u32) {
        let pool = gpu
            .map(|v| format!("gpu_pool = [\"{v}\"]\n"))
            .unwrap_or_default();
        self.write_scheduler_config(&format!(
            "cpus={cpus}\nexternal_memory_block_percent={threshold}\n{pool}"
        ));
    }
    fn write_scheduler_config(&self, settings: &str) {
        let tmp = self.root.join("new.toml");
        fs::write(
            &tmp,
            format!("[scheduler]\npoll_interval_seconds=1\n{settings}"),
        )
        .unwrap();
        fs::rename(tmp, self.root.join("config.toml")).unwrap();
    }
    fn start(&mut self) {
        let log = File::options()
            .append(true)
            .create(true)
            .open(self.root.join("daemon.log"))
            .unwrap();
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_ghs"))
                .arg("daemon")
                .env_remove("GHS_SOCKET")
                .arg("--data-dir")
                .arg(&self.root)
                .arg("--config")
                .arg(self.root.join("config.toml"))
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                panic!(
                    "daemon {status}: {}",
                    fs::read_to_string(self.root.join("daemon.log")).unwrap()
                );
            }
            if self.exec(&["list"]).status.success() {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "daemon startup timeout"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }
    fn stop(&mut self) {
        if let Some(mut c) = self.child.take() {
            c.kill().unwrap();
            c.wait().unwrap();
        }
    }
    fn exec(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_ghs"))
            .arg("--data-dir")
            .arg(&self.root)
            .env_remove("GHS_SOCKET")
            .args(args)
            .output()
            .unwrap()
    }
    fn cli(&self, args: &[&str]) -> Value {
        let mut command = vec!["--json"];
        command.extend_from_slice(args);
        let o = self.exec(&command);
        assert!(
            o.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        serde_json::from_slice(&o.stdout).unwrap()
    }
    fn submit(&self, tag: &str, m: &Value) -> Value {
        let path = self.root.join(format!("{tag}.json"));
        write_json(&path, m).unwrap();
        self.cli(&["submit", path.to_str().unwrap(), "--request-id", tag])
    }
    fn wait(&self, id: &str, pred: impl Fn(&Value) -> bool) -> Value {
        let start = Instant::now();
        loop {
            let r = self.cli(&["get", id]);
            if pred(&r) {
                return r;
            }
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "run did not reach expected state: {r}"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }
    fn state(&self, id: &str, state: &str) -> Value {
        self.wait(id, |r| r["state"] == state)
    }
    fn mcp(&self, name: &str, args: Value) -> Value {
        let mut c = Command::new(env!("CARGO_BIN_EXE_ghs"))
            .arg("mcp")
            .arg("--data-dir")
            .arg(&self.root)
            .env_remove("GHS_SOCKET")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        {
            let mut input = c.stdin.take().unwrap();
            for packet in [
                json!({"id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","clientInfo":{"name":"test","version":"1"},"capabilities":{}}}),
                json!({"method":"notifications/initialized"}),
                json!({"id":2,"method":"tools/list"}),
                json!({"id":3,"method":"tools/call","params":{"name":name,"arguments":args}}),
            ] {
                let mut p = packet;
                p["jsonrpc"] = json!("2.0");
                writeln!(input, "{p}").unwrap();
            }
        }
        let o = c.wait_with_output().unwrap();
        assert!(o.status.success());
        let lines: Vec<Value> = std::str::from_utf8(&o.stdout)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines[0]["result"]["protocolVersion"], "2025-06-18");
        assert_eq!(lines[1]["result"]["tools"].as_array().unwrap().len(), 14);
        assert_eq!(lines[2]["result"]["isError"], false, "{}", lines[2]);
        lines[2]["result"]["structuredContent"].clone()
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.stop();
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.root).unwrap();
        } else {
            eprintln!("test directory {}", self.root.display());
        }
    }
}
fn stage(name: &str, script: &str) -> Value {
    json!({"name":name,"command":["/bin/sh","-ec",script]})
}
fn manifest(runs: Value) -> Value {
    json!({"version":1,"project":"tests","purpose":"experiment","runs":runs})
}
fn rid(receipt: &Value, i: usize) -> &str {
    receipt["runs"][i]["run_id"].as_str().unwrap()
}
fn attempt(r: &Value, stage: usize, n: usize) -> &str {
    r["stages"][stage]["attempts"][n]["attempt_id"]
        .as_str()
        .unwrap()
}
#[test]
fn recover_priority_cancel_and_retry() {
    let mut s = Service::new();
    let gate = s.root.join("gate");
    let mut held = stage(
        "hold",
        r#"echo started; if [ ! -f "$GATE" ]; then setsid sleep 30 & echo $! > "$GHS_OUTPUT_DIR/child"; wait; fi"#,
    );
    held["env"] = json!({"GATE":gate});
    held["artifacts"] = json!([]);
    let mut dependent = stage("after", "echo done");
    dependent["depends_on"] = json!(["hold"]);
    let m = manifest(json!([{"name":"held","purpose":"validation","stages":[held,dependent]}]));
    let receipt = s.submit("hold", &m);
    assert_eq!(s.submit("hold", &m), receipt);
    let conflict = s.root.join("different.json");
    write_json(&conflict, &manifest(json!([]))).unwrap();
    assert!(
        String::from_utf8_lossy(
            &s.exec(&["submit", conflict.to_str().unwrap(), "--request-id", "hold"])
                .stderr
        )
        .contains("conflict")
    );
    let run = rid(&receipt, 0);
    let running = s.wait(run, |r| {
        r["stages"][0]["state"] == "running"
            && Path::new(r["stages"][0]["attempts"][0]["directory"].as_str().unwrap())
                .join("artifacts/child")
                .exists()
    });
    let aid = attempt(&running, 0, 0);
    let child = fs::read_to_string(
        Path::new(
            running["stages"][0]["attempts"][0]["directory"]
                .as_str()
                .unwrap(),
        )
        .join("artifacts/child"),
    )
    .unwrap();
    let outside = Command::new("sleep").arg("30").spawn().unwrap();
    let mut outside = ChildGuard(outside);
    let order = s.root.join("order");
    let mut log = stage("log", r#"echo "$NAME" >> "$ORDER""#);
    log["env"] = json!({"NAME":"low","ORDER":order});
    let mut high = log.clone();
    high["env"]["NAME"] = json!("high");
    let queue=s.submit("queue",&manifest(json!([{"name":"low","priority":-1,"stages":[log]},{"name":"high","priority":4,"stages":[high]}])));
    s.stop();
    s.start();
    let recovered = s.mcp("get_run", json!({"run_id":run}));
    assert_eq!(attempt(&recovered, 0, 0), aid);
    assert_eq!(recovered["state"], "running");
    assert_eq!(
        s.mcp("read_log", json!({"attempt_id":aid}))["text"],
        "started\n"
    );
    assert!(
        s.cli(&["get", rid(&queue, 0)])["stages"][0]["attempts"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(s.cli(&["cancel", aid])["state"], "cancelling");
    let cancelled = s.state(run, "cancelled");
    assert!(cancelled["stages"][0]["attempts"][0]["exit"]["signal"].is_number());
    assert!(!Path::new(&format!("/proc/{}", child.trim())).exists());
    assert!(outside.0.try_wait().unwrap().is_none());
    s.state(rid(&queue, 0), "succeeded");
    assert_eq!(fs::read_to_string(order).unwrap(), "high\nlow\n");
    fs::write(gate, "open").unwrap();
    s.cli(&[
        "retry",
        receipt["runs"][0]["stages"]["hold"].as_str().unwrap(),
    ]);
    let done = s.state(run, "succeeded");
    assert_ne!(aid, attempt(&done, 0, 1));
    assert_eq!(done["stages"][0]["attempts"][0]["state"], "cancelled");
    assert_eq!(done["stages"][1]["state"], "succeeded");
    assert_eq!(s.cli(&["list"])["runs"].as_array().unwrap().len(), 3);
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            self.0.kill().unwrap();
            self.0.wait().unwrap();
        }
    }
}

#[test]
fn metrics_progress_history_and_relocation() {
    let s = Service::new();
    let script = r#"printf '{"version":1,"seq":1,"time":123,"attempt_id":"%s","stage_id":"%s","completed":2,"total":2,"unit":"items"}\n' "$GHS_ATTEMPT_ID" "$GHS_STAGE_ID" > "$GHS_EVENTS"
printf '{"version":1,"metric":"time","value":%s,"unit":"%s","protocol":"v1","group":"test","source":"artifacts/raw"}\n' "$VAL" "$UNIT" > "$GHS_RESULTS"
echo measured > "$GHS_OUTPUT_DIR/raw"; echo hello"#;
    let mut runlist = vec![];
    for (name, value, unit, purpose) in [
        ("base", 8, "ms", "experiment"),
        ("variant", 5, "ms", "experiment"),
        ("other", 4, "s", "experiment"),
        ("check", 2, "s", "validation"),
    ] {
        let mut st = stage("measure", script);
        st["env"] = json!({"VAL":value.to_string(),"UNIT":unit});
        st["artifacts"] = json!(["raw"]);
        runlist.push(json!({"name":name,"purpose":purpose,"stages":[st]}));
    }
    let mut missing = stage("measure", "exit 0");
    missing["artifacts"] = json!(["missing"]);
    runlist.push(json!({"name":"broken","stages":[missing]}));
    let mut m = manifest(json!(runlist));
    m["baseline"] = json!("base");
    let receipt = s.submit("metrics", &m);
    for i in 0..4 {
        s.state(rid(&receipt, i), "succeeded");
    }
    let failed = s.state(rid(&receipt, 4), "failed");
    assert_eq!(
        failed["stages"][0]["attempts"][0]["exit"]["missing_artifacts"],
        json!(["missing"])
    );
    let report = s.mcp(
        "compare_runs",
        json!({"batch_id":receipt["batch_id"],"metrics":["time"],"group":"test"}),
    );
    assert_eq!(report["runs"].as_array().unwrap().len(), 4);
    let rows = report["observations"].as_array().unwrap();
    let row = |i: usize| {
        rows.iter()
            .find(|r| r["run_id"] == rid(&receipt, i))
            .unwrap()
    };
    assert_eq!(row(1)["delta"], -3.0);
    assert_eq!(row(1)["baseline_source"]["line"], 1);
    assert!(row(2)["delta"].is_null());
    assert!(
        row(2)["reasons"][0]
            .as_str()
            .unwrap()
            .contains("unit mismatch")
    );
    assert!(row(4)["value"].is_null());
    assert!(row(4)["reason"].as_str().unwrap().contains("failed"));
    let run = s.cli(&["get", rid(&receipt, 1)]);
    let aid = attempt(&run, 0, 0);
    assert_eq!(
        s.mcp("get_events", json!({"attempt_id":aid}))["next_cursor"],
        1
    );
    assert_eq!(s.cli(&["events", aid, "--after", "1"])["events"], json!([]));
    assert_eq!(
        s.exec(&["log", aid, "--offset", "1", "--length", "3"])
            .stdout,
        b"ell"
    );
    // Import a real output directory into a second isolated daemon, then relocate it.
    let hist = Service::new();
    let path = run["directory"].as_str().unwrap();
    let local = hist.root.join("original");
    assert!(
        Command::new("cp")
            .arg("-r")
            .arg(path)
            .arg(&local)
            .status()
            .unwrap()
            .success()
    );
    let meta = local.join("run.json");
    let mut metadata: Value = read_json(&meta).unwrap();
    metadata["purpose"] = Value::Null;
    write_json(&meta, &metadata).unwrap();
    let import = |dir: &Path| {
        hist.cli(&[
            "import",
            dir.to_str().unwrap(),
            "--purpose",
            "experiment",
            "--purpose-source",
            "archive register",
        ])
    };
    assert_eq!(import(&local)["purpose_source"], "archive register");
    let moved = hist.root.join("moved");
    fs::rename(&local, &moved).unwrap();
    import(&moved);
    let historical = hist.cli(&["compare", rid(&receipt, 1)]);
    assert_eq!(historical["observations"][0]["value"], 5.0);
    assert!(
        historical["observations"][0]["reasons"][0]
            .as_str()
            .unwrap()
            .contains("not registered")
    );
    let reindexed = hist.cli(&["reindex"]);
    assert_eq!(reindexed["indexed_attempt_ids"], json!([aid]));
    let files = hist.cli(&["artifacts", rid(&receipt, 1)])["artifacts"]
        .as_array()
        .unwrap()
        .clone();
    assert!(files.iter().any(|f| f["relative_path"] == "artifacts/raw"
        && Path::new(f["path"].as_str().unwrap()).starts_with(&moved)));
    assert!(
        !hist
            .exec(&["retry", run["stages"][0]["stage_id"].as_str().unwrap()])
            .status
            .success()
    );
}

#[test]
fn rejects_invalid_graph_and_missing_purpose_before_enqueue() {
    let s = Service::new();
    let mut m = manifest(json!([{"name":"r","stages":[stage("a","true")]}]));
    for (kind, dep, purpose) in [
        ("unknown", Some("b"), true),
        ("cycle", Some("a"), true),
        ("purpose", None, false),
    ] {
        m["runs"][0]["stages"][0]["depends_on"] = json!(dep.into_iter().collect::<Vec<_>>());
        if !purpose {
            m["purpose"] = Value::Null;
        }
        let path = s.root.join("bad.json");
        write_json(&path, &m).unwrap();
        let o = s.exec(&["submit", path.to_str().unwrap(), "--request-id", kind]);
        assert!(!o.status.success());
        assert!(String::from_utf8_lossy(&o.stderr).contains(kind));
    }
    assert_eq!(s.cli(&["list"])["runs"], json!([]));
}

// This opt-in test creates two real CUDA contexts only on GHS_TEST_GPU_UUID.
// The external process increases its allocation on a file signal and frees it on exit.
const CUDA_WORKER: &str = r#"
import ctypes as c, os, sys, time
from pathlib import Path
driver = c.CDLL('libcuda.so.1')
def check(code):
    if code != 0:
        raise RuntimeError(f'CUDA driver error {code}')
check(driver.cuInit(0))
device = c.c_int()
check(driver.cuDeviceGet(c.byref(device), 0))
ctx = c.c_void_p()
check(driver.cuCtxCreate_v2(c.byref(ctx), 0, device))
first = c.c_uint64()
check(driver.cuMemAlloc_v2(c.byref(first), c.c_size_t(1024 * 1024)))
directory = Path(sys.argv[1])
(directory / 'ready').write_text(str(os.getpid()))
amount = int(sys.argv[2])
end = time.monotonic() + 60
while time.monotonic() < end and not (directory / 'stop').exists():
    if amount and (directory / 'grow').exists():
        extra = c.c_uint64()
        check(driver.cuMemAlloc_v2(c.byref(extra), c.c_size_t(amount)))
        amount = 0
    time.sleep(.05)
check(driver.cuCtxDestroy_v2(ctx))
"#;
fn wait_gpu(s: &Service, uuid: &str, predicate: impl Fn(&Value) -> bool) -> Value {
    let start = Instant::now();
    loop {
        let snap = s.cli(&["status", "--processes", "--uuid", uuid]);
        let d = &snap["devices"][0];
        if predicate(d) {
            return d.clone();
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "GPU condition timeout: {d}"
        );
        thread::sleep(Duration::from_millis(60));
    }
}
#[test]
#[ignore = "requires an explicitly selected idle NVIDIA GPU, host /proc and Python 3"]
fn live_gpu_ownership_threshold_and_recovery() {
    let uuid = std::env::var("GHS_TEST_GPU_UUID").expect("explicit GHS_TEST_GPU_UUID required");
    let mut s = Service::new();
    s.configure(4, Some(&uuid), 5);
    let whole = s.cli(&["status"]);
    let other = s.mcp("get_gpu_status", json!({}));
    assert_eq!(whole["host"], other["host"]);
    assert_eq!(
        whole["devices"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| &d["uuid"])
            .collect::<Vec<_>>(),
        other["devices"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| &d["uuid"])
            .collect::<Vec<_>>()
    );
    for d in whole["devices"].as_array().unwrap() {
        if d["uuid"] != uuid {
            assert!(
                d["reasons"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("outside configured GPU pool"))
            );
        }
    }
    let initial = s.cli(&["status", "--uuid", &uuid]);
    let card = &initial["devices"][0];
    assert_eq!(
        card["state"], "available",
        "choose an idle test device: {card}"
    );
    assert!(
        card["used_bytes"].as_u64().unwrap() < card["total_bytes"].as_u64().unwrap() / 20,
        "test GPU must be idle"
    );
    let script = s.root.join("cuda.py");
    fs::write(&script, CUDA_WORKER).unwrap();
    let ext_dir = s.root.join("external");
    fs::create_dir(&ext_dir).unwrap();
    let mut external = ChildGuard(
        Command::new("python3")
            .arg(&script)
            .arg(&ext_dir)
            .arg((card["total_bytes"].as_u64().unwrap() * 6 / 100).to_string())
            .env("CUDA_VISIBLE_DEVICES", &uuid)
            .spawn()
            .unwrap(),
    );
    let external_pid = external.0.id();
    wait_gpu(&s, &uuid, |d| {
        d["state"] == "available"
            && d["processes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p["pid"] == external_pid && p["ownership"] == "external")
    });
    s.configure(4, Some(&uuid), 0);
    let resources = json!({"gpus":1,"allowed_gpus":[uuid]});
    let mut first = stage("hold", r#"python3 "$WORKER" "$GHS_OUTPUT_DIR" 0 & wait"#);
    first["env"] = json!({"WORKER":script});
    first["resources"] = resources.clone();
    let primary = s.submit(
        "gpu",
        &manifest(json!([{"name":"managed","purpose":"validation","stages":[first]}])),
    );
    let run = rid(&primary, 0);
    s.wait(run, |r| {
        r["stages"][0]["reason"]
            .as_str()
            .is_some_and(|v| v.contains("reaches 0%"))
    });
    s.configure(4, Some(&uuid), 5);
    let running = s.state(run, "running");
    let aid = attempt(&running, 0, 0);
    let both =
        |d: &Value| {
            d["state"] == "reserved"
                && d["processes"].as_array().unwrap().iter().any(|p| {
                    p["owner"]["attempt_id"] == aid && p["owner"]["purpose"] == "validation"
                })
        };
    wait_gpu(&s, &uuid, both);
    s.stop();
    s.start();
    wait_gpu(&s, &uuid, both);
    let mut next = stage("next", r#"echo "$CUDA_VISIBLE_DEVICES""#);
    next["resources"] = resources;
    let queued = s.submit(
        "next",
        &manifest(json!([{"name":"queued","stages":[next]}])),
    );
    s.wait(rid(&queued, 0), |r| {
        r["stages"][0]["reason"]
            .as_str()
            .is_some_and(|s| s.contains("reserved"))
    });
    fs::write(ext_dir.join("grow"), "").unwrap();
    wait_gpu(&s, &uuid, |d| {
        d["state"] == "external_blocked" && d["external_percent"].as_f64().unwrap() >= 5.
    });
    assert_eq!(s.cli(&["get", run])["state"], "running");
    s.cli(&["cancel", aid]);
    s.state(run, "cancelled");
    wait_gpu(&s, &uuid, |d| {
        d["state"] == "external_blocked"
            && d["reservations"] == json!([])
            && d["processes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p["pid"] == external_pid)
    });
    assert!(external.0.try_wait().unwrap().is_none());
    assert_eq!(
        s.cli(&["get", rid(&queued, 0)])["stages"][0]["attempts"],
        json!([])
    );
    fs::write(ext_dir.join("stop"), "").unwrap();
    assert!(external.0.wait().unwrap().success());
    let completed = s.state(rid(&queued, 0), "succeeded");
    assert_eq!(
        s.mcp("read_log", json!({"attempt_id":attempt(&completed,0,0)}))["text"],
        format!("{uuid}\n")
    );
}

#[test]
fn unverifiable_identity_holds_cpu_until_reconciled() {
    let mut s = Service::new();
    let receipt=s.submit("identity",&manifest(json!([{"name":"first","stages":[stage("hold","sleep 30")]},{"name":"second","stages":[stage("after","true")]}])));
    let run = rid(&receipt, 0);
    let live = s.state(run, "running");
    let aid = attempt(&live, 0, 0);
    let dir = Path::new(
        live["stages"][0]["attempts"][0]["directory"]
            .as_str()
            .unwrap(),
    );
    let path = dir.join("identity.json");
    let identity: Value = read_json(&path).unwrap();
    s.stop();
    let mut damaged = identity.clone();
    damaged["boot_id"] = json!("other boot");
    write_json(&path, &damaged).unwrap();
    s.start();
    let review = s.state(run, "needs_review");
    assert!(
        review["stages"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("boot ID")
    );
    let other = s.wait(rid(&receipt, 1), |r| {
        r["stages"][0]["reason"]
            .as_str()
            .is_some_and(|s| s.contains("1/1 reserved"))
    });
    assert_eq!(other["stages"][0]["attempts"], json!([]));
    assert!(!s.exec(&["cancel", aid]).status.success());
    write_json(&path, &identity).unwrap();
    s.state(run, "running");
    s.cli(&["cancel", aid]);
    s.state(run, "cancelled");
    s.state(rid(&receipt, 1), "succeeded");
}

#[test]
fn invalid_project_data_has_explicit_errors_and_can_be_reindexed() {
    let s = Service::new();
    let r=s.submit("data",&manifest(json!([{"name":"指标","stages":[stage("计算",r#"echo broken > "$GHS_EVENTS"; echo broken > "$GHS_RESULTS""#)]}])));
    let run = rid(&r, 0);
    let done = s.state(run, "succeeded");
    let a = &done["stages"][0]["attempts"][0];
    assert!(a["index_error"].as_str().unwrap().contains("line 1"));
    assert!(a["progress_error"].as_str().unwrap().contains("line 1"));
    assert!(!s.exec(&["events", attempt(&done, 0, 0)]).status.success());
    assert!(!s.exec(&["reindex", "--run-id", run]).status.success());
    let dir = Path::new(a["directory"].as_str().unwrap());
    fs::write(
        dir.join("results.jsonl"),
        r#"{"version":1,"metric":"count","source":"output.log","value":0}"#,
    )
    .unwrap();
    s.cli(&["reindex", "--run-id", run]);
    assert_eq!(s.cli(&["compare", run])["observations"][0]["value"], 0.0);
}
#[test]
fn quickstart_manifest_executes_and_produces_a_real_digest() {
    let s = Service::new();
    let m: Value = serde_json::from_str(include_str!("../experiments/sha256.json")).unwrap();
    let r = s.submit("sha", &m);
    let run = rid(&r, 0);
    let state = s.state(run, "succeeded");
    let dir = Path::new(
        state["stages"][0]["attempts"][0]["directory"]
            .as_str()
            .unwrap(),
    );
    assert_eq!(
        fs::read_to_string(dir.join("artifacts/sha256.txt"))
            .unwrap()
            .trim()
            .len(),
        64
    );
    let report = s.mcp(
        "compare_runs",
        json!({"run_ids":[run],"purpose":"validation"}),
    );
    assert!(report["observations"][0]["value"].as_f64().unwrap() > 0.);
}

#[test]
fn gpu_quota_queues_oversized_requests_and_allows_cpu_work() {
    let defaults = ghs::model::Config::load(None).unwrap();
    assert_eq!(defaults.max_gpus, None);
    assert_eq!(defaults.poll_interval_seconds, 5);
    let s = Service::new();
    s.write_scheduler_config("max_gpus=0");
    let mut gpu = stage("gpu", "true");
    gpu["resources"] = json!({"gpus": 5});
    let receipt = s.submit(
        "quota",
        &manifest(json!([
            {"name":"gpu","stages":[gpu]},
            {"name":"cpu","stages":[stage("cpu","true")]}
        ])),
    );
    let check = |limit| {
        let reason = format!("needs 5 GPU(s), 0 reserved, scheduler max_gpus = {limit}");
        let r = s.wait(rid(&receipt, 0), |r| r["stages"][0]["reason"] == reason);
        assert_eq!(r["state"], "queued");
        assert_eq!(r["stages"][0]["attempts"], json!([]));
    };
    check(0);
    s.state(rid(&receipt, 1), "succeeded");
    s.write_scheduler_config("max_gpus=4");
    check(4);
}

#[test]
#[ignore = "requires two explicit GPU UUIDs for reservation tests; workloads execute only shell commands"]
fn live_gpu_quota_across_batches_and_daemon_restart() {
    let ids = std::env::var("GHS_TEST_GPU_UUIDS")
        .expect("set GHS_TEST_GPU_UUIDS to two comma-separated UUIDs");
    let uuids: Vec<_> = ids.split(',').collect();
    assert_eq!(uuids.len(), 2);
    assert_ne!(uuids[0], uuids[1]);
    let mut s = Service::new();
    // This isolated daemon only runs sleep and true. No CUDA context is created.
    let settings = format!(
        "cpus=4\nexternal_memory_block_percent=100\ngpu_pool={}",
        json!(uuids)
    );
    let set_limit = |service: &Service, limit| {
        service.write_scheduler_config(&format!("{settings}\nmax_gpus={limit}"))
    };
    set_limit(&s, 1);
    let submit = |service: &Service, name: &str, script: &str, allowed: &[&str]| {
        let mut job = stage("work", script);
        job["resources"] = json!({"gpus":allowed.len(),"allowed_gpus":allowed});
        service.submit(name, &manifest(json!([{"name":name,"stages":[job]}])))
    };
    let holding = submit(&s, "holding", "sleep 60", &uuids[..1]);
    let active = s.state(rid(&holding, 0), "running");
    let aid = attempt(&active, 0, 0);
    let queued = submit(&s, "waiting", "true", &uuids[1..]);
    let oversized = submit(&s, "both", "true", &uuids);
    let wait_quota = |service: &Service, receipt: &Value, req, used, max| {
        let reason = format!("needs {req} GPU(s), {used} reserved, scheduler max_gpus = {max}");
        let r = service.wait(rid(receipt, 0), |r| r["stages"][0]["reason"] == reason);
        assert_eq!(r["stages"][0]["attempts"], json!([]));
    };
    wait_quota(&s, &queued, 1, 1, 1);
    let snap = s.mcp("get_gpu_status", json!({"uuid":uuids[1]}));
    assert_eq!(snap["max_gpus"], 1);
    assert_eq!(snap["reserved_gpus"], 1);
    assert_eq!(snap["devices"][0]["state"], "quota_blocked");
    s.stop();
    set_limit(&s, 0);
    s.start();
    wait_quota(&s, &queued, 1, 1, 0);
    assert_eq!(s.cli(&["get", rid(&holding, 0)])["state"], "running");
    s.cli(&["cancel", aid]);
    wait_quota(&s, &queued, 1, 0, 0);
    set_limit(&s, 1);
    s.state(rid(&queued, 0), "succeeded");
    wait_quota(&s, &oversized, 2, 0, 1);
    set_limit(&s, 2);
    let both = s.state(rid(&oversized, 0), "succeeded");
    assert_eq!(
        both["stages"][0]["attempts"][0]["gpus"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn queue_display_and_numbering_survive_an_upgrade() {
    let mut s = Service::new();
    s.write_scheduler_config("max_gpus=0");
    let mut gpu = stage("train", "true");
    gpu["resources"] = json!({"gpus":1});
    let mut after = stage("evaluate", "true");
    after["depends_on"] = json!(["train"]);
    let mut m = manifest(json!([
        {"name":"detailed-experiment-name","stages":[gpu,after], "priority": 17},
        {"name":"another-experiment","stages":[stage("calculate","true")]}
    ]));
    m["description"] = json!("城市：评估不同参数");
    let receipt = s.submit("batch", &m);
    s.state("@2", "succeeded");
    let contents = |o: Output| {
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        String::from_utf8(o.stdout).unwrap()
    };
    let list = contents(s.exec(&["list"]));
    assert_eq!(list.lines().count(), 4, "{list}");
    for col in [
        "ID",
        "组别",
        "状态",
        "进度",
        "开始时间",
        "已用时",
        "优先级",
        "允许 GPU",
        "实际 GPU",
    ] {
        assert!(list.lines().nth(1).unwrap().contains(col));
    }
    let first: Vec<_> = list.lines().nth(2).unwrap().split_whitespace().collect();
    assert_eq!(
        first,
        [
            "@1",
            "B1",
            "城市",
            "排队中",
            "0/2",
            "阶段",
            "—",
            "—",
            "17",
            "池内任意",
            "—"
        ]
    );
    assert!(!list.contains("detailed-experiment-name"));
    assert!(!list.contains(rid(&receipt, 0)));
    assert!(contents(s.exec(&["list", "--ids"])).contains(rid(&receipt, 0)));
    assert!(contents(s.exec(&["get", "@1"])).contains("detailed-experiment-name"));
    assert_eq!(
        s.cli(&["artifacts", "@2"])["artifacts"],
        s.cli(&["artifacts", rid(&receipt, 1)])["artifacts"]
    );
    assert_eq!(
        s.cli(&["compare", "@1", "--baseline-id", "@2"])["runs"][0]["run_id"],
        rid(&receipt, 0)
    );
    s.cli(&["reindex", "--run-id", "@2"]);
    s.stop();
    // Recreate the on-disk schema from before local IDs, including the original receipt.
    let db = rusqlite::Connection::open(s.root.join("scheduler.sqlite3")).unwrap();
    db.execute_batch("DROP TRIGGER number_new_run; DROP TABLE run_numbers;")
        .unwrap();
    drop(db);
    {
        let store = ghs::store::Store::open(s.root.clone()).unwrap();
        assert_eq!(store.run_counts().unwrap(), json!({"running":0,"queued":1}));
        store
            .db
            .execute(
                "UPDATE stages SET state='running' WHERE run=?",
                [rid(&receipt, 0)],
            )
            .unwrap();
        assert_eq!(store.run_counts().unwrap(), json!({"running":1,"queued":0}));
        store
            .db
            .execute(
                "UPDATE stages SET state='queued' WHERE run=?",
                [rid(&receipt, 0)],
            )
            .unwrap();
    }
    s.start();
    assert_eq!(s.submit("batch", &m), receipt);
    assert_eq!(s.cli(&["get", "@1"])["run_id"], rid(&receipt, 0));
    assert_eq!(s.cli(&["get", "@2"])["group_id"], "B1");
    let next = s.submit(
        "next",
        &manifest(json!([{"name":"next","stages":[stage("cpu","true")]}])),
    );
    assert_eq!(next["runs"][0]["local_id"], "@3");
    s.state("@3", "succeeded");
}

#[test]
fn failure_stops_parallel_and_waiting_stages_without_stopping_other_runs() {
    let s = Service::new();
    s.configure(3, None, 5);
    let release = s.root.join("release-failure");
    let retry = s.root.join("retry");
    let mut fail = stage(
        "fail",
        r#"if [ ! -f "$RETRY" ]; then while [ ! -f "$RELEASE" ]; do sleep .02; done; exit 7; fi"#,
    );
    fail["env"] = json!({"RELEASE":release,"RETRY":retry});
    fail["depends_on"] = json!(["prepare"]);
    let mut hold = stage(
        "parallel",
        r#"if [ ! -f "$RETRY" ]; then setsid sleep 60 & echo $! > "$GHS_OUTPUT_DIR/child"; wait; fi"#,
    );
    hold["env"] = json!({"RETRY":retry});
    hold["depends_on"] = json!(["prepare"]);
    let mut dependent = stage("dependent", "echo after");
    dependent["depends_on"] = json!(["fail"]);
    let mut queued = stage("waiting", "echo waiting");
    queued["resources"] = json!({"cpus":3});
    queued["depends_on"] = json!(["prepare"]);
    let m = manifest(json!([
        {"name":"fail-fast","stages":[stage("prepare","echo prepared"),fail,hold,dependent,queued]},
        {"name":"unrelated","stages":[stage("independent","echo independent")]}
    ]));
    let receipt = s.submit("fail-fast", &m);
    let run = rid(&receipt, 0);
    let running = s.wait(run, |r| {
        r["stages"][1]["state"] == "running"
            && r["stages"][2]["state"] == "running"
            && Path::new(r["stages"][2]["attempts"][0]["directory"].as_str().unwrap())
                .join("artifacts/child")
                .exists()
    });
    let child = fs::read_to_string(
        Path::new(
            running["stages"][2]["attempts"][0]["directory"]
                .as_str()
                .unwrap(),
        )
        .join("artifacts/child"),
    )
    .unwrap();
    fs::write(&release, "go").unwrap();
    let failed = s.state(run, "failed");
    assert_eq!(failed["failure"]["stage"], "fail");
    assert!(failed["failure"]["reason"].as_str().unwrap().contains("7"));
    assert_eq!(failed["stages"][2]["state"], "cancelled");
    assert!(!Path::new(&format!("/proc/{}", child.trim())).exists());
    for i in [3, 4] {
        assert_eq!(failed["stages"][i]["state"], "skipped");
        assert_eq!(failed["stages"][i]["attempts"], json!([]));
    }
    s.state(rid(&receipt, 1), "succeeded");
    let stage_retry = s.exec(&["retry", failed["stages"][1]["stage_id"].as_str().unwrap()]);
    assert!(!stage_retry.status.success());
    assert!(String::from_utf8_lossy(&stage_retry.stderr).contains("retry-run"));
    fs::write(retry, "go").unwrap();
    let restarted = s.mcp("retry_run", json!({"run_id":run}));
    assert_eq!(restarted["requeued_stages"], 4);
    let succeeded = s.state(run, "succeeded");
    assert!(succeeded["failure"].is_null());
    assert_eq!(
        succeeded["stages"][0]["attempts"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        succeeded["stages"][1]["attempts"].as_array().unwrap().len(),
        2
    );
    assert_eq!(
        succeeded["stages"][2]["attempts"].as_array().unwrap().len(),
        2
    );
}

fn configure_wake(s: &Service, executable: &Path, cpus: u32) {
    s.write_scheduler_config(&format!(
        "cpus={cpus}\n[t3code.test]\nexecutable={}\nbase_dir={}\n",
        serde_json::to_string(executable).unwrap(),
        serde_json::to_string(&s.root).unwrap()
    ));
}
fn wake_target() -> Value {
    json!({"runtime":"t3code","instance":"test","session_id":"00000000-0000-4000-8000-000000000001"})
}

#[test]
fn failure_wakes_while_cleanup_needs_review_and_survives_restart() {
    let mut s = Service::new();
    // A real command with exit zero but no T3 receipt must NOT count as successful delivery.
    configure_wake(&s, Path::new("/bin/true"), 2);
    let release = s.root.join("release");
    let retry = s.root.join("retry");
    let mut fail = stage(
        "failure",
        r#"if [ ! -f "$RETRY" ]; then while [ ! -f "$RELEASE" ]; do sleep .02; done; exit 9; fi"#,
    );
    fail["env"] = json!({"RELEASE":release,"RETRY":retry});
    let mut held = stage("held", r#"if [ ! -f "$RETRY" ]; then sleep 60; fi"#);
    held["env"] = json!({"RETRY":retry});
    let args = json!({"request_id":"wake-failure","manifest":manifest(json!([{"name":"failure","stages":[fail,held]}])),"wake":wake_target()});
    let receipt = s.mcp("submit_batch", args.clone());
    assert_eq!(s.mcp("submit_batch", args), receipt);
    let run = rid(&receipt, 0);
    let running = s.wait(run, |r| {
        r["stages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["state"] == "running")
    });
    let identity_path = Path::new(
        running["stages"][1]["attempts"][0]["directory"]
            .as_str()
            .unwrap(),
    )
    .join("identity.json");
    let identity: Value = read_json(&identity_path).unwrap();
    let mut invalid = identity.clone();
    invalid["start"] = json!(0);
    write_json(&identity_path, &invalid).unwrap();
    fs::write(&release, "go").unwrap();
    let failed = s.wait(run, |r| {
        r["state"] == "failing"
            && r["stages"][1]["state"] == "needs_review"
            && r["wake"]["events"][0]["state"] == "unknown"
    });
    assert_eq!(failed["wake"]["events"].as_array().unwrap().len(), 1);
    let event = failed["wake"]["events"][0]["event_id"].as_str().unwrap();
    let event_dir = Path::new(failed["wake"]["events"][0]["directory"].as_str().unwrap());
    let text = fs::read_to_string(event_dir.join("message.txt")).unwrap();
    assert!(text.contains("\"cleanup_pending\":true"));
    assert!(text.contains("run.failed"));
    assert!(
        failed["wake"]["events"][0]["error"]
            .as_str()
            .unwrap()
            .contains("JSON receipt")
    );
    assert!(!s.exec(&["retry-run", run]).status.success());
    assert!(!s.exec(&["retry-wake", event]).status.success());
    s.stop();
    s.start();
    let restored = s.cli(&["get", run]);
    assert_eq!(restored["state"], "failing");
    assert_eq!(restored["wake"]["events"], failed["wake"]["events"]);
    let display = String::from_utf8(s.exec(&["get", run]).stdout).unwrap();
    assert!(
        display.contains("失败清理中")
            && display.contains("唤醒事件")
            && display.contains("JSON receipt")
    );
    write_json(&identity_path, &identity).unwrap();
    s.state(run, "failed");
    fs::write(retry, "go").unwrap();
    s.cli(&["retry-run", run]);
    let done = s.wait(run, |r| {
        r["state"] == "succeeded" && r["wake"]["events"][1]["state"] == "unknown"
    });
    assert_eq!(done["wake"]["events"].as_array().unwrap().len(), 2);
    assert_eq!(done["wake"]["events"][1]["event"], "run.succeeded");
    assert_eq!(done["wake"]["events"][1]["generation"], 2);
    s.cli(&["retry-wake", event, "--allow-duplicate"]);
    let resent = s.wait(run, |r| {
        r["wake"]["events"][0]["delivery"] == 2 && r["wake"]["events"][0]["state"] == "unknown"
    });
    assert_eq!(resent["wake"]["events"].as_array().unwrap().len(), 2);
    assert!(event_dir.join("result.json").exists());
}

#[test]
fn wake_target_validation_and_idempotency_cover_cli_and_mcp() {
    let s = Service::new();
    configure_wake(&s, Path::new("/bin/false"), 1);
    let m = manifest(json!([{"name":"quick","stages":[stage("stage","true")]}]));
    let path = s.root.join("manifest.json");
    write_json(&path, &m).unwrap();
    let thread = wake_target()["session_id"].as_str().unwrap().to_owned();
    assert!(
        !s.exec(&[
            "submit",
            path.to_str().unwrap(),
            "--request-id",
            "bad",
            "--wake-session",
            &thread
        ])
        .status
        .success()
    );
    assert!(
        !s.exec(&[
            "submit",
            path.to_str().unwrap(),
            "--request-id",
            "bad",
            "--wake-session",
            &thread,
            "--wake-instance",
            "absent"
        ])
        .status
        .success()
    );
    assert_eq!(s.cli(&["list"])["runs"], json!([]));
    let receipt = s.cli(&[
        "submit",
        path.to_str().unwrap(),
        "--request-id",
        "good",
        "--wake-session",
        &thread,
        "--wake-instance",
        "test",
    ]);
    assert_eq!(
        s.mcp(
            "submit_batch",
            json!({"manifest":m,"request_id":"good","wake":wake_target()})
        ),
        receipt
    );
    let changed = s.exec(&["submit", path.to_str().unwrap(), "--request-id", "good"]);
    assert!(!changed.status.success());
    assert!(String::from_utf8_lossy(&changed.stderr).contains("different wake target"));
    let done = s.wait(rid(&receipt, 0), |r| {
        r["wake"]["events"][0]["state"] == "unknown"
    });
    assert_eq!(done["state"], "succeeded");
    assert!(
        done["wake"]["events"][0]["error"]
            .as_str()
            .unwrap()
            .contains("exit status: 1")
    );
}

#[test]
fn offline_completion_persists_wake_and_failed_delivery_can_be_retried() {
    use std::os::unix::fs::symlink;
    let mut s = Service::new();
    let executable = s.root.join("t3-command");
    symlink("/bin/false", &executable).unwrap();
    configure_wake(&s, &executable, 1);
    let gate = s.root.join("gate");
    let mut held = stage("held", r#"while [ ! -f "$GATE" ]; do sleep .02; done"#);
    held["env"] = json!({"GATE":gate});
    let receipt = s.mcp("submit_batch",json!({"request_id":"offline","manifest":manifest(json!([{"name":"offline","stages":[held]}])),"wake":wake_target()}));
    let run = rid(&receipt, 0);
    let running = s.state(run, "running");
    let dir = Path::new(
        running["stages"][0]["attempts"][0]["directory"]
            .as_str()
            .unwrap(),
    );
    s.stop();
    fs::remove_file(&executable).unwrap();
    fs::write(gate, "go").unwrap();
    let start = Instant::now();
    while !dir.join("exit.json").exists() {
        assert!(start.elapsed() < Duration::from_secs(5));
        thread::sleep(Duration::from_millis(20));
    }
    // Reconcile directly while the daemon is stopped: outcome and pending notification are committed together.
    {
        let mut store = ghs::store::Store::open(s.root.clone()).unwrap();
        store.reconcile().unwrap();
        assert_eq!(
            store.get_run(run).unwrap()["wake"]["events"][0]["state"],
            "pending"
        );
    }
    s.start();
    let done = s.wait(run, |r| r["wake"]["events"][0]["state"] == "failed");
    assert_eq!(done["state"], "succeeded");
    let event = done["wake"]["events"][0]["event_id"].as_str().unwrap();
    s.stop();
    s.start();
    assert_eq!(
        s.cli(&["get", run])["wake"]["events"],
        done["wake"]["events"]
    );
    symlink("/bin/false", &executable).unwrap();
    s.mcp("retry_wake", json!({"event_id":event}));
    let resent = s.wait(run, |r| {
        r["wake"]["events"][0]["delivery"] == 2 && r["wake"]["events"][0]["state"] == "unknown"
    });
    assert_eq!(resent["wake"]["events"].as_array().unwrap().len(), 1);
}

#[test]
#[ignore = "sends a real wake to GHS_TEST_T3_THREAD_ID using GHS_TEST_T3_EXECUTABLE and GHS_TEST_T3_BASE_DIR"]
fn live_t3_wake_returns_a_matching_receipt() {
    let s = Service::new();
    let executable = PathBuf::from(std::env::var("GHS_TEST_T3_EXECUTABLE").unwrap());
    let base_dir = PathBuf::from(std::env::var("GHS_TEST_T3_BASE_DIR").unwrap());
    let thread_id = std::env::var("GHS_TEST_T3_THREAD_ID").unwrap();
    s.write_scheduler_config(&format!(
        "cpus=1\n[t3code.live]\nexecutable={}\nbase_dir={}\n",
        serde_json::to_string(&executable).unwrap(),
        serde_json::to_string(&base_dir).unwrap()
    ));
    let receipt = s.mcp("submit_batch",json!({"request_id":"live-t3-wake",
        "manifest":manifest(json!([{"name":"GHS live wake acceptance test","stages":[stage("complete","true")]}])),
        "wake":{"runtime":"t3code","instance":"live","session_id":thread_id}}));
    let result = s.wait(rid(&receipt, 0), |r| {
        matches!(
            r["wake"]["events"][0]["state"].as_str(),
            Some("accepted" | "failed" | "unknown")
        )
    });
    assert_eq!(result["wake"]["events"][0]["state"], "accepted", "{result}");
    assert_eq!(
        result["wake"]["events"][0]["receipt"]["threadId"],
        thread_id
    );
}

#[test]
fn interrupted_delivery_is_observable_and_never_replayed_automatically() {
    use std::os::unix::process::CommandExt;
    let mut s = Service::new();
    configure_wake(&s, Path::new("/bin/false"), 1);
    let gate = s.root.join("gate");
    let mut held = stage("held", r#"while [ ! -f "$GATE" ]; do sleep .02; done"#);
    held["env"] = json!({"GATE":gate});
    held["depends_on"] = json!(["first"]);
    let receipt = s.mcp("submit_batch",json!({"request_id":"interrupted","manifest":manifest(json!([{"name":"interrupted","stages":[stage("first","true"),held]}])),"wake":wake_target()}));
    let run = rid(&receipt, 0);
    let running = s.wait(run, |r| {
        r["stages"][0]["state"] == "succeeded" && r["stages"][1]["state"] == "running"
    });
    assert_eq!(running["wake"]["events"], json!([]));
    s.stop();
    fs::write(gate, "go").unwrap();
    let exit_path = Path::new(
        running["stages"][1]["attempts"][0]["directory"]
            .as_str()
            .unwrap(),
    )
    .join("exit.json");
    let start = Instant::now();
    while !exit_path.exists() {
        assert!(start.elapsed() < Duration::from_secs(5));
        thread::sleep(Duration::from_millis(20));
    }
    // Inject the crash boundary after an outbox claim using a real, separately owned process.
    // It never fabricates a T3 reply or successful delivery.
    let mut command = Command::new("sleep");
    command.arg("60");
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let mut worker = ChildGuard(command.spawn().unwrap());
    let event;
    {
        let mut store = ghs::store::Store::open(s.root.clone()).unwrap();
        store.reconcile().unwrap();
        let snapshot = store.get_run(run).unwrap();
        event = snapshot["wake"]["events"][0]["event_id"]
            .as_str()
            .unwrap()
            .to_owned();
        store
            .db
            .execute(
                "UPDATE wake_outbox SET state='sending',delivery=1 WHERE id=?",
                [&event],
            )
            .unwrap();
        let dir = s.root.join("wakeups").join(&event).join("1");
        fs::create_dir_all(&dir).unwrap();
        write_json(
            &dir.join("identity.json"),
            &ghs::process::Identity {
                version: 1,
                attempt_id: format!("{event}:1"),
                boot_id: ghs::process::boot_id().unwrap(),
                pid: worker.0.id(),
                start: ghs::process::stat(worker.0.id()).unwrap().start,
            },
        )
        .unwrap();
    }
    s.start();
    let other = s.submit(
        "while-sending",
        &manifest(json!([{"name":"other","stages":[stage("work","true")]}])),
    );
    s.state(rid(&other, 0), "succeeded");
    assert_eq!(
        s.cli(&["get", run])["wake"]["events"][0]["state"],
        "sending"
    );
    worker.0.kill().unwrap();
    worker.0.wait().unwrap();
    let unknown = s.wait(run, |r| r["wake"]["events"][0]["state"] == "unknown");
    assert!(
        unknown["wake"]["events"][0]["error"]
            .as_str()
            .unwrap()
            .contains("interrupted")
    );
    s.stop();
    s.start();
    assert_eq!(
        s.cli(&["get", run])["wake"]["events"],
        unknown["wake"]["events"]
    );
    assert!(!s.root.join("wakeups").join(event).join("2").exists());
}

#[test]
fn upgrade_adopts_existing_failures_before_scheduling_independent_stages() {
    let mut s = Service::new();
    let mut queued = stage("independent", "echo must-not-run");
    queued["resources"] = json!({"cpus":2});
    let receipt = s.submit(
        "legacy",
        &manifest(json!([{"name":"legacy","stages":[stage("fail","exit 4"),queued]}])),
    );
    let run = rid(&receipt, 0);
    s.state(run, "failed");
    s.cli(&["retry-run", run]);
    let failed = s.state(run, "failed");
    s.stop();
    let db = rusqlite::Connection::open(s.root.join("scheduler.sqlite3")).unwrap();
    db.execute_batch("DROP TABLE run_control").unwrap();
    db.execute(
        "UPDATE stages SET state='queued',reason=NULL WHERE id=?",
        [failed["stages"][1]["stage_id"].as_str().unwrap()],
    )
    .unwrap();
    drop(db);
    s.configure(2, None, 5);
    s.start();
    let restored = s.state(run, "failed");
    assert_eq!(restored["failure"]["stage"], "fail");
    assert_eq!(
        restored["failure"]["attempt_id"],
        failed["stages"][0]["attempts"][1]["attempt_id"]
    );
    assert_eq!(restored["stages"][1]["state"], "skipped");
    assert_eq!(restored["stages"][1]["attempts"], json!([]));
}
