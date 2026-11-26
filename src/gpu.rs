use crate::{
    model::{Attempt, Config},
    now,
    process::{self, Identity},
};
use anyhow::{Context, Result, ensure};
use nvml_wrapper::{Nvml, enums::device::UsedGpuMemory};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashSet},
    fs,
};

#[derive(Clone, Serialize)]
pub struct Owner {
    pub run_id: String,
    pub stage_id: String,
    pub attempt_id: String,
    pub purpose: String,
}
pub struct Reservation {
    pub owner: Owner,
    pub cpus: u32,
    pub gpus: Vec<String>,
    pub identity: Result<Identity>,
}
impl Reservation {
    pub fn new(a: Attempt, identity: Result<Identity>) -> Self {
        Self {
            owner: Owner {
                run_id: a.run_id,
                stage_id: a.stage_id,
                attempt_id: a.attempt_id,
                purpose: a.purpose.name().into(),
            },
            cpus: a.stage.resources.cpus,
            gpus: a.gpus,
            identity,
        }
    }
}
pub fn reserved_uuids(reservations: &[Reservation]) -> HashSet<&str> {
    reservations
        .iter()
        .flat_map(|r| r.gpus.iter().map(String::as_str))
        .collect()
}

pub fn gpu_limit_reason(max: Option<usize>, reserved: usize, requested: usize) -> Option<String> {
    let max = max?;
    (requested > max.saturating_sub(reserved)).then(|| {
        format!("needs {requested} GPU(s), {reserved} reserved, scheduler max_gpus = {max}")
    })
}
#[derive(Serialize)]
pub struct Process {
    pub pid: u32,
    pub name: Option<String>,
    pub memory_bytes: Option<u64>,
    pub ownership: &'static str,
    pub owner: Option<Owner>,
    pub reason: Option<String>,
}
#[derive(Serialize, Default)]
pub struct Device {
    pub index: u32,
    pub uuid: Option<String>,
    pub name: Option<String>,
    pub utilization_percent: Option<u32>,
    pub used_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub external_bytes: Option<u64>,
    pub external_percent: Option<f64>,
    pub reservations: Vec<Owner>,
    pub state: String,
    pub reasons: Vec<String>,
    pub errors: Vec<String>,
    pub processes: Option<Vec<Process>>,
}
#[derive(Serialize)]
pub struct Snapshot {
    pub host: String,
    pub sampled_at: u64,
    pub external_memory_block_percent: u32,
    pub max_gpus: Option<usize>,
    pub reserved_gpus: usize,
    pub poll_interval_seconds: u32,
    pub devices: Vec<Device>,
}
/// Device ordinals are host-local. Archived GPU UUIDs can be absent on this host.
pub fn indices() -> Result<BTreeMap<String, u32>> {
    let mut indices = BTreeMap::new();
    if std::path::Path::new("/sys/module/nvidia").try_exists()? {
        let nvml = Nvml::init().context("NVML initialization for GPU indices")?;
        for index in 0..nvml.device_count()? {
            indices.insert(nvml.device_by_index(index)?.uuid()?, index);
        }
    }
    Ok(indices)
}
fn owner(
    pid: u32,
    known: &[Reservation],
    boot: &str,
    before: u64,
) -> Result<(String, Option<Owner>)> {
    let first = process::stat(pid)?;
    ensure!(
        first.start < before && first.state != 'Z',
        "PID {pid} started during sampling or exited"
    );
    let resolve = |current: u32, session: u32| -> Result<Option<Owner>> {
        for r in known {
            if let Ok(i) = &r.identity
                && (i.pid == session || i.pid == current)
            {
                i.verify(&r.owner.attempt_id, boot)?;
                return Ok(Some(r.owner.clone()));
            }
        }
        Ok(None)
    };
    let mut current = pid;
    let mut parent = first.parent;
    let mut session = first.session;
    let found = loop {
        if let Some(o) = resolve(current, session)? {
            break Some(o);
        }
        if parent == 0 {
            break None;
        }
        current = parent;
        let ancestor = process::stat(current)?;
        parent = ancestor.parent;
        session = ancestor.session;
    };
    let last = process::stat(pid)?;
    ensure!(
        first.start == last.start && first.parent == last.parent && first.session == last.session,
        "PID {pid} changed during sampling"
    );
    if found.is_none() {
        for r in known {
            let i = r
                .identity
                .as_ref()
                .map_err(|e| anyhow::anyhow!("unverified runner {}: {e:#}", r.owner.attempt_id))?;
            // A fully traversed process ancestry did not match this runner.
            // Its death must not make unrelated GPU processes unclassifiable.
            // Keep malformed or foreign-boot identities explicit errors; matching
            // runners are still verified by resolve above.
            ensure!(
                i.version == 1 && i.attempt_id == r.owner.attempt_id && i.boot_id == boot,
                "runner record or boot ID mismatch"
            );
        }
    }
    Ok((first.name, found))
}
fn field<T, E: std::fmt::Display>(
    label: &str,
    r: std::result::Result<T, E>,
    errors: &mut Vec<String>,
) -> Option<T> {
    match r {
        Ok(v) => Some(v),
        Err(e) => {
            errors.push(format!("{label}: {e}"));
            None
        }
    }
}
impl Device {
    fn decide(&mut self, config: &Config, reservations: &[Reservation], processes: &[Process]) {
        if self.total_bytes == Some(0) {
            self.errors.push("total memory is zero".into());
        }
        let mut external = 0u128;
        let mut missing = self.total_bytes.is_none_or(|n| n == 0)
            || self.errors.iter().any(|s| {
                s.starts_with("compute:") || s.starts_with("graphics:") || s.starts_with("device:")
            });
        for p in processes {
            match (p.ownership, p.memory_bytes) {
                ("managed", _) => (),
                ("external", Some(n)) => external += u128::from(n),
                _ => {
                    missing = true;
                    self.reasons.push(format!(
                        "PID {}: {}",
                        p.pid,
                        p.reason.as_deref().unwrap_or("missing GPU memory")
                    ));
                }
            }
        }
        self.reasons.extend(self.errors.clone());
        let mut state = "available";
        if let Some(uuid) = &self.uuid {
            self.reservations = reservations
                .iter()
                .filter(|r| r.gpus.contains(uuid))
                .map(|r| r.owner.clone())
                .collect();
            if config.pool.as_ref().is_some_and(|p| !p.contains(uuid)) {
                state = "outside_pool";
                self.reasons.push("outside configured GPU pool".into());
            }
        }
        if !self.reservations.is_empty() {
            state = "reserved";
            self.reasons.push("reserved by active attempt".into());
        }
        if !missing {
            let total = self.total_bytes.unwrap();
            let pct = external as f64 * 100.0 / total as f64;
            self.external_percent = Some(pct);
            self.external_bytes = u64::try_from(external).ok();
            if external * 100 >= u128::from(total) * u128::from(config.threshold) {
                state = "external_blocked";
                self.reasons.push(format!(
                    "external memory {external}/{total} bytes reaches {}%",
                    config.threshold
                ));
            }
        }
        if missing || !self.errors.is_empty() || self.uuid.is_none() {
            state = "needs_review";
        }
        self.state = state.into();
    }
}
fn before_sample() -> Result<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    ensure!(hz > 0, "invalid clock tick rate");
    Ok((ts.tv_sec * hz + ts.tv_nsec * hz / 1_000_000_000) as u64)
}
pub fn snapshot(config: &Config, reservations: &[Reservation], details: bool) -> Result<Snapshot> {
    let nvml = Nvml::init().context("NVML initialization")?;
    let boot = process::boot_id()?;
    let mut devices = vec![];
    let reserved_gpus = reserved_uuids(reservations).len();
    let limit_reason = gpu_limit_reason(config.max_gpus, reserved_gpus, 1);
    for index in 0..nvml.device_count().context("NVML device count")? {
        let mut row = Device {
            index,
            ..Device::default()
        };
        let mut processes = vec![];
        if let Some(d) = field("device", nvml.device_by_index(index), &mut row.errors) {
            row.uuid = field("uuid", d.uuid(), &mut row.errors);
            row.name = field("name", d.name(), &mut row.errors);
            row.utilization_percent =
                field("utilization", d.utilization_rates(), &mut row.errors).map(|v| v.gpu);
            if let Some(m) = field("memory", d.memory_info(), &mut row.errors) {
                row.total_bytes = Some(m.total);
                row.used_bytes = Some(m.used);
            }
            let before = before_sample()?;
            let mut pids = BTreeMap::new();
            for (kind, list) in [
                ("compute", d.running_compute_processes()),
                ("graphics", d.running_graphics_processes()),
            ] {
                if let Some(list) = field(kind, list, &mut row.errors) {
                    for p in list {
                        let memory = match p.used_gpu_memory {
                            UsedGpuMemory::Used(v) => Some(v),
                            UsedGpuMemory::Unavailable => None,
                        };
                        // Compute and graphics APIs report total per-PID memory on this device.
                        pids.entry(p.pid)
                            .and_modify(|old: &mut Option<u64>| *old = (*old).max(memory))
                            .or_insert(memory);
                    }
                }
            }
            for (pid, memory_bytes) in pids {
                let (name, owner, ownership, reason) = match owner(pid, reservations, &boot, before)
                {
                    Ok((name, Some(o))) => (Some(name), Some(o), "managed", None),
                    Ok((name, None)) => (Some(name), None, "external", None),
                    Err(e) => (None, None, "unknown", Some(format!("{e:#}"))),
                };
                processes.push(Process {
                    pid,
                    name,
                    memory_bytes,
                    owner,
                    ownership,
                    reason,
                });
            }
        }
        row.decide(config, reservations, &processes);
        if row.state == "available"
            && let Some(reason) = &limit_reason
        {
            row.state = "quota_blocked".into();
            row.reasons.push(reason.clone());
        }
        if details {
            row.processes = Some(processes);
        }
        devices.push(row);
    }
    Ok(Snapshot {
        host: fs::read_to_string("/proc/sys/kernel/hostname")?
            .trim()
            .into(),
        sampled_at: now(),
        external_memory_block_percent: config.threshold,
        max_gpus: config.max_gpus,
        reserved_gpus,
        poll_interval_seconds: config.poll_interval_seconds,
        devices,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{model::Resources, scheduler::select_gpus};

    fn process(pid: u32, ownership: &'static str, memory_bytes: Option<u64>) -> Process {
        Process {
            pid,
            name: Some("test".into()),
            memory_bytes,
            ownership,
            owner: None,
            reason: None,
        }
    }
    fn card() -> Device {
        Device {
            uuid: Some("GPU-test".into()),
            total_bytes: Some(20 << 30),
            ..Default::default()
        }
    }
    fn config(threshold: u32) -> Config {
        Config {
            threshold,
            cpus: 1,
            pool: None,
            max_gpus: None,
            poll_interval_seconds: 5,
            t3code: Default::default(),
        }
    }
    #[test]
    fn dead_unrelated_runner_does_not_poison_process_ownership() {
        let boot = process::boot_id().unwrap();
        let pid = std::process::id();
        let mut reservation = Reservation {
            owner: Owner {
                run_id: "r".into(),
                stage_id: "s".into(),
                attempt_id: "a".into(),
                purpose: "experiment".into(),
            },
            cpus: 1,
            gpus: vec!["GPU-test".into()],
            identity: Ok(Identity {
                version: 1,
                attempt_id: "a".into(),
                boot_id: boot.clone(),
                pid: u32::MAX,
                start: 0,
            }),
        };
        assert!(
            reservation
                .identity
                .as_ref()
                .unwrap()
                .verify("a", &boot)
                .is_err()
        );
        assert!(
            owner(pid, std::slice::from_ref(&reservation), &boot, u64::MAX)
                .unwrap()
                .1
                .is_none()
        );

        // An unrelated dead runner still reserves its assigned GPU.
        let mut device = card();
        device.decide(&config(5), std::slice::from_ref(&reservation), &[]);
        assert_eq!(device.state, "reserved");

        // A matching but invalid runner must never be counted as managed.
        reservation.identity.as_mut().unwrap().pid = pid;
        assert!(owner(pid, std::slice::from_ref(&reservation), &boot, u64::MAX).is_err());

        reservation.identity.as_mut().unwrap().pid = u32::MAX;
        reservation.identity.as_mut().unwrap().boot_id = "another-boot".into();
        assert!(owner(pid, std::slice::from_ref(&reservation), &boot, u64::MAX).is_err());
        reservation.identity = Err(anyhow::anyhow!("missing identity"));
        assert!(owner(pid, &[reservation], &boot, u64::MAX).is_err());
    }
    #[test]
    fn one_byte_boundary_sum_and_scheduler_agree() {
        let limit = 1u64 << 30;
        for (n, state) in [
            (limit - 1, "available"),
            (limit, "external_blocked"),
            (limit + 1, "external_blocked"),
        ] {
            for (threshold, expected) in [(5, state), (6, "available")] {
                let mut d = card();
                d.decide(
                    &config(threshold),
                    &[],
                    &[
                        process(1, "external", Some(n / 2)),
                        process(2, "external", Some(n - n / 2)),
                        process(3, "managed", Some(10 << 30)),
                    ],
                );
                assert_eq!(d.external_bytes, Some(n));
                assert_eq!(d.state, expected);
                let snap = Snapshot {
                    host: "test".into(),
                    sampled_at: 0,
                    external_memory_block_percent: threshold,
                    max_gpus: None,
                    reserved_gpus: 0,
                    poll_interval_seconds: 5,
                    devices: vec![d],
                };
                let request = Resources {
                    gpus: 1,
                    allowed_gpus: Some(vec!["GPU-test".into()]),
                    ..Default::default()
                };
                assert_eq!(
                    select_gpus(&request, &snap, &HashSet::new()).len(),
                    usize::from(expected == "available")
                );
            }
        }
    }
    #[test]
    fn multi_gpu_selection_intersects_pool_allowlist_and_availability() {
        let config = Config {
            pool: Some(vec!["A".into(), "B".into(), "C".into(), "D".into()]),
            ..config(5)
        };
        let mut devices = vec![];
        for uuid in ["outside", "A", "B", "C", "D"] {
            let mut d = Device {
                uuid: Some(uuid.into()),
                ..card()
            };
            let bytes = if uuid == "B" { 2 << 30 } else { 0 };
            d.decide(&config, &[], &[process(1, "external", Some(bytes))]);
            devices.push(d);
        }
        let snap = Snapshot {
            host: "test".into(),
            sampled_at: 0,
            external_memory_block_percent: 5,
            max_gpus: None,
            reserved_gpus: 0,
            poll_interval_seconds: 5,
            devices,
        };
        let resources = Resources {
            gpus: 2,
            allowed_gpus: Some(vec!["outside".into(), "A".into(), "B".into(), "D".into()]),
            ..Default::default()
        };
        assert_eq!(select_gpus(&resources, &snap, &HashSet::new()), ["A", "D"]);
        assert_eq!(select_gpus(&resources, &snap, &HashSet::from(["A"])), ["D"]);
    }
    #[test]
    fn quota_requires_room_for_the_whole_request() {
        assert!(gpu_limit_reason(None, 4, 8).is_none());
        assert!(gpu_limit_reason(Some(4), 2, 2).is_none());
        assert!(gpu_limit_reason(Some(4), 3, 2).is_some());
        assert!(gpu_limit_reason(Some(4), 5, 1).is_some());
        assert!(gpu_limit_reason(Some(0), 0, 1).is_some());
        assert!(gpu_limit_reason(Some(0), 2, 0).is_none());
    }
    #[test]
    fn reservation_pool_and_uncertain_ownership() {
        let r = Reservation {
            owner: Owner {
                run_id: "r".into(),
                stage_id: "s".into(),
                attempt_id: "a".into(),
                purpose: "validation".into(),
            },
            cpus: 1,
            gpus: vec!["GPU-test".into()],
            identity: Err(anyhow::anyhow!("missing identity")),
        };
        let mut d = card();
        d.decide(&config(5), &[r], &[process(1, "external", Some(123))]);
        assert_eq!(d.state, "reserved");
        assert_eq!(d.external_bytes, Some(123));
        assert_eq!(d.reservations[0].run_id, "r");
        let mut d = card();
        d.decide(
            &Config {
                pool: Some(vec![]),
                ..config(5)
            },
            &[],
            &[],
        );
        assert_eq!(d.state, "outside_pool");
        for p in [process(9, "unknown", Some(1)), process(9, "external", None)] {
            let mut d = card();
            d.decide(&config(5), &[], &[p]);
            assert_eq!(d.state, "needs_review");
            assert_eq!(d.external_bytes, None);
            assert!(d.reasons[0].contains("PID 9"));
        }
    }
}
