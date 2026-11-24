use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Validation,
    Experiment,
}
impl Purpose {
    pub fn name(self) -> &'static str {
        match self {
            Self::Validation => "validation",
            Self::Experiment => "experiment",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub project: String,
    #[serde(default)]
    pub description: String,
    pub purpose: Option<Purpose>,
    pub baseline: Option<String>,
    pub runs: Vec<Run>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    pub name: String,
    pub purpose: Option<Purpose>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub parameters: BTreeMap<String, Value>,
    pub code_ref: Option<String>,
    pub stages: Vec<Stage>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stage {
    pub name: String,
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub resources: Resources,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub artifacts: Vec<PathBuf>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Resources {
    pub cpus: u32,
    pub gpus: usize,
    pub allowed_gpus: Option<Vec<String>>,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            cpus: 1,
            gpus: 0,
            allowed_gpus: None,
        }
    }
}
pub fn relative(path: &Path) -> Result<()> {
    ensure!(
        path.components().all(|c| matches!(c, Component::Normal(_)))
            && !path.as_os_str().is_empty(),
        "expected relative path without traversal: {}",
        path.display()
    );
    Ok(())
}
pub fn component(s: &str) -> Result<()> {
    ensure!(
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
            && s != "."
            && s != "..",
        "invalid identifier: {s}"
    );
    Ok(())
}
impl Manifest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1,
            "unsupported manifest version {}",
            self.version
        );
        ensure!(
            !self.project.trim().is_empty() && !self.runs.is_empty(),
            "project and runs are required"
        );
        let mut runs = BTreeSet::new();
        for r in &self.runs {
            ensure!(!r.name.trim().is_empty(), "empty run name");
            ensure!(runs.insert(&r.name), "duplicate run {}", r.name);
            r.purpose
                .or(self.purpose)
                .with_context(|| format!("missing purpose for {}", r.name))?;
            ensure!(!r.stages.is_empty(), "run {} has no stages", r.name);
            let mut names = BTreeSet::new();
            for s in &r.stages {
                ensure!(!s.name.trim().is_empty(), "empty stage name");
                ensure!(names.insert(&s.name), "duplicate stage {}", s.name);
                ensure!(
                    s.command.first().is_some_and(|v| !v.is_empty())
                        && !s.command.iter().any(|v| v.contains('\0')),
                    "invalid command for {}",
                    s.name
                );
                ensure!(
                    s.cwd.as_ref().is_none_or(|p| p.is_absolute()),
                    "cwd must be absolute"
                );
                for (k, v) in &s.env {
                    ensure!(
                        !k.is_empty()
                            && !k.contains(['=', '\0'])
                            && !v.contains('\0')
                            && !k.starts_with("GHS_")
                            && k != "CUDA_VISIBLE_DEVICES",
                        "invalid or reserved env key {k}"
                    );
                }
                ensure!(s.resources.cpus > 0, "cpus must be positive");
                if let Some(allowed) = &s.resources.allowed_gpus {
                    ensure!(
                        allowed.iter().collect::<BTreeSet<_>>().len() == allowed.len()
                            && allowed.len() >= s.resources.gpus,
                        "allowed_gpus must contain enough distinct UUIDs"
                    );
                }
                for p in &s.artifacts {
                    relative(p)?;
                }
            }
            for s in &r.stages {
                ensure!(
                    s.depends_on.iter().all(|d| names.contains(d)),
                    "unknown dependency for {}",
                    s.name
                );
                ensure!(
                    s.depends_on.iter().collect::<BTreeSet<_>>().len() == s.depends_on.len(),
                    "duplicate dependency for {}",
                    s.name
                );
            }
            let mut done = BTreeSet::new();
            while done.len() < names.len() {
                let before = done.len();
                for s in &r.stages {
                    if s.depends_on.iter().all(|d| done.contains(d)) {
                        done.insert(&s.name);
                    }
                }
                ensure!(done.len() > before, "dependency cycle in {}", r.name);
            }
        }
        ensure!(
            self.baseline.as_ref().is_none_or(|n| runs.contains(n)),
            "unknown baseline"
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub threshold: u32,
    pub cpus: u32,
    pub pool: Option<Vec<String>>,
    pub max_gpus: Option<usize>,
    pub poll_interval_seconds: u32,
    pub t3code: BTreeMap<String, T3Instance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct T3Instance {
    pub executable: PathBuf,
    pub base_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeRuntime {
    T3code,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WakeTarget {
    pub runtime: WakeRuntime,
    pub instance: String,
    pub session_id: String,
}
impl WakeTarget {
    pub fn resolve(&self, config: &Config) -> Result<T3Instance> {
        ensure!(
            !self.session_id.is_empty()
                && !self.session_id.starts_with('-')
                && !self.session_id.chars().any(char::is_whitespace)
                && !self.session_id.chars().any(char::is_control),
            "wake.session_id must be an explicit T3 thread ID"
        );
        let instance = config
            .t3code
            .get(&self.instance)
            .with_context(|| format!("unknown T3 instance {}", self.instance))?;
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::metadata(&instance.executable)
            .with_context(|| format!("T3 executable {}", instance.executable.display()))?;
        ensure!(
            meta.is_file() && meta.permissions().mode() & 0o111 != 0,
            "T3 executable is not executable"
        );
        ensure!(
            instance.base_dir.is_dir(),
            "T3 base_dir must be an existing directory"
        );
        Ok(instance.clone())
    }
}
impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut config = Self {
            threshold: 5,
            cpus: std::thread::available_parallelism()?.get().try_into()?,
            pool: None,
            max_gpus: None,
            poll_interval_seconds: 5,
            t3code: BTreeMap::new(),
        };
        if let Some(path) = path {
            let doc = fs::read_to_string(path)
                .with_context(|| path.display().to_string())?
                .parse::<toml_edit::DocumentMut>()?;
            for (key, _) in doc.iter() {
                ensure!(
                    matches!(key, "scheduler" | "t3code"),
                    "unknown config section {key}"
                );
            }
            if let Some(instances) = doc.get("t3code") {
                for (name, item) in instances
                    .as_table_like()
                    .context("expected [t3code.NAME]")?
                    .iter()
                {
                    component(name)?;
                    let instance = item.as_table_like().context("expected T3 instance table")?;
                    for (key, _) in instance.iter() {
                        ensure!(
                            matches!(key, "executable" | "base_dir"),
                            "unknown T3 instance setting {key}"
                        );
                    }
                    let path =
                        |key: &str| -> Result<PathBuf> {
                            let path =
                                PathBuf::from(
                                    instance.get(key).and_then(|v| v.as_str()).with_context(
                                        || format!("t3code.{name}.{key} must be a path string"),
                                    )?,
                                );
                            ensure!(path.is_absolute(), "t3code.{name}.{key} must be absolute");
                            Ok(path)
                        };
                    config.t3code.insert(
                        name.to_owned(),
                        T3Instance {
                            executable: path("executable")?,
                            base_dir: path("base_dir")?,
                        },
                    );
                }
            }
            let table = doc
                .get("scheduler")
                .and_then(|v| v.as_table_like())
                .context("expected [scheduler]")?;
            for (k, v) in table.iter() {
                match k {
                    "max_gpus" => {
                        config.max_gpus = Some(
                            v.as_integer()
                                .and_then(|n| n.try_into().ok())
                                .context("max_gpus must be a non-negative integer")?,
                        );
                    }
                    "poll_interval_seconds" => {
                        config.poll_interval_seconds = v
                            .as_integer()
                            .and_then(|n| u32::try_from(n).ok())
                            .filter(|n| *n > 0)
                            .context(
                                "poll_interval_seconds must be a positive integer in the u32 range",
                            )?;
                    }
                    "external_memory_block_percent" => {
                        config.threshold = v
                            .as_integer()
                            .context("threshold must be an integer")?
                            .try_into()?
                    }
                    "cpus" => {
                        config.cpus = v
                            .as_integer()
                            .context("cpus must be an integer")?
                            .try_into()?
                    }
                    "gpu_pool" => {
                        config.pool = Some(
                            v.as_array()
                                .context("gpu_pool must be an array")?
                                .iter()
                                .map(|item| {
                                    Ok(item.as_str().context("expected GPU UUID")?.to_owned())
                                })
                                .collect::<Result<_>>()?,
                        )
                    }
                    _ => bail!("unknown scheduler setting {k}"),
                }
            }
        }
        ensure!(
            config.threshold <= 100 && config.cpus > 0,
            "threshold must be 0..100 and cpus positive"
        );
        Ok(config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub version: u32,
    pub run_id: String,
    pub batch_id: String,
    pub project: String,
    pub purpose: Option<Purpose>,
    pub baseline_id: Option<String>,
    pub config: Run,
    pub stage_ids: BTreeMap<String, String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub version: u32,
    pub attempt_id: String,
    pub run_id: String,
    pub stage_id: String,
    pub purpose: Purpose,
    pub number: u32,
    pub stage: Stage,
    pub gpus: Vec<String>,
    pub created_at: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exit {
    pub version: u32,
    pub attempt_id: String,
    pub finished_at: u64,
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub cancelled: bool,
    pub missing_artifacts: Vec<PathBuf>,
    pub error: Option<String>,
}
impl Exit {
    pub fn state(&self) -> &'static str {
        if self.cancelled {
            "cancelled"
        } else if self.code == Some(0) && self.error.is_none() && self.missing_artifacts.is_empty()
        {
            "succeeded"
        } else {
            "failed"
        }
    }
}
