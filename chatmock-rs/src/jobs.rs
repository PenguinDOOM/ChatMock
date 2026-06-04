use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use rand::{distr::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::{Notify, RwLock},
    time::Instant,
};

pub const MAX_TOOL_WAIT_MS: u64 = 10_000;
const DEFAULT_MAX_JOBS: usize = 64;
const DEFAULT_COMPLETED_TTL: Duration = Duration::from_secs(30 * 60);
const DEFAULT_MAX_RUNNING: Duration = Duration::from_secs(30 * 60);
const DEFAULT_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const JOB_INSTRUCTIONS: &str = "For long-running ChatMock-owned operations, do not block on completion. Use chatmock_start_job to start the work, report the job_id, and use chatmock_poll_job, chatmock_read_job_output, or chatmock_get_job_result in later turns when needed. Do not loop on chatmock_poll_job indefinitely.";

#[derive(Debug, Clone)]
pub struct JobManagerConfig {
    pub max_jobs: usize,
    pub completed_ttl: Duration,
    pub max_running: Duration,
    pub max_output_bytes: usize,
}

impl Default for JobManagerConfig {
    fn default() -> Self {
        Self {
            max_jobs: DEFAULT_MAX_JOBS,
            completed_ttl: DEFAULT_COMPLETED_TTL,
            max_running: DEFAULT_MAX_RUNNING,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }
}

#[derive(Debug)]
pub struct JobManager {
    config: JobManagerConfig,
    jobs: RwLock<HashMap<String, JobEntry>>,
}

#[derive(Debug)]
struct JobEntry {
    id: String,
    status: JobStatus,
    updated_at: Instant,
    output: VecDeque<JobOutputChunk>,
    next_offset: u64,
    retained_output_bytes: usize,
    result: Option<JobResult>,
    error: Option<JobError>,
    cancel_requested: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Shell,
    TestRun,
    RepoScan,
    CodexSubagent,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Starting,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    fn terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
    System,
}

#[derive(Debug, Clone)]
struct JobOutputChunk {
    offset: u64,
    stream: OutputStream,
    text: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobResult {
    pub summary: String,
    pub exit_code: Option<i32>,
    pub files_changed: Vec<String>,
    pub patch_path: Option<PathBuf>,
    pub artifact_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobError {
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StartJobArgs {
    pub kind: JobKind,
    pub command: Option<Vec<String>>,
    pub prompt: Option<String>,
    pub cwd: Option<String>,
    pub max_initial_wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PollJobArgs {
    pub job_id: String,
    pub since_offset: Option<u64>,
    pub max_wait_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadJobOutputArgs {
    pub job_id: String,
    pub since_offset: Option<u64>,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobIdArgs {
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobStartResult {
    pub job_id: String,
    pub status: JobStatus,
    pub next_offset: u64,
    pub output: String,
    pub result: Option<JobResult>,
    pub error: Option<JobError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobPollResult {
    pub job_id: String,
    pub status: JobStatus,
    pub next_offset: u64,
    pub output: String,
    pub result: Option<JobResult>,
    pub error: Option<JobError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobOutputResult {
    pub job_id: String,
    pub status: JobStatus,
    pub next_offset: u64,
    pub output: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobResultResult {
    pub job_id: String,
    pub status: JobStatus,
    pub next_offset: u64,
    pub output: String,
    pub result: Option<JobResult>,
    pub error: Option<JobError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CancelResult {
    pub job_id: String,
    pub status: JobStatus,
    pub cancelled: bool,
    pub error: Option<JobError>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum JobManagerError {
    #[error("ChatMock job capacity is exhausted.")]
    Capacity,
    #[error("Job not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    InvalidRequest(String),
}

impl JobManager {
    pub fn new(config: JobManagerConfig) -> Self {
        Self {
            config,
            jobs: RwLock::new(HashMap::new()),
        }
    }

    pub async fn start_job(
        self: &Arc<Self>,
        args: StartJobArgs,
    ) -> Result<JobStartResult, JobManagerError> {
        self.gc().await;
        let command = resolve_command(&args)?;
        let cwd = resolve_cwd(args.cwd.as_deref())?;
        let id = random_job_id();
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());

        {
            let mut jobs = self.jobs.write().await;
            if jobs.len() >= self.config.max_jobs {
                return Err(JobManagerError::Capacity);
            }
            jobs.insert(
                id.clone(),
                JobEntry {
                    id: id.clone(),
                    status: JobStatus::Starting,
                    updated_at: Instant::now(),
                    output: VecDeque::new(),
                    next_offset: 0,
                    retained_output_bytes: 0,
                    result: None,
                    error: None,
                    cancel_requested: Arc::clone(&cancel_requested),
                    notify: Arc::clone(&notify),
                },
            );
        }

        let manager = Arc::clone(self);
        let job_id = id.clone();
        tokio::spawn(async move {
            manager
                .run_process_job(job_id, args.kind, command, cwd, cancel_requested)
                .await;
        });

        let initial_wait = args.max_initial_wait_ms.unwrap_or(0).min(MAX_TOOL_WAIT_MS);
        if initial_wait > 0 {
            tokio::select! {
                _ = notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(initial_wait)) => {}
            }
        }
        let snapshot = self.snapshot(&id, 0, None).await?;
        Ok(JobStartResult {
            job_id: snapshot.job_id,
            status: snapshot.status,
            next_offset: snapshot.next_offset,
            output: snapshot.output,
            result: snapshot.result,
            error: snapshot.error,
        })
    }

    pub async fn poll_job(&self, args: PollJobArgs) -> Result<JobPollResult, JobManagerError> {
        let wait_ms = args.max_wait_ms.unwrap_or(0).min(MAX_TOOL_WAIT_MS);
        if wait_ms > 0 {
            if let Some(notify) = self.notify_for_job(&args.job_id).await {
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {}
                }
            }
        }
        let snapshot = self
            .snapshot(&args.job_id, args.since_offset.unwrap_or(0), None)
            .await?;
        Ok(JobPollResult {
            job_id: snapshot.job_id,
            status: snapshot.status,
            next_offset: snapshot.next_offset,
            output: snapshot.output,
            result: snapshot.result,
            error: snapshot.error,
        })
    }

    pub async fn read_job_output(
        &self,
        args: ReadJobOutputArgs,
    ) -> Result<JobOutputResult, JobManagerError> {
        let snapshot = self
            .snapshot(
                &args.job_id,
                args.since_offset.unwrap_or(0),
                args.max_bytes.or(Some(DEFAULT_MAX_OUTPUT_BYTES)),
            )
            .await?;
        Ok(JobOutputResult {
            job_id: snapshot.job_id,
            status: snapshot.status,
            next_offset: snapshot.next_offset,
            output: snapshot.output,
        })
    }

    pub async fn get_job_result(
        &self,
        args: JobIdArgs,
    ) -> Result<JobResultResult, JobManagerError> {
        let snapshot = self.snapshot(&args.job_id, 0, None).await?;
        Ok(JobResultResult {
            job_id: snapshot.job_id,
            status: snapshot.status,
            next_offset: snapshot.next_offset,
            output: snapshot.output,
            result: snapshot.result,
            error: snapshot.error,
        })
    }

    pub async fn cancel_job(&self, args: JobIdArgs) -> Result<CancelResult, JobManagerError> {
        let mut jobs = self.jobs.write().await;
        let job = jobs
            .get_mut(&args.job_id)
            .ok_or_else(|| JobManagerError::NotFound(args.job_id.clone()))?;
        if job.status.terminal() {
            return Ok(CancelResult {
                job_id: job.id.clone(),
                status: job.status.clone(),
                cancelled: false,
                error: job.error.clone(),
            });
        }
        job.cancel_requested.store(true, Ordering::SeqCst);
        job.updated_at = Instant::now();
        job.notify.notify_waiters();
        Ok(CancelResult {
            job_id: job.id.clone(),
            status: job.status.clone(),
            cancelled: true,
            error: None,
        })
    }

    pub async fn execute_tool_call(self: &Arc<Self>, name: &str, arguments: &str) -> Value {
        match name {
            "chatmock_start_job" => match serde_json::from_str::<StartJobArgs>(arguments) {
                Ok(args) => serialize_result(self.start_job(args).await),
                Err(error) => error_value(error.to_string()),
            },
            "chatmock_poll_job" => match serde_json::from_str::<PollJobArgs>(arguments) {
                Ok(args) => serialize_result(self.poll_job(args).await),
                Err(error) => error_value(error.to_string()),
            },
            "chatmock_read_job_output" => {
                match serde_json::from_str::<ReadJobOutputArgs>(arguments) {
                    Ok(args) => serialize_result(self.read_job_output(args).await),
                    Err(error) => error_value(error.to_string()),
                }
            }
            "chatmock_get_job_result" => match serde_json::from_str::<JobIdArgs>(arguments) {
                Ok(args) => serialize_result(self.get_job_result(args).await),
                Err(error) => error_value(error.to_string()),
            },
            "chatmock_cancel_job" => match serde_json::from_str::<JobIdArgs>(arguments) {
                Ok(args) => serialize_result(self.cancel_job(args).await),
                Err(error) => error_value(error.to_string()),
            },
            _ => error_value(format!("Unknown ChatMock job tool: {name}")),
        }
    }

    async fn run_process_job(
        self: Arc<Self>,
        job_id: String,
        kind: JobKind,
        command: Vec<String>,
        cwd: PathBuf,
        cancel_requested: Arc<AtomicBool>,
    ) {
        self.mark_running(&job_id).await;
        self.append_output(
            &job_id,
            OutputStream::System,
            format!("starting {:?}: {}\n", kind, command.join(" ")),
        )
        .await;

        let mut child = match Command::new(&command[0])
            .args(&command[1..])
            .current_dir(cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                self.mark_failed(&job_id, error.to_string(), None).await;
                return;
            }
        };

        if let Some(stdout) = child.stdout.take() {
            let manager = Arc::clone(&self);
            let output_job_id = job_id.clone();
            tokio::spawn(async move {
                manager
                    .read_stream(output_job_id, OutputStream::Stdout, stdout)
                    .await;
            });
        }
        if let Some(stderr) = child.stderr.take() {
            let manager = Arc::clone(&self);
            let output_job_id = job_id.clone();
            tokio::spawn(async move {
                manager
                    .read_stream(output_job_id, OutputStream::Stderr, stderr)
                    .await;
            });
        }

        let deadline = Instant::now() + self.config.max_running;
        loop {
            if cancel_requested.load(Ordering::SeqCst) {
                let _ = child.kill().await;
                self.mark_cancelled(&job_id).await;
                return;
            }
            if Instant::now() >= deadline {
                let _ = child.kill().await;
                self.mark_failed(&job_id, "Job exceeded maximum runtime.".to_string(), None)
                    .await;
                return;
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    let exit_code = status.code();
                    if status.success() {
                        self.mark_completed(&job_id, exit_code).await;
                    } else {
                        self.mark_failed(
                            &job_id,
                            format!("Process exited with status {status}."),
                            exit_code,
                        )
                        .await;
                    }
                    return;
                }
                Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(error) => {
                    self.mark_failed(&job_id, error.to_string(), None).await;
                    return;
                }
            }
        }
    }

    async fn read_stream<R>(&self, job_id: String, stream: OutputStream, mut reader: R)
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let mut buffer = [0_u8; 8192];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break,
                Ok(count) => {
                    let text = String::from_utf8_lossy(&buffer[..count]).into_owned();
                    self.append_output(&job_id, stream.clone(), text).await;
                }
                Err(error) => {
                    self.append_output(&job_id, OutputStream::System, format!("{error}\n"))
                        .await;
                    break;
                }
            }
        }
    }

    async fn mark_running(&self, job_id: &str) {
        self.update_job(job_id, |job| job.status = JobStatus::Running)
            .await;
    }

    async fn mark_completed(&self, job_id: &str, exit_code: Option<i32>) {
        self.update_job(job_id, |job| {
            job.status = JobStatus::Completed;
            job.result = Some(JobResult {
                summary: "Job completed successfully.".to_string(),
                exit_code,
                files_changed: Vec::new(),
                patch_path: None,
                artifact_paths: Vec::new(),
            });
        })
        .await;
    }

    async fn mark_cancelled(&self, job_id: &str) {
        self.update_job(job_id, |job| {
            job.status = JobStatus::Cancelled;
            job.result = Some(JobResult {
                summary: "Job was cancelled.".to_string(),
                exit_code: None,
                files_changed: Vec::new(),
                patch_path: None,
                artifact_paths: Vec::new(),
            });
        })
        .await;
    }

    async fn mark_failed(&self, job_id: &str, message: String, exit_code: Option<i32>) {
        self.update_job(job_id, |job| {
            job.status = JobStatus::Failed;
            job.error = Some(JobError {
                message: message.clone(),
            });
            job.result = Some(JobResult {
                summary: message,
                exit_code,
                files_changed: Vec::new(),
                patch_path: None,
                artifact_paths: Vec::new(),
            });
        })
        .await;
    }

    async fn append_output(&self, job_id: &str, stream: OutputStream, text: String) {
        self.update_job(job_id, |job| {
            let offset = job.next_offset;
            let byte_len = text.len();
            job.next_offset = job.next_offset.saturating_add(byte_len as u64);
            job.retained_output_bytes = job.retained_output_bytes.saturating_add(byte_len);
            job.output.push_back(JobOutputChunk {
                offset,
                stream,
                text,
            });
            while job.retained_output_bytes > self.config.max_output_bytes {
                let Some(chunk) = job.output.pop_front() else {
                    break;
                };
                job.retained_output_bytes =
                    job.retained_output_bytes.saturating_sub(chunk.text.len());
            }
        })
        .await;
    }

    async fn update_job(&self, job_id: &str, update: impl FnOnce(&mut JobEntry)) {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(job_id) {
            update(job);
            job.updated_at = Instant::now();
            job.notify.notify_waiters();
        }
    }

    async fn notify_for_job(&self, job_id: &str) -> Option<Arc<Notify>> {
        self.jobs
            .read()
            .await
            .get(job_id)
            .map(|job| Arc::clone(&job.notify))
    }

    async fn snapshot(
        &self,
        job_id: &str,
        since_offset: u64,
        max_bytes: Option<usize>,
    ) -> Result<JobSnapshot, JobManagerError> {
        let jobs = self.jobs.read().await;
        let job = jobs
            .get(job_id)
            .ok_or_else(|| JobManagerError::NotFound(job_id.to_string()))?;
        Ok(JobSnapshot {
            job_id: job.id.clone(),
            status: job.status.clone(),
            next_offset: job.next_offset,
            output: collect_output(job, since_offset, max_bytes),
            result: job.result.clone(),
            error: job.error.clone(),
        })
    }

    pub async fn gc(&self) {
        let now = Instant::now();
        let mut jobs = self.jobs.write().await;
        jobs.retain(|_, job| {
            !job.status.terminal() || now.duration_since(job.updated_at) < self.config.completed_ttl
        });
    }
}

#[derive(Debug)]
struct JobSnapshot {
    job_id: String,
    status: JobStatus,
    next_offset: u64,
    output: String,
    result: Option<JobResult>,
    error: Option<JobError>,
}

fn collect_output(job: &JobEntry, since_offset: u64, max_bytes: Option<usize>) -> String {
    let mut output = String::new();
    let mut remaining = max_bytes.unwrap_or(usize::MAX);
    for chunk in &job.output {
        let chunk_end = chunk.offset.saturating_add(chunk.text.len() as u64);
        if chunk_end <= since_offset {
            continue;
        }
        let start = since_offset.saturating_sub(chunk.offset) as usize;
        let bytes = chunk.text.as_bytes();
        let available = &bytes[start.min(bytes.len())..];
        if remaining == 0 {
            break;
        }
        let take = remaining.min(available.len());
        if chunk.stream == OutputStream::Stderr {
            output.push_str("[stderr] ");
        }
        output.push_str(&String::from_utf8_lossy(&available[..take]));
        remaining -= take;
    }
    output
}

fn resolve_command(args: &StartJobArgs) -> Result<Vec<String>, JobManagerError> {
    let command = match args.kind {
        JobKind::RepoScan => args
            .command
            .clone()
            .unwrap_or_else(|| vec!["git".to_string(), "status".to_string(), "--short".to_string()]),
        JobKind::Shell | JobKind::TestRun | JobKind::CodexSubagent => args.command.clone().ok_or_else(|| {
            JobManagerError::InvalidRequest("command is required for this job kind".to_string())
        })?,
    };
    if command.is_empty() || command[0].trim().is_empty() {
        return Err(JobManagerError::InvalidRequest(
            "command must contain an executable".to_string(),
        ));
    }
    Ok(command)
}

fn resolve_cwd(cwd: Option<&str>) -> Result<PathBuf, JobManagerError> {
    let cwd = match cwd {
        Some(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => std::env::current_dir().map_err(|error| {
            JobManagerError::InvalidRequest(format!("failed to read current directory: {error}"))
        })?,
    };
    let canonical = cwd.canonicalize().map_err(|error| {
        JobManagerError::InvalidRequest(format!("invalid cwd '{}': {error}", cwd.display()))
    })?;
    if !canonical.is_dir() {
        return Err(JobManagerError::InvalidRequest(format!(
            "cwd '{}' is not a directory",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn random_job_id() -> String {
    let suffix: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect();
    format!("job_{suffix}")
}

fn serialize_result<T: Serialize>(result: Result<T, JobManagerError>) -> Value {
    match result {
        Ok(value) => serde_json::to_value(value).expect("job result json"),
        Err(error) => error_value(error.to_string()),
    }
}

fn error_value(message: String) -> Value {
    serde_json::json!({
        "status": "failed",
        "error": {
            "message": message
        }
    })
}

pub fn inject_chatmock_job_tools(payload: &mut Map<String, Value>) {
    let tools = payload
        .entry("tools")
        .or_insert_with(|| Value::Array(Vec::new()));
    let Some(tools) = tools.as_array_mut() else {
        return;
    };
    let existing = tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
        .collect::<std::collections::HashSet<_>>();
    for tool in chatmock_job_tools() {
        let name = tool.get("name").and_then(Value::as_str).unwrap_or_default();
        if !existing.contains(name) {
            tools.push(tool);
        }
    }
}

pub fn inject_chatmock_job_instructions(payload: &mut Map<String, Value>) {
    let current = payload
        .get("instructions")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let instructions = if current.trim().is_empty() {
        JOB_INSTRUCTIONS.to_string()
    } else if current.contains(JOB_INSTRUCTIONS) {
        current.to_string()
    } else {
        format!("{current}\n\n{JOB_INSTRUCTIONS}")
    };
    payload.insert("instructions".to_string(), Value::String(instructions));
}

fn chatmock_job_tools() -> Vec<Value> {
    vec![
        serde_json::json!({
            "type": "function",
            "name": "chatmock_start_job",
            "description": "Start a long-running background job managed by ChatMock. Returns quickly with a job_id.",
            "strict": false,
            "parameters": {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "enum": ["shell", "codex_subagent", "repo_scan", "test_run"]},
                    "command": {"type": "array", "items": {"type": "string"}},
                    "prompt": {"type": "string"},
                    "cwd": {"type": "string"},
                    "max_initial_wait_ms": {"type": "integer", "minimum": 0, "maximum": MAX_TOOL_WAIT_MS}
                },
                "required": ["kind"]
            }
        }),
        serde_json::json!({
            "type": "function",
            "name": "chatmock_poll_job",
            "description": "Poll a running ChatMock job for status and new output.",
            "strict": false,
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "since_offset": {"type": "integer"},
                    "max_wait_ms": {"type": "integer", "minimum": 0, "maximum": MAX_TOOL_WAIT_MS}
                },
                "required": ["job_id"]
            }
        }),
        serde_json::json!({
            "type": "function",
            "name": "chatmock_read_job_output",
            "description": "Read buffered output from a ChatMock job.",
            "strict": false,
            "parameters": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "since_offset": {"type": "integer"},
                    "max_bytes": {"type": "integer", "minimum": 0}
                },
                "required": ["job_id"]
            }
        }),
        serde_json::json!({
            "type": "function",
            "name": "chatmock_get_job_result",
            "description": "Get final result of a completed ChatMock job.",
            "strict": false,
            "parameters": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"]
            }
        }),
        serde_json::json!({
            "type": "function",
            "name": "chatmock_cancel_job",
            "description": "Cancel a running ChatMock job.",
            "strict": false,
            "parameters": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"]
            }
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wait_until_terminal(manager: &JobManager, job_id: &str) -> JobPollResult {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let polled = manager
                .poll_job(PollJobArgs {
                    job_id: job_id.to_string(),
                    since_offset: Some(0),
                    max_wait_ms: Some(100),
                })
                .await
                .expect("poll job");
            if polled.status.terminal() {
                return polled;
            }
            assert!(Instant::now() < deadline, "job did not finish in time");
        }
    }

    #[tokio::test]
    async fn manager_starts_polls_and_completes_shell_job() {
        let manager = Arc::new(JobManager::new(JobManagerConfig::default()));
        let command = if cfg!(windows) {
            vec![
                "cmd".to_string(),
                "/C".to_string(),
                "echo hello".to_string(),
            ]
        } else {
            vec!["sh".to_string(), "-c".to_string(), "echo hello".to_string()]
        };
        let started = manager
            .start_job(StartJobArgs {
                kind: JobKind::Shell,
                command: Some(command),
                prompt: None,
                cwd: None,
                max_initial_wait_ms: Some(1000),
            })
            .await
            .expect("start job");

        let polled = wait_until_terminal(&manager, &started.job_id).await;
        assert_eq!(polled.status, JobStatus::Completed);
        assert!(polled.output.contains("hello"));
        assert_eq!(polled.result.expect("result").exit_code, Some(0));
    }

    #[tokio::test]
    async fn manager_can_cancel_running_job() {
        let manager = Arc::new(JobManager::new(JobManagerConfig::default()));
        let command = if cfg!(windows) {
            vec![
                "cmd".to_string(),
                "/C".to_string(),
                "ping -n 20 127.0.0.1 > nul".to_string(),
            ]
        } else {
            vec!["sh".to_string(), "-c".to_string(), "sleep 20".to_string()]
        };
        let started = manager
            .start_job(StartJobArgs {
                kind: JobKind::Shell,
                command: Some(command),
                prompt: None,
                cwd: None,
                max_initial_wait_ms: Some(0),
            })
            .await
            .expect("start job");
        let cancel = manager
            .cancel_job(JobIdArgs {
                job_id: started.job_id.clone(),
            })
            .await
            .expect("cancel");
        assert!(cancel.cancelled);
        let polled = wait_until_terminal(&manager, &started.job_id).await;
        assert_eq!(polled.status, JobStatus::Cancelled);
    }

    #[tokio::test]
    async fn completed_job_gc_removes_expired_entries() {
        let manager = Arc::new(JobManager::new(JobManagerConfig {
            completed_ttl: Duration::from_millis(1),
            ..JobManagerConfig::default()
        }));
        let command = if cfg!(windows) {
            vec!["cmd".to_string(), "/C".to_string(), "echo done".to_string()]
        } else {
            vec!["sh".to_string(), "-c".to_string(), "echo done".to_string()]
        };
        let started = manager
            .start_job(StartJobArgs {
                kind: JobKind::Shell,
                command: Some(command),
                prompt: None,
                cwd: None,
                max_initial_wait_ms: Some(1000),
            })
            .await
            .expect("start job");
        let _ = wait_until_terminal(&manager, &started.job_id).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        manager.gc().await;
        let err = manager
            .get_job_result(JobIdArgs {
                job_id: started.job_id,
            })
            .await
            .expect_err("job should be gc'd");
        assert!(matches!(err, JobManagerError::NotFound(_)));
    }

    #[tokio::test]
    async fn output_buffer_keeps_bounded_tail() {
        let manager = Arc::new(JobManager::new(JobManagerConfig {
            max_output_bytes: 8,
            ..JobManagerConfig::default()
        }));
        let command = if cfg!(windows) {
            vec![
                "cmd".to_string(),
                "/C".to_string(),
                "echo 123456789abcdef".to_string(),
            ]
        } else {
            vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf 123456789abcdef".to_string(),
            ]
        };
        let started = manager
            .start_job(StartJobArgs {
                kind: JobKind::Shell,
                command: Some(command),
                prompt: None,
                cwd: None,
                max_initial_wait_ms: Some(0),
            })
            .await
            .expect("start job");
        let _ = wait_until_terminal(&manager, &started.job_id).await;
        let output = manager
            .read_job_output(ReadJobOutputArgs {
                job_id: started.job_id,
                since_offset: Some(0),
                max_bytes: None,
            })
            .await
            .expect("read output");
        assert!(output.output.len() <= 8);
    }
}
