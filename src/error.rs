use miette::Diagnostic;
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
pub enum OrchestratorError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("S2 error: {0}")]
    S2(#[from] s2_sdk::types::S2Error),

    #[error("S2 connection failed: {0}")]
    S2Init(String),

    #[error("Planner error: {0}")]
    Planner(String),

    #[error("Executor error: {0}")]
    Executor(#[from] ExecutorError),

    #[error("Worker error: {0}")]
    Worker(String),

    #[error("Research error: {0}")]
    Research(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}

#[derive(Error, Debug, Diagnostic)]
pub enum ExecutorError {
    #[error("Codex CLI not found. Is `codex` installed and in PATH?")]
    CodexNotFound,

    #[error("Codex CLI exited with code {0:?}: {1}")]
    CodexFailed(Option<i32>, String),

    #[error("Failed to spawn codex: {0}")]
    CodexSpawn(#[source] std::io::Error),
}

pub type Result<T> = std::result::Result<T, OrchestratorError>;
