use tokio::process::Command;

use crate::error::{ExecutorError, OrchestratorError, Result};

#[derive(Clone, Debug)]
pub enum AgentBackend {
    Claude { model: String },
    Codex,
}

impl AgentBackend {
    pub fn from_str(s: &str, model: &str) -> Self {
        match s.to_lowercase().as_str() {
            "codex" => Self::Codex,
            _ => Self::Claude {
                model: model.to_string(),
            },
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Claude { .. } => "claude".into(),
            Self::Codex => "codex".into(),
        }
    }

    /// Returns model string for Claude, "codex" for Codex.
    pub fn model(&self) -> &str {
        match self {
            Self::Claude { model } => model,
            Self::Codex => "codex",
        }
    }

    pub async fn prompt(&self, system: &str, user_message: &str) -> Result<String> {
        match self {
            Self::Claude { model } => {
                let mut args = vec![
                    "-p",
                    user_message,
                    "--output-format",
                    "text",
                    "--dangerously-skip-permissions",
                    "--max-turns",
                    "10",
                    "--tools",
                    "default",
                ];
                if !system.is_empty() {
                    args.extend_from_slice(&["--system-prompt", system]);
                }
                if !model.is_empty() {
                    args.extend_from_slice(&["--model", model]);
                }
                let output = Command::new("claude")
                    .args(&args)
                    .env_remove("CLAUDECODE")
                    .output()
                    .await
                    .map_err(|e| {
                        OrchestratorError::Worker(format!("Failed to run claude: {e}"))
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(OrchestratorError::Worker(format!(
                        "Claude prompt failed (exit {:?}): {}",
                        output.status.code(),
                        stderr.trim()
                    )));
                }

                Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
            }

            Self::Codex => {
                let full_prompt = if system.is_empty() {
                    user_message.to_string()
                } else {
                    format!("{}\n\n{}", system, user_message)
                };

                let output = Command::new("codex")
                    .args([
                        "-a",
                        "never",
                        "--sandbox",
                        "read-only",
                        "exec",
                        "--skip-git-repo-check",
                        "--color",
                        "never",
                        &full_prompt,
                    ])
                    .output()
                    .await
                    .map_err(|e| {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            OrchestratorError::Executor(ExecutorError::CodexNotFound)
                        } else {
                            OrchestratorError::Executor(ExecutorError::CodexSpawn(e))
                        }
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let detail = if stderr.trim().is_empty() {
                        stdout.trim()
                    } else {
                        stderr.trim()
                    };
                    return Err(OrchestratorError::Executor(ExecutorError::CodexFailed(
                        output.status.code(),
                        detail.to_string(),
                    )));
                }

                // Codex outputs a lot of metadata — extract just the last non-empty line
                let stdout = String::from_utf8_lossy(&output.stdout);
                let response = stdout
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .last()
                    .unwrap_or(stdout.trim())
                    .to_string();

                Ok(response)
            }
        }
    }
}
