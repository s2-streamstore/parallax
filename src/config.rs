use crate::error::{OrchestratorError, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub s2: S2Config,
    #[serde(default)]
    pub anthropic: AnthropicConfig,
    #[serde(default)]
    pub hooks: HooksConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HooksConfig {
    /// Shell command to run after each task completes (receives task title as $1)
    pub on_task_complete: Option<String>,
    /// Shell command to run after all branches merge
    pub on_merge: Option<String>,
    /// Shell command to run after the entire swarm finishes
    pub on_finish: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S2Config {
    pub access_token: Option<String>,
    pub basin: Option<String>,
    pub account_endpoint: Option<String>,
    pub basin_endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicConfig {
    pub api_key: Option<String>,
    /// Model for planner (task decomposition)
    #[serde(default = "default_model")]
    pub model: String,
    /// Model for spawned Claude Code agents (default: sonnet)
    #[serde(default = "default_agent_model")]
    pub agent_model: String,
}

impl Default for AnthropicConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            model: default_model(),
            agent_model: default_agent_model(),
        }
    }
}

fn default_model() -> String {
    "claude-sonnet-4-5-20250929".to_string()
}

pub fn default_agent_model() -> String {
    String::new()
}

impl Config {
    /// Priority: env vars > config file > defaults.
    pub fn load() -> Result<Self> {
        let mut config = Self::load_from_file().unwrap_or_default();

        if let Ok(token) = std::env::var("S2_ACCESS_TOKEN") {
            config.s2.access_token = Some(token);
        }
        if let Ok(basin) = std::env::var("PARALLAX_BASIN") {
            config.s2.basin = Some(basin);
        }
        if let Ok(endpoint) = std::env::var("S2_ACCOUNT_ENDPOINT") {
            config.s2.account_endpoint = Some(endpoint);
        }
        if let Ok(endpoint) = std::env::var("S2_BASIN_ENDPOINT") {
            config.s2.basin_endpoint = Some(endpoint);
        }
        if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
            config.anthropic.api_key = Some(key);
        }
        if let Ok(model) = std::env::var("PARALLAX_MODEL") {
            config.anthropic.model = model;
        }
        if let Ok(model) = std::env::var("ARC_AGENT_MODEL") {
            config.anthropic.agent_model = model;
        }

        Ok(config)
    }

    fn load_from_file() -> Option<Self> {
        let path = config_path()?;
        let content = std::fs::read_to_string(&path).ok()?;
        match toml::from_str(&content) {
            Ok(config) => Some(config),
            Err(e) => {
                eprintln!("Warning: failed to parse config at {}: {}", path.display(), e);
                None
            }
        }
    }

    pub fn s2_access_token(&self) -> Result<&str> {
        self.s2
            .access_token
            .as_deref()
            .ok_or_else(|| OrchestratorError::Config(
                "S2 access token not set. Set S2_ACCESS_TOKEN env var or add to config file.".into(),
            ))
    }

    pub fn basin_name<'a>(&'a self, cli_override: Option<&'a str>) -> Result<&'a str> {
        cli_override
            .or(self.s2.basin.as_deref())
            .ok_or_else(|| OrchestratorError::Config(
                "Basin name not set. Use --basin flag, PARALLAX_BASIN env var, or add to config file.".into(),
            ))
    }

    pub fn anthropic_api_key(&self) -> Result<&str> {
        self.anthropic
            .api_key
            .as_deref()
            .ok_or_else(|| OrchestratorError::Config(
                "Anthropic API key not set. Set ANTHROPIC_API_KEY env var or add to config file.".into(),
            ))
    }
}

fn config_path() -> Option<PathBuf> {
    // Prefer XDG-style ~/.config/arc/config.toml; fall back to platform dir
    let xdg_path = dirs::home_dir().map(|d| d.join(".config").join("parallax").join("config.toml"));
    if let Some(ref p) = xdg_path {
        if p.exists() {
            return xdg_path;
        }
    }
    let platform_path = dirs::config_dir().map(|d| d.join("parallax").join("config.toml"));
    if let Some(ref p) = platform_path {
        if p.exists() {
            return platform_path;
        }
    }
    xdg_path
}
