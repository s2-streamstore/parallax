use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunId(pub String);

impl RunId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

fn short_id(s: &str, n: usize) -> &str {
    &s[..s.len().min(n)]
}

impl RunId {
    pub fn short(&self) -> &str { short_id(&self.0, 8) }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub run_id: RunId,
    pub event_type: EventType,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EventType {
    RunStarted {
        goal: String,
        task_count: usize,
    },
    RunCompleted,
    RunFailed {
        error: String,
    },
}

impl Event {
    fn now() -> u64 {
        chrono::Utc::now().timestamp_millis() as u64
    }

    pub fn run_started(run_id: &RunId, goal: &str, task_count: usize) -> Self {
        Self {
            run_id: run_id.clone(),
            event_type: EventType::RunStarted {
                goal: goal.to_string(),
                task_count,
            },
            timestamp: Self::now(),
        }
    }

    pub fn run_completed(run_id: &RunId) -> Self {
        Self {
            run_id: run_id.clone(),
            event_type: EventType::RunCompleted,
            timestamp: Self::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDef {
    pub id: String,
    pub title: String,
    pub prompt: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub files: Vec<String>,
    /// Agent backend override (e.g. "claude", "codex"). None = use swarm default.
    #[serde(default)]
    pub agent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmMessage {
    pub from: String,
    /// Target agent, or None for broadcast.
    #[serde(default)]
    pub to: Option<String>,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub content: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub timestamp: u64,
}

fn deserialize_timestamp<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<u64, D::Error> {
    let v: serde_json::Value = serde::Deserialize::deserialize(d)?;
    match v {
        serde_json::Value::Number(n) => Ok(n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)).unwrap_or(0)),
        _ => Ok(0),
    }
}

impl SwarmMessage {
    fn now() -> u64 {
        chrono::Utc::now().timestamp_millis() as u64
    }

    pub fn steer(content: &str, to: Option<&str>) -> Self {
        Self {
            from: "human".into(),
            to: to.map(String::from),
            msg_type: "steer".into(),
            content: content.into(),
            timestamp: Self::now(),
        }
    }
}
