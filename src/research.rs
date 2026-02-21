use colored::Colorize;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::agent::AgentBackend;
use crate::cli::ResearchAgentMode;
use crate::config::Config;
use crate::error::Result;
use crate::signal::wait_for_shutdown_signal;
use crate::streams::{self, OrchestratorStreams};
use crate::swarm::{ResearchGroup, Strategy, StrategyType};
use crate::types::*;

// ───────────────────────────────────────────────────────────────────
// Types — Generic Strategy System
// ───────────────────────────────────────────────────────────────────

/// A complete research strategy designed by the planner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchStrategy {
    pub name: String,
    pub question: String,
    pub topology: StreamTopology,
    pub execution: ExecutionPlan,
    pub aggregation: Vec<AggregationStep>,
    pub synthesis: SynthesisConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamTopology {
    #[serde(rename = "groups")]
    Groups { groups: Vec<GroupDef> },

    #[serde(rename = "rounds")]
    Rounds(RoundsConfig),

    #[serde(rename = "hierarchical")]
    Hierarchical {
        root_groups: Vec<GroupDef>,
        #[serde(default)]
        spawn_rules: Vec<String>,
    },

    #[serde(rename = "custom")]
    Custom {
        stream_names: Vec<String>,
        #[serde(default)]
        relationships: Vec<(String, String)>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupDef {
    pub name: String,
    pub prompt: String,
    pub agents: usize,
    /// Agent backend override: "claude", "codex", or omit for session default.
    #[serde(default)]
    pub agent: Option<String>,
}

/// Rounds configuration - supports both simple and complex multi-round designs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RoundsConfig {
    /// Simple: just specify number of rounds and instances
    Simple {
        rounds: usize,
        instances_per_round: usize,
    },
    /// Complex: different groups per round (sophisticated Delphi)
    Complex {
        rounds: Vec<RoundSpec>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoundSpec {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub round: usize,
    pub name: String,
    #[serde(alias = "streams")]
    pub groups: Vec<GroupDef>,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// Round identifier - can be numeric index or string name
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum RoundIdentifier {
    Index(usize),
    Name(String),
}

impl std::fmt::Display for RoundIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoundIdentifier::Index(i) => write!(f, "{}", i),
            RoundIdentifier::Name(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub agent_mode: AgentMode,
    pub agent_count: usize,
    pub distribution: AgentDistribution,
    pub max_messages_per_agent: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AgentMode {
    #[serde(rename = "persistent_chat")]
    PersistentChat { system_prompt: String },

    #[serde(rename = "one_shot")]
    OneShot {
        prompt_template: String,
        #[serde(default)]
        output_format: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AgentDistribution {
    #[serde(rename = "per_stream")]
    PerStream { count: usize },

    #[serde(rename = "even_split")]
    EvenSplit,

    #[serde(rename = "custom")]
    Custom {
        allocations: Vec<(String, usize)>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationStep {
    pub trigger: AggregationTrigger,
    pub sources: Vec<String>,
    pub method: AggregationMethod,
    pub destination: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AggregationTrigger {
    #[serde(rename = "message_count")]
    MessageCount { count: usize },

    #[serde(rename = "round_end", alias = "round_complete")]
    RoundEnd {
        #[serde(with = "round_id_serde")]
        round: RoundIdentifier
    },

    #[serde(rename = "budget_percent")]
    BudgetPercent { percent: f64 },

    #[serde(rename = "all_complete")]
    AllComplete,
}

mod round_id_serde {
    use super::RoundIdentifier;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(id: &RoundIdentifier, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match id {
            RoundIdentifier::Index(i) => serializer.serialize_u64(*i as u64),
            RoundIdentifier::Name(s) => serializer.serialize_str(s),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<RoundIdentifier, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrInt {
            Int(usize),
            String(String),
        }

        match StringOrInt::deserialize(deserializer)? {
            StringOrInt::Int(i) => Ok(RoundIdentifier::Index(i)),
            StringOrInt::String(s) => Ok(RoundIdentifier::Name(s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AggregationMethod {
    #[serde(rename = "llm_synthesis")]
    LLMSynthesis { prompt: String },

    #[serde(rename = "statistical")]
    Statistical { metrics: Vec<String> },

    #[serde(rename = "collect")]
    Collect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SynthesisConfig {
    pub input_streams: Vec<String>,
    pub prompt_template: String,
    #[serde(default)]
    pub sections: Vec<String>,
}

// ───────────────────────────────────────────────────────────────────
// Display
// ───────────────────────────────────────────────────────────────────

const GROUP_COLORS: &[colored::Color] = &[
    colored::Color::BrightCyan,
    colored::Color::BrightYellow,
    colored::Color::BrightGreen,
    colored::Color::BrightMagenta,
    colored::Color::BrightBlue,
    colored::Color::BrightRed,
];

fn group_color(group_name: &str, all_groups: &[String]) -> colored::Color {
    let idx = all_groups
        .iter()
        .position(|g| g == group_name)
        .unwrap_or(0);
    GROUP_COLORS[idx % GROUP_COLORS.len()]
}

fn print_strategy_header(strategy: &ResearchStrategy, swarm_id: &RunId) {
    println!();
    println!("{}", "━".repeat(70).bright_cyan());
    println!(
        "{} {} {}",
        "RESEARCH".bright_cyan().bold(),
        "//".dimmed(),
        strategy.name.bold()
    );
    println!("{}", "━".repeat(70).bright_cyan());
    println!("  {} {}", "Question:".dimmed(), strategy.question);
    println!("  {} parallax join {}", "Join:".dimmed(), swarm_id.0);
    println!("  {} swarm/{}/moderator/decisions", "Moderator log:".dimmed(), swarm_id.short());
    println!();

    // Topology
    println!("  {} {}",  "[TOPOLOGY]".bold(), "//".dimmed());
    match &strategy.topology {
        StreamTopology::Groups { groups } => {
            println!("    Type: {} ({} groups, {} total agents)",
                "Independent Groups".bright_green(),
                groups.len(),
                strategy.execution.agent_count
            );
            let group_names: Vec<String> = groups.iter().map(|g| g.name.clone()).collect();
            for group in groups {
                let color = group_color(&group.name, &group_names);
                println!(
                    "      {} {} ({} agents) — {}",
                    "→".color(color),
                    group.name.color(color).bold(),
                    group.agents,
                    truncate_display(&group.prompt, 50).dimmed()
                );
            }
        }
        StreamTopology::Rounds(rounds_config) => {
            match rounds_config {
                RoundsConfig::Simple { rounds, instances_per_round } => {
                    println!("    Type: {} ({} panelists, {} rounds)",
                        "Multi-Round Convergence".bright_green(),
                        instances_per_round,
                        rounds
                    );
                }
                RoundsConfig::Complex { rounds } => {
                    let total_groups: usize = rounds.iter().map(|r| r.groups.len()).sum();
                    println!("    Type: {} ({} rounds, {} total groups)",
                        "Complex Delphi".bright_green(),
                        rounds.len(),
                        total_groups
                    );
                }
            }
        }
        StreamTopology::Hierarchical { root_groups, .. } => {
            println!("    Type: {} ({} root groups, dynamic breakouts enabled)",
                "Hierarchical with Breakouts".bright_green(),
                root_groups.len()
            );
        }
        _ => {
            println!("    Type: {}", "Custom Topology".bright_green());
        }
    }
    println!();

    // Execution
    println!("  {} {}",  "[EXECUTION]".bold(), "//".dimmed());
    match &strategy.execution.agent_mode {
        AgentMode::PersistentChat { system_prompt } => {
            println!("    Mode: {} (agents discuss and build on findings)",
                "Persistent Chat".bright_yellow()
            );
            println!("    System: {}", truncate_display(system_prompt, 60).dimmed());
        }
        AgentMode::OneShot { prompt_template: _, output_format } => {
            println!("    Mode: {} (one estimate per agent per round)",
                "One-Shot".bright_yellow()
            );
            println!("    Output: {}", output_format.dimmed());
        }
    }
    println!("    Budget: {} messages per agent", strategy.execution.max_messages_per_agent);
    println!();

    // Aggregation
    if !strategy.aggregation.is_empty() {
        println!("  {} {}",  "[AGGREGATION]".bold(), "//".dimmed());
        for (i, agg) in strategy.aggregation.iter().enumerate() {
            let trigger_desc = match &agg.trigger {
                AggregationTrigger::BudgetPercent { percent } => format!("at {:.0}% of budget", percent * 100.0),
                AggregationTrigger::MessageCount { count } => format!("after {} messages", count),
                AggregationTrigger::RoundEnd { round } => format!("end of round {}", round),
                AggregationTrigger::AllComplete => "when all agents complete".to_string(),
            };
            let method_desc = match &agg.method {
                AggregationMethod::LLMSynthesis { .. } => "LLM synthesis",
                AggregationMethod::Statistical { .. } => "Statistical (median, range)",
                AggregationMethod::Collect => "Collect",
            };
            println!("    {} {} → {}", (i + 1), trigger_desc.dimmed(), method_desc.bright_magenta());
        }
        println!();
    }

    println!("  {} {}",  "[MODERATOR]".bold(), "//".dimmed());
    println!("    {} swarm/{}/moderator/decisions",
        "Log:".dimmed(),
        swarm_id.short()
    );
    println!();
    println!("{}", "━".repeat(70).bright_cyan());
    println!();
}

fn print_phase(phase: &str, detail: &str) {
    println!();
    println!(
        "  {} {} {}",
        "---".dimmed(),
        phase.bright_cyan().bold(),
        detail.dimmed()
    );
    println!();
}

fn print_research_message(group_name: &str, author: &str, text: &str, groups: &[String]) {
    let color = group_color(group_name, groups);
    println!(
        "  {} {} {}",
        format!("[{}]", group_name).color(color),
        format!("{}:", author).bold(),
        text
    );
}

fn print_topology_diagram(strategy: &ResearchStrategy) {
    println!();
    println!("  {} {}", "[TOPOLOGY DIAGRAM]".bold(), "//".dimmed());
    println!();

    match &strategy.topology {
        StreamTopology::Groups { groups } => {
            let max_name = groups.iter().map(|g| g.name.len()).max().unwrap_or(10);
            for (i, g) in groups.iter().enumerate() {
                let backend = g.agent.as_deref().unwrap_or("claude");
                let pipe = if i == groups.len() - 1 { "└" } else { "├" };
                println!(
                    "    {} {:>width$}  ×{} {} ──→  swarm/*/group/{}",
                    pipe,
                    g.name,
                    g.agents,
                    format!("[{}]", backend).dimmed(),
                    g.name,
                    width = max_name,
                );
            }
            println!("    {}",  "│".dimmed());
            println!("    {} {}", "▼".dimmed(), "moderator reads all streams, drives topology".dimmed());
            println!("    {}",  "│".dimmed());
            println!("    {} {}", "▼".dimmed(), "synthesis".dimmed());
        }

        StreamTopology::Rounds(RoundsConfig::Simple { rounds, instances_per_round }) => {
            for r in 1..=*rounds {
                let label = format!("Round {}", r);
                println!("    {} {}", if r == 1 { "┌" } else { "├" }, label.bold());
                for i in 0..*instances_per_round {
                    let pipe = if i == instances_per_round - 1 { "└" } else { "├" };
                    println!("    │  {} panelist-{}  ──→  swarm/*/round/{}/panelist-{}", pipe, i, r, i);
                }
                if r < *rounds {
                    println!("    {} {}", "│  ▼".dimmed(), "moderator aggregates, carries context forward".dimmed());
                }
            }
            println!("    {}",  "│".dimmed());
            println!("    {} {}", "▼".dimmed(), "synthesis".dimmed());
        }

        StreamTopology::Rounds(RoundsConfig::Complex { rounds }) => {
            for (ri, spec) in rounds.iter().enumerate() {
                let label = format!("Round {} — {}", spec.round, spec.name);
                println!("    {} {}", if ri == 0 { "┌" } else { "├" }, label.bold());
                for g in &spec.groups {
                    let backend = g.agent.as_deref().unwrap_or("claude");
                    println!("    │  ├ {}  ×{} {} ──→  swarm/*/round/{}/group/{}", g.name, g.agents, format!("[{}]", backend).dimmed(), spec.round, g.name);
                }
                if ri < rounds.len() - 1 {
                    println!("    {} {}", "│  ▼".dimmed(), "moderator aggregates, starts next round".dimmed());
                }
            }
            println!("    {}",  "│".dimmed());
            println!("    {} {}", "▼".dimmed(), "synthesis".dimmed());
        }

        StreamTopology::Hierarchical { root_groups, .. } => {
            for g in root_groups {
                println!("    ├ {}  ×{} ──→  swarm/*/group/{}", g.name, g.agents, g.name);
                println!("    │  └ {}", "(moderator can spawn sub-streams dynamically)".dimmed());
            }
            println!("    {}",  "│".dimmed());
            println!("    {} {}", "▼".dimmed(), "synthesis".dimmed());
        }

        StreamTopology::Custom { stream_names, relationships } => {
            for name in stream_names {
                println!("    ├ {} ──→  swarm/*/{}", name, name);
            }
            if !relationships.is_empty() {
                println!("    {}", "│".dimmed());
                for (from, to) in relationships {
                    println!("    {} {} → {}", "↗".dimmed(), from, to);
                }
            }
            println!("    {}",  "│".dimmed());
            println!("    {} {}", "▼".dimmed(), "synthesis".dimmed());
        }
    }

    println!();
}

fn print_strategy_report(strategy: &ResearchStrategy, report: &str) {
    println!();
    println!("{}", "━".repeat(60).bright_cyan());
    println!("{} {}", ">>".bright_cyan().bold(), strategy.name.to_uppercase().bold());
    println!("{}", "━".repeat(60).bright_cyan());
    println!();
    println!("{}", report);
    println!();
    println!("{}", "━".repeat(60).bright_cyan());
}

/// Convert a name to a kebab-case slug for use in S2 stream paths.
fn slugify(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Normalize moderator-provided stream names into safe relative paths.
/// Enforces a collaboration namespace and blocks reserved/synthesis paths.
fn normalize_stream_name(name: &str) -> Option<String> {
    let trimmed = name.trim().trim_matches('/');
    if trimmed.is_empty() {
        return None;
    }

    let lowered = trimmed.to_lowercase();
    if lowered.contains("synthesis")
        || lowered.starts_with("moderator")
        || lowered.starts_with("events")
        || lowered.starts_with("plan")
        || lowered.starts_with("join-")
    {
        return None;
    }

    let mut parts = trimmed
        .split('/')
        .map(slugify)
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }

    if parts.len() == 1 {
        Some(format!("group/{}", parts.remove(0)))
    } else {
        Some(parts.join("/"))
    }
}

pub fn truncate_display(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max.saturating_sub(3);
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

/// Truncate a findings string in-place at a UTF-8 char boundary, appending a marker.
pub fn truncate_findings(s: &mut String, max: usize) {
    if s.len() > max {
        let mut at = max;
        while at > 0 && !s.is_char_boundary(at) {
            at -= 1;
        }
        s.truncate(at);
        s.push_str("\n\n[TRUNCATED]");
    }
}


// ─────────────────────────────────────────────────────────────────────────────
// Generic execution engine
//
// ALL topologies — groups, rounds (simple/complex), hierarchical, custom —
// run through one loop: extract_initial_streams → execute_generic → moderator
// drives everything from there via JSON actions.
//
// The planner's topology is the starting configuration. From that point the
// moderator can add streams, transition phases, inject context, or conclude.
// ─────────────────────────────────────────────────────────────────────────────

/// A single stream spec used during execution.
/// `name` is relative to the swarm (e.g. "group/Skeptics", "round/1/panelist-2").
#[derive(Debug, Clone)]
struct StreamSpec {
    name: String,
    prompt: String,
    agents: usize,
    /// Per-stream agent backend override. None = use session default.
    agent: Option<String>,
}

#[derive(Debug, Clone)]
enum ModeratorAction {
    Continue,
    /// Open a new stream (breakout, new perspective, sub-investigation).
    /// `reads_from` lists existing streams whose current findings are injected as context.
    AddStream {
        name: String,
        prompt: String,
        agents: usize,
        reads_from: Vec<String>,
        agent: Option<String>,
    },
    /// Inject a steering message into one or more streams.
    Steer {
        target: String, // stream name or "all"
        message: String,
    },
    /// Transition to a new phase: collect context from done streams, seed new ones.
    /// This is how Delphi rounds work: moderator sees round N is done → StartPhase.
    StartPhase {
        context_from: Vec<String>,
        streams: Vec<StreamSpec>,
    },
    /// Kill agents on a specific stream. Use when a line of inquiry is unproductive.
    StopStream {
        target: String,
    },
    /// Ask a stream (or all streams) to post a final summary.
    WrapUp {
        target: Option<String>,
    },
    /// Finalize: stop agents, move to synthesis.
    Conclude,
}

/// Ask Claude to respond with a JSON action object:
/// {"action": "continue"|"add_stream"|"steer"|"start_phase"|"wrap_up"|"conclude", ...}
fn parse_moderator_action(output: &str) -> ModeratorAction {
    // Try to extract and parse JSON
    let json_str = extract_json_object(output).unwrap_or_else(|| output.to_string());
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&json_str) {
        match v["action"].as_str().unwrap_or("").to_lowercase().as_str() {
            "continue" => return ModeratorAction::Continue,
            "conclude" => return ModeratorAction::Conclude,
            "stop_stream" | "stop" => {
                return ModeratorAction::StopStream {
                    target: v["target"].as_str().unwrap_or("").to_string(),
                };
            }
            "wrap_up" | "wrap" => {
                return ModeratorAction::WrapUp {
                    target: v["target"].as_str().filter(|s| *s != "all").map(String::from),
                };
            }
            "steer" => {
                return ModeratorAction::Steer {
                    target: v["target"].as_str().unwrap_or("all").to_string(),
                    message: v["message"].as_str().unwrap_or("").to_string(),
                };
            }
            "add_stream" => {
                return ModeratorAction::AddStream {
                    name: v["name"].as_str().unwrap_or("breakout").to_string(),
                    prompt: v["prompt"].as_str().unwrap_or("").to_string(),
                    agents: v["agents"].as_u64().unwrap_or(2) as usize,
                    reads_from: v["reads_from"].as_array()
                        .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    agent: v["agent"].as_str().map(String::from),
                };
            }
            "start_phase" => {
                let streams = v["streams"].as_array().map(|arr| {
                    arr.iter().filter_map(|s| {
                        Some(StreamSpec {
                            name: s["name"].as_str()?.to_string(),
                            prompt: s["prompt"].as_str().unwrap_or("").to_string(),
                            agents: s["agents"].as_u64().unwrap_or(3) as usize,
                            agent: s["agent"].as_str().map(String::from),
                        })
                    }).collect()
                }).unwrap_or_default();
                let context_from = v["context_from"].as_array()
                    .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                return ModeratorAction::StartPhase { context_from, streams };
            }
            _ => {}
        }
    }

    // Text fallback for robustness
    let u = output.to_uppercase();
    if u.contains("CONCLUDE") || u.contains("RESEARCH COMPLETE") || u.contains("SYNTHESIZE NOW") {
        ModeratorAction::Conclude
    } else if u.contains("WRAP") {
        ModeratorAction::WrapUp { target: None }
    } else {
        ModeratorAction::Continue
    }
}

/// Pull the first {...} JSON object out of a text response.
fn extract_json_object(text: &str) -> Option<String> {
    let start = text.find('{')?;
    let mut depth = 0i32;
    for (i, c) in text[start..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(text[start..start + i + 1].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

/// Convert any planner topology into the initial set of streams for `execute_generic`.
fn extract_initial_streams(strategy: &ResearchStrategy) -> Vec<StreamSpec> {
    match &strategy.topology {
        StreamTopology::Groups { groups } => groups.iter().map(|g| StreamSpec {
            name: format!("group/{}", slugify(&g.name)),
            prompt: g.prompt.clone(),
            agents: g.agents,
            agent: g.agent.clone(),
        }).collect(),

        StreamTopology::Rounds(RoundsConfig::Simple { instances_per_round, .. }) => {
            let prompt = match &strategy.execution.agent_mode {
                AgentMode::OneShot { prompt_template, .. } => prompt_template
                    .replace("{question}", &strategy.question)
                    .replace("{context}", "")
                    .replace("{panelist_id}", "0"),
                AgentMode::PersistentChat { system_prompt } => system_prompt.clone(),
            };
            (0..*instances_per_round).map(|i| StreamSpec {
                name: format!("round/1/panelist-{}", i),
                prompt: prompt.replace("{panelist_id}", &i.to_string()),
                agents: 1,
                agent: None,
            }).collect()
        }

        StreamTopology::Rounds(RoundsConfig::Complex { rounds }) => {
            rounds.first().map(|r| r.groups.iter().map(|g| StreamSpec {
                name: format!("round/{}/group/{}", r.round.max(1), slugify(&g.name)),
                prompt: g.prompt.clone(),
                agents: g.agents,
                agent: g.agent.clone(),
            }).collect()).unwrap_or_default()
        }

        StreamTopology::Hierarchical { root_groups, .. } => root_groups.iter().map(|g| StreamSpec {
            name: format!("group/{}", slugify(&g.name)),
            prompt: g.prompt.clone(),
            agents: g.agents,
            agent: g.agent.clone(),
        }).collect(),

        StreamTopology::Custom { stream_names, .. } => {
            let per_stream = (strategy.execution.agent_count / stream_names.len().max(1)).max(1);
            stream_names.iter().map(|name| StreamSpec {
                name: name.clone(),
                prompt: format!(
                    "You are a researcher on stream '{}'. Question: {}",
                    name, strategy.question
                ),
                agents: per_stream,
                agent: None,
            }).collect()
        }
    }
}

/// Seed a stream, spawn its agents, and wire it into the monitoring channel.
/// Agent and monitor handles are tracked per stream name so individual streams can be stopped.
async fn start_stream(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    spec: &StreamSpec,
    agent_mode: &AgentMode,
    default_backend: &AgentBackend,
    max_messages_per_agent: usize,
    child_groups: &std::sync::Arc<std::sync::Mutex<Vec<i32>>>,
    msg_tx: &tokio::sync::mpsc::UnboundedSender<(String, String, String)>,
    active_streams: &mut Vec<StreamSpec>,
    all_stream_names: &mut Vec<String>,
    stream_agents: &mut std::collections::HashMap<String, Vec<tokio::task::JoinHandle<()>>>,
    stream_monitors: &mut std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
) -> Result<()> {
    let full_name = format!("swarm/{}/{}", swarm_id.0, spec.name);
    let role_prompt = match agent_mode {
        AgentMode::PersistentChat { system_prompt } => {
            if system_prompt.trim().is_empty() {
                spec.prompt.clone()
            } else {
                format!("{}\n\n{}", system_prompt, spec.prompt)
            }
        }
        AgentMode::OneShot { .. } => spec.prompt.clone(),
    };

    // Resolve per-stream backend: use the stream's override if set, otherwise session default
    let backend = match &spec.agent {
        Some(agent_str) => AgentBackend::from_str(agent_str, default_backend.model()),
        None => default_backend.clone(),
    };

    crate::chat::append_to_stream(streams, &full_name, "System", &role_prompt).await?;

    let mut handles = Vec::new();
    match agent_mode {
        AgentMode::OneShot { prompt_template, .. } => {
            for i in 0..spec.agents {
                let b = backend.clone();
                let s = streams.clone();
                let sn = full_name.clone();
                let prompt = prompt_template
                    .replace("{question}", "")
                    .replace("{context}", "")
                    .replace("{panelist_id}", &i.to_string());
                let spec_prompt = role_prompt.clone();
                let stream_label = spec.name.clone();
                handles.push(tokio::spawn(async move {
                    let response = b.prompt(&spec_prompt, &prompt)
                        .await
                        .unwrap_or_else(|e| format!("(error: {e})"));
                    let author = format!("{}-panelist-{}", stream_label.split('/').last().unwrap_or(&stream_label), i);
                    let _ = crate::chat::append_to_stream(&s, &sn, &author, &response).await;
                }));
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
        AgentMode::PersistentChat { .. } => {
            if matches!(backend, AgentBackend::Claude { .. }) {
                let h = spawn_group_agents_on_stream(
                    streams, swarm_id, &spec.name,
                    &GroupDef { name: spec.name.clone(), prompt: role_prompt.clone(), agents: spec.agents, agent: spec.agent.clone() },
                    &backend, max_messages_per_agent, child_groups.clone(),
                ).await;
                handles.extend(h);
            } else {
                // Non-Claude backends (Codex, etc.) don't support persistent stream-json sessions.
                // Use poll-and-respond: read recent messages, call backend.prompt(), append response.
                for _ in 0..spec.agents {
                    let b = backend.clone();
                    let s = streams.clone();
                    let sn = full_name.clone();
                    let system = role_prompt.clone();
                    let display = spec.name.split('/').last().unwrap_or(&spec.name).to_string();
                    let agent_name = format!("{}-{}", display, &uuid::Uuid::new_v4().to_string()[..8]);
                    let max_msgs = max_messages_per_agent;

                    handles.push(tokio::spawn(async move {
                        let _ = run_polling_agent(&s, &sn, &agent_name, &system, &b, max_msgs).await;
                    }));
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                }
            }
        }
    }

    stream_agents.insert(spec.name.clone(), handles);
    stream_monitors.insert(spec.name.clone(), spawn_stream_monitor(
        streams.clone(), full_name.clone(), spec.name.clone(), msg_tx.clone(),
    ));

    active_streams.push(spec.clone());
    all_stream_names.push(full_name);

    Ok(())
}

/// Abort agents and monitor for a specific stream.
fn stop_stream(
    name: &str,
    active_streams: &mut Vec<StreamSpec>,
    stream_agents: &mut std::collections::HashMap<String, Vec<tokio::task::JoinHandle<()>>>,
    stream_monitors: &mut std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
) {
    if let Some(handles) = stream_agents.remove(name) {
        for h in handles { h.abort(); }
    }
    if let Some(monitor) = stream_monitors.remove(name) {
        monitor.abort();
    }
    active_streams.retain(|s| s.name != name);
}

/// Read multiple streams and return their messages as a single context string.
async fn collect_stream_context(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    stream_names: &[String],
) -> String {
    let mut context = String::new();
    for name in stream_names {
        let full = if name.starts_with("swarm/") {
            name.clone()
        } else {
            format!("swarm/{}/{}", swarm_id.0, name)
        };
        if let Ok(msgs) = crate::chat::read_stream_messages(streams, &full).await {
            let label = name.split('/').skip(2).collect::<Vec<_>>().join("/");
            let label = if label.is_empty() { name.as_str() } else { &label };
            context.push_str(&format!("\n### {}\n", label));
            for (author, text) in msgs {
                if author != "System" && author != "Moderator" {
                    context.push_str(&format!("- {}: {}\n", author, text));
                }
            }
        }
    }
    context
}

/// Build the moderator decision prompt with full context.
/// Poll-and-respond agent for non-Claude backends (Codex, etc.) that don't support
/// persistent bidirectional streaming. Reads the stream periodically, calls backend.prompt()
/// with recent messages as context, and appends the response.
async fn run_polling_agent(
    streams: &OrchestratorStreams,
    stream_name: &str,
    agent_name: &str,
    system: &str,
    backend: &AgentBackend,
    max_messages: usize,
) -> Result<()> {
    // Post introduction
    let intro = backend.prompt(
        system,
        &format!("Introduce yourself as {} and share your initial analysis.", agent_name),
    ).await?;
    if !intro.is_empty() {
        crate::chat::append_to_stream(streams, stream_name, agent_name, &intro).await?;
    }

    let mut my_messages = 1usize;
    let mut last_seen = 0usize;

    while my_messages < max_messages {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Read recent messages from the stream
        let msgs = crate::chat::read_stream_messages(streams, stream_name).await.unwrap_or_default();
        if msgs.len() <= last_seen {
            continue;
        }

        // Collect new messages from others
        let new: Vec<String> = msgs[last_seen..].iter()
            .filter(|(author, _)| author != agent_name && author != "System")
            .map(|(author, text)| format!("{}: {}", author, text))
            .collect();
        last_seen = msgs.len();

        if new.is_empty() {
            continue;
        }

        let response = backend.prompt(
            system,
            &format!(
                "Recent messages:\n{}\n\nRespond according to your role and strategy instructions. \
                 Build on, challenge, or refine specific prior claims when appropriate.",
                new.join("\n")
            ),
        ).await.unwrap_or_default();

        if !response.is_empty() {
            crate::chat::append_to_stream(streams, stream_name, agent_name, &response).await?;
            my_messages += 1;
        }
    }

    Ok(())
}

fn build_moderator_prompt(
    strategy: &ResearchStrategy,
    active_streams: &[StreamSpec],
    recent_activity: &str,
    summaries: &str,
    total_messages: usize,
    max_total: usize,
) -> String {
    let streams_list = active_streams.iter()
        .map(|s| format!("  - {} ({} agents)", s.name, s.agents))
        .collect::<Vec<_>>().join("\n");

    let summaries_section = if summaries.is_empty() {
        String::new()
    } else {
        format!("\n\nSTREAM SUMMARIES:\n{}", summaries)
    };

    // Serialize the full strategy so the moderator can understand the intended
    // research methodology (topology, rounds, aggregation, synthesis) and
    // decide how to drive it. This is the key to being truly generic —
    // the moderator figures out the protocol from the strategy itself.
    let strategy_json = serde_json::to_string_pretty(strategy)
        .unwrap_or_else(|_| format!("name: {}", strategy.name));

    format!(
        r#"You are moderating a multi-agent research session on S2 streams.

RESEARCH QUESTION: {question}

FULL STRATEGY (designed by planner):
```json
{strategy_json}
```

ACTIVE STREAMS:
{streams}

RECENT ACTIVITY:
{activity}{summaries}

PROGRESS: {total}/{max} messages

Your job: read the strategy to understand the intended methodology, then drive the
research using the actions below toward convergence and a final document.
The strategy is your starting point, not a constraint, but you must avoid endless
debate loops and unnecessary stream growth.

STRICT CONVERGENCE RULES:
- Prefer steer/wrap_up/conclude over add_stream.
- Do NOT create any stream with names containing "synthesis" or "moderator".
- Only use add_stream for genuinely missing perspectives.
- Once you issue wrap_up, your next action should be conclude.
- If the budget is beyond ~70%, strongly prefer conclude.

Respond with exactly ONE JSON action:

{{"action": "continue"}}
  → Let agents keep working.

{{"action": "add_stream", "name": "group/NewTopic", "prompt": "Investigation prompt", "agents": 2, "reads_from": ["group/ExistingStream"]}}
  → Open a new investigation stream. reads_from injects context from other streams.

{{"action": "steer", "target": "group/Name", "message": "Guidance"}}
  → Inject a message into a stream (or "all" streams).

{{"action": "start_phase", "context_from": ["current/streams"], "streams": [{{"name": "next/stream", "prompt": "Next phase prompt", "agents": 3}}]}}
  → Transition to a new phase. Wraps up current streams, collects their findings,
    and starts new streams with that context injected. Use this for round transitions.

{{"action": "stop_stream", "target": "group/UnproductiveGroup"}}
  → Kill agents on a specific stream. Use when a line of inquiry is unproductive.

{{"action": "wrap_up", "target": "all"}}
  → Ask streams to post final summaries.

{{"action": "conclude"}}
  → End the research and move to final synthesis."#,
        question = strategy.question,
        strategy_json = strategy_json,
        streams = streams_list,
        activity = recent_activity,
        summaries = summaries_section,
        total = total_messages,
        max = max_total,
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream monitoring
// ─────────────────────────────────────────────────────────────────────────────

/// Spawn a background task that monitors one S2 stream and forwards parsed messages to a channel.
/// Each stream gets its own task so new streams (breakouts, new phases) can be added at runtime.
fn spawn_stream_monitor(
    streams: OrchestratorStreams,
    stream_name: String,
    label: String,
    tx: tokio::sync::mpsc::UnboundedSender<(String, String, String)>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let s2_stream = match streams.stream(&stream_name) {
            Ok(s) => s,
            Err(_) => return,
        };
        let input = s2_sdk::types::ReadInput::new()
            .with_start(s2_sdk::types::ReadStart::new()
                .with_from(s2_sdk::types::ReadFrom::SeqNum(0)));
        let mut session = match s2_stream.read_session(input).await {
            Ok(s) => s,
            Err(_) => return,
        };
        while let Some(batch) = session.next().await {
            if let Ok(batch) = batch {
                for record in &batch.records {
                    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&record.body) {
                        let author = msg["author"].as_str().unwrap_or("?").to_string();
                        let text = msg["message"].as_str().unwrap_or("").to_string();
                        if author != "System" && !text.is_empty() {
                            let _ = tx.send((label.clone(), author, text));
                        }
                    }
                }
            }
        }
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Agent spawning
// ─────────────────────────────────────────────────────────────────────────────

async fn spawn_group_agents_on_stream(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    stream_path: &str,
    group: &GroupDef,
    backend: &AgentBackend,
    max_messages: usize,
    child_groups: std::sync::Arc<std::sync::Mutex<Vec<i32>>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for _ in 0..group.agents {
        let s = streams.clone();
        let sid = swarm_id.clone();
        let stream_name = format!("swarm/{}/{}", sid.0, stream_path);
        let gprompt = group.prompt.clone();
        let b = backend.clone();
        let display = stream_path.split('/').last().unwrap_or(stream_path);
        let agent_name = format!("{}-{}", display, &uuid::Uuid::new_v4().to_string()[..8]);
        let groups = child_groups.clone();

        handles.push(tokio::spawn(async move {
            let _ = crate::chat::run_persistent_chat_agent_with_group_list(
                &s, &stream_name, &agent_name, &gprompt, &[], max_messages, b.model(), groups,
            ).await;
        }));

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    handles
}

// ─────────────────────────────────────────────────────────────────────────────
// Unified executor
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_strategy(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    strategy: &ResearchStrategy,
    backend: &AgentBackend,
    max_dynamic_streams: Option<usize>,
    max_phase_transitions: usize,
    timeout_minutes: Option<u64>,
) -> Result<String> {
    execute_generic(
        streams,
        swarm_id,
        strategy,
        backend,
        max_dynamic_streams,
        max_phase_transitions,
        timeout_minutes,
    )
    .await
}

/// The single execution loop for all research topologies.
///
/// Converts any strategy topology into initial streams, then runs a unified
/// monitoring and moderator loop. The moderator (JSON actions) drives all
/// subsequent topology evolution: adding streams, transitioning phases,
/// injecting cross-stream context, and concluding.
async fn execute_generic(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    strategy: &ResearchStrategy,
    backend: &AgentBackend,
    max_dynamic_streams: Option<usize>,
    max_phase_transitions: usize,
    timeout_minutes: Option<u64>,
) -> Result<String> {
    let initial = extract_initial_streams(strategy);
    let agent_mode = &strategy.execution.agent_mode;

    let mut active_streams: Vec<StreamSpec> = Vec::new();
    let mut all_stream_names: Vec<String> = Vec::new();
    let mut stream_agents: std::collections::HashMap<String, Vec<tokio::task::JoinHandle<()>>> =
        std::collections::HashMap::new();
    let mut stream_monitors: std::collections::HashMap<String, tokio::task::JoinHandle<()>> =
        std::collections::HashMap::new();
    let child_groups: std::sync::Arc<std::sync::Mutex<Vec<i32>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel::<(String, String, String)>();

    print_phase("RESEARCH", &format!("{} initial streams ({})", initial.len(), strategy.name));

    for spec in &initial {
        start_stream(
            streams, swarm_id, spec, agent_mode, backend,
            strategy.execution.max_messages_per_agent,
            &child_groups, &msg_tx,
            &mut active_streams, &mut all_stream_names,
            &mut stream_agents, &mut stream_monitors,
        ).await?;
    }

    let mut recent_activity: Vec<String> = Vec::new();
    let mut total_messages = 0usize;
    let mut last_moderator_check = std::time::Instant::now();
    let mut group_summaries: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let max_total = strategy.execution.max_messages_per_agent
        * active_streams.iter().map(|s| s.agents).sum::<usize>().max(1);

    let mut interrupted = false;
    let mut shutdown_signal = std::pin::pin!(wait_for_shutdown_signal());
    let mut dynamic_streams_added = 0usize;
    let mut wrap_up_started: Option<(usize, std::time::Instant)> = None;
    let mut phase_transitions = 0usize;
    let started_at = std::time::Instant::now();
    let timeout = timeout_minutes.map(|m| std::time::Duration::from_secs(m.saturating_mul(60)));
    let mut timeout_tick = tokio::time::interval(tokio::time::Duration::from_secs(2));

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => { interrupted = true; break; },
            _ = timeout_tick.tick() => {
                if let Some(limit) = timeout {
                    if started_at.elapsed() >= limit {
                        let ds = format!("swarm/{}/moderator/decisions", swarm_id.0);
                        let _ = crate::chat::append_to_stream(
                            streams,
                            &ds,
                            "System",
                            &format!(
                                "Wall-clock timeout reached ({} min); forcing conclude.",
                                timeout_minutes.unwrap_or_default()
                            ),
                        ).await;
                        println!("\n  {} Wall-clock timeout reached; concluding", ">>".bright_yellow().bold());
                        break;
                    }
                }
            }
            item = msg_rx.recv() => {
                let Some((label, author, text)) = item else { break };

                let active_labels: Vec<String> = active_streams.iter().map(|s| s.name.clone()).collect();
                print_research_message(&label, &author, &text, &active_labels);
                recent_activity.push(format!("[{}] {}: {}", label, author, text));
                total_messages += 1;

                if total_messages % 20 == 0 {
                    for spec in &active_streams {
                        let sn = format!("swarm/{}/{}", swarm_id.0, spec.name);
                        if let Ok(msgs) = crate::chat::read_stream_messages(streams, &sn).await {
                            let findings: Vec<_> = msgs.iter()
                                .filter(|(a, _)| a != "System" && a != "Moderator")
                                .map(|(a, t)| format!("{}: {}", a, t))
                                .collect();
                            if !findings.is_empty() {
                                if let Ok(s) = backend.prompt(
                                    "Summarize research findings concisely in 2-3 sentences.",
                                    &findings.join("\n"),
                                ).await {
                                    group_summaries.insert(spec.name.clone(), s);
                                }
                            }
                        }
                    }
                }

                if recent_activity.len() >= 5 || last_moderator_check.elapsed().as_secs() >= 15 {
                    if !recent_activity.is_empty() {
                        let activity = recent_activity.join("\n");
                        recent_activity.clear();
                        last_moderator_check = std::time::Instant::now();

                        let summaries_text = group_summaries.iter()
                            .map(|(n, s)| format!("  {}: {}", n, s))
                            .collect::<Vec<_>>().join("\n");

                        let moderator_prompt = build_moderator_prompt(
                            strategy, &active_streams, &activity,
                            &summaries_text, total_messages, max_total,
                        );

                        if let Ok(decision) = backend.prompt(
                            "You are an autonomous research moderator. Output a single JSON action.",
                            &moderator_prompt,
                        ).await {
                            let ds = format!("swarm/{}/moderator/decisions", swarm_id.0);
                            let _ = crate::chat::append_to_stream(
                                streams, &ds, "Moderator",
                                &format!("[T+{}] {}", total_messages, decision),
                            ).await;
                            match parse_moderator_action(&decision) {
                                ModeratorAction::AddStream { name, prompt, agents, reads_from, agent } => {
                                    if wrap_up_started.is_some() {
                                        let _ = crate::chat::append_to_stream(
                                            streams,
                                            &ds,
                                            "System",
                                            "Ignoring add_stream: wrap-up already started; converging to synthesis.",
                                        ).await;
                                        continue;
                                    }
                                    if max_dynamic_streams.is_some_and(|limit| dynamic_streams_added >= limit) {
                                        let _ = crate::chat::append_to_stream(
                                            streams,
                                            &ds,
                                            "System",
                                            "Ignoring add_stream: dynamic stream cap reached; converge with existing streams.",
                                        ).await;
                                        continue;
                                    }
                                    let Some(name) = normalize_stream_name(&name) else {
                                        let _ = crate::chat::append_to_stream(
                                            streams,
                                            &ds,
                                            "System",
                                            "Ignoring add_stream: invalid or reserved stream name.",
                                        ).await;
                                        continue;
                                    };
                                    println!("\n  {} Adding stream: {} ({})", ">>".bright_magenta().bold(), name, agent.as_deref().unwrap_or("default"));
                                    let context = collect_stream_context(streams, swarm_id, &reads_from).await;
                                    let full_prompt = if context.is_empty() {
                                        prompt
                                    } else {
                                        format!("{}\n\n=== CONTEXT FROM RELATED STREAMS ===\n{}", prompt, truncate_display(&context, 3000))
                                    };
                                    let spec = StreamSpec { name, prompt: full_prompt, agents, agent };
                                    start_stream(
                                        streams, swarm_id, &spec, agent_mode, backend,
                                        strategy.execution.max_messages_per_agent,
                                        &child_groups, &msg_tx,
                                        &mut active_streams, &mut all_stream_names,
                                        &mut stream_agents, &mut stream_monitors,
                                    ).await?;
                                    dynamic_streams_added += 1;
                                }
                                ModeratorAction::Steer { target, message } => {
                                    println!("\n  {} Steering {}: {}", ">>".bright_magenta().bold(), target, truncate_display(&message, 60));
                                    let targets: Vec<String> = if target == "all" {
                                        active_streams.iter().map(|s| s.name.clone()).collect()
                                    } else {
                                        vec![target]
                                    };
                                    for t in targets {
                                        let sn = format!("swarm/{}/{}", swarm_id.0, t);
                                        let _ = crate::chat::append_to_stream(streams, &sn, "Moderator", &message).await;
                                    }
                                }
                                ModeratorAction::StartPhase { context_from, streams: new_specs } => {
                                    if phase_transitions >= max_phase_transitions {
                                        let _ = crate::chat::append_to_stream(
                                            streams,
                                            &ds,
                                            "System",
                                            "Max phase transitions reached; forcing conclude.",
                                        ).await;
                                        println!("\n  {} Max phase transitions reached; concluding", ">>".bright_magenta().bold());
                                        break;
                                    }
                                    println!("\n  {} Phase transition → {} new streams", ">>".bright_magenta().bold(), new_specs.len());
                                    // Collect context BEFORE killing old streams
                                    let sources: Vec<String> = if context_from.is_empty() {
                                        active_streams.iter().map(|s| s.name.clone()).collect()
                                    } else {
                                        context_from
                                    };
                                    let context = collect_stream_context(streams, swarm_id, &sources).await;
                                    // Kill all current streams — agents, monitors, active list
                                    let old_names: Vec<String> = active_streams.iter().map(|s| s.name.clone()).collect();
                                    for name in &old_names {
                                        stop_stream(name, &mut active_streams, &mut stream_agents, &mut stream_monitors);
                                    }
                                    // Start new phase with prior context injected
                                    for mut spec in new_specs {
                                        if !context.is_empty() {
                                            spec.prompt = format!(
                                                "{}\n\n=== PREVIOUS PHASE ===\n{}",
                                                spec.prompt,
                                                truncate_display(&context, 4000),
                                            );
                                        }
                                        start_stream(
                                            streams, swarm_id, &spec, agent_mode, backend,
                                            strategy.execution.max_messages_per_agent,
                                            &child_groups, &msg_tx,
                                            &mut active_streams, &mut all_stream_names,
                                            &mut stream_agents, &mut stream_monitors,
                                        ).await?;
                                    }
                                    phase_transitions += 1;
                                }
                                ModeratorAction::StopStream { target } => {
                                    println!("\n  {} Stopping stream: {}", ">>".bright_magenta().bold(), target);
                                    stop_stream(&target, &mut active_streams, &mut stream_agents, &mut stream_monitors);
                                }
                                ModeratorAction::WrapUp { target } => {
                                    println!("\n  {} Wrapping up", ">>".bright_magenta().bold());
                                    if wrap_up_started.is_none() {
                                        wrap_up_started = Some((total_messages, std::time::Instant::now()));
                                    }
                                    let targets: Vec<String> = target
                                        .map(|t| vec![t])
                                        .unwrap_or_else(|| active_streams.iter().map(|s| s.name.clone()).collect());
                                    for t in targets {
                                        let sn = format!("swarm/{}/{}", swarm_id.0, t);
                                        let _ = crate::chat::append_to_stream(
                                            streams, &sn, "System",
                                            "WRAP UP: Please post your final summary.",
                                        ).await;
                                    }
                                }
                                ModeratorAction::Conclude => {
                                    println!("\n  {} Research concluded by moderator", ">>".bright_magenta().bold());
                                    break;
                                }
                                ModeratorAction::Continue => {}
                            }
                        }
                    }
                }

                if total_messages >= max_total {
                    println!("\n  {} Budget exhausted ({} messages)", ">>".bright_yellow(), max_total);
                    break;
                }
            }
        }
    }

    // Cleanup: abort all tasks and kill child processes immediately
    if interrupted {
        println!("\n\n  {} Interrupted — cleaning up...", "■".bright_red());
    }

    for (_, h) in &stream_monitors { h.abort(); }
    for (_, handles) in &stream_agents {
        for h in handles { h.abort(); }
    }
    drop(msg_tx);
    #[cfg(unix)]
    {
        let mut groups = child_groups.lock().unwrap().clone();
        groups.sort_unstable();
        groups.dedup();
        for pgid in groups {
            if pgid <= 0 {
                continue;
            }
            // Negative pid targets the entire process group.
            unsafe {
                let _ = libc::kill(-pgid, libc::SIGTERM);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
        let mut groups = child_groups.lock().unwrap().clone();
        groups.sort_unstable();
        groups.dedup();
        for pgid in groups {
            if pgid <= 0 {
                continue;
            }
            unsafe {
                let _ = libc::kill(-pgid, libc::SIGKILL);
            }
        }
    }

    if interrupted {
        println!("  {} Agent tasks stopped.", "■".bright_red());
        return Ok("(research interrupted by user)".to_string());
    }

    print_phase("SYNTHESIZE", "producing final report");

    synthesize_research(
        streams, swarm_id, &strategy.synthesis, &strategy.question, backend, &all_stream_names,
    ).await
}


async fn synthesize_research(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    config: &SynthesisConfig,
    question: &str,
    backend: &AgentBackend,
    // Streams created during execution — used to resolve glob patterns in config.input_streams.
    // S2 has no stream listing API, so callers pass what they know was created.
    known_streams: &[String],
) -> Result<String> {
    let mut all_findings = String::new();
    let swarm_prefix = format!("swarm/{}/", swarm_id.0);

    for stream_pattern in &config.input_streams {
        if stream_pattern.contains('*') {
            // Resolve glob against known_streams (e.g. "group/*" matches group/Foo, group/Bar)
            let prefix = stream_pattern.replace("/*", "/").replace('*', "");
            for s in known_streams.iter().filter(|s| {
                let rel = s.strip_prefix(&swarm_prefix).unwrap_or(s);
                rel.starts_with(&prefix)
            }) {
                if let Ok(msgs) = crate::chat::read_stream_messages(streams, s).await {
                    let label = s.strip_prefix(&swarm_prefix).unwrap_or(s);
                    all_findings.push_str(&format!("\n## {}\n", label));
                    for (author, text) in &msgs {
                        if author != "System" {
                            all_findings.push_str(&format!("- {}: {}\n", author, text));
                        }
                    }
                }
            }
        } else {
            // Exact name — normalize to full swarm path
            let full_name = if stream_pattern.starts_with("swarm/") {
                stream_pattern.replace("{swarm_id}", &swarm_id.0)
            } else {
                format!("{}{}", swarm_prefix, stream_pattern)
            };
            if let Ok(msgs) = crate::chat::read_stream_messages(streams, &full_name).await {
                let label = full_name.strip_prefix(&swarm_prefix).unwrap_or(&full_name);
                all_findings.push_str(&format!("\n## {}\n", label));
                for (author, text) in &msgs {
                    if author != "System" {
                        all_findings.push_str(&format!("- {}: {}\n", author, text));
                    }
                }
            }
        }
    }

    // Fallback: if the synthesis config produced nothing, read all known streams directly
    if all_findings.is_empty() {
        for stream_name in known_streams {
            if let Ok(msgs) = crate::chat::read_stream_messages(streams, stream_name).await {
                let label = stream_name.strip_prefix(&swarm_prefix).unwrap_or(stream_name);
                all_findings.push_str(&format!("\n## {}\n", label));
                for (author, text) in &msgs {
                    if author != "System" {
                        all_findings.push_str(&format!("- {}: {}\n", author, text));
                    }
                }
            }
        }
    }

    truncate_findings(&mut all_findings, 50_000);

    let prompt = config.prompt_template
        .replace("{question}", question)
        .replace("{findings}", &all_findings);

    Ok(backend.prompt("You are a research moderator.", &prompt).await
        .unwrap_or_else(|e| format!("(synthesis failed: {e})")))
}

// ───────────────────────────────────────────────────────────────────
// Entry point
// ───────────────────────────────────────────────────────────────────

fn apply_group_runtime_controls(
    group: &mut GroupDef,
    agents_per_group: usize,
    default_agent: &str,
    agent_mode: ResearchAgentMode,
) {
    if group.agents > agents_per_group {
        group.agents = agents_per_group;
    }

    match agent_mode {
        ResearchAgentMode::Fixed => {
            group.agent = Some(default_agent.to_string());
        }
        ResearchAgentMode::Planner => {
            // Keep planner-selected backend when present, otherwise fall back to --agent.
            let needs_fallback = group
                .agent
                .as_ref()
                .map(|s| s.trim().is_empty())
                .unwrap_or(true);
            if needs_fallback {
                group.agent = Some(default_agent.to_string());
            }
        }
    }
}

pub async fn start_research(
    question: &str,
    hint: Option<&str>,
    num_groups: usize,
    agents_per_group: usize,
    max_messages: usize,
    max_dynamic_streams: Option<usize>,
    max_phase_transitions: usize,
    timeout_minutes: Option<u64>,
    agent_type: &str,
    planner_agent_type: Option<&str>,
    agent_mode: ResearchAgentMode,
    role_diversity: bool,
    model_override: Option<&str>,
    config: &Config,
    basin_override: Option<&str>,
) -> Result<()> {
    let streams = streams::connect(config, basin_override)?;
    let swarm_id = RunId::generate();
    let model = model_override.unwrap_or(&config.anthropic.agent_model);
    let backend = AgentBackend::from_str(agent_type, model);

    // Plan the research dynamically - AI designs strategy on the fly
    let planner_backend_name = planner_agent_type.unwrap_or("claude");
    let planner_backend = AgentBackend::from_str(planner_backend_name, &config.anthropic.model);
    let planner = crate::planner::Planner::new(planner_backend.clone());

    println!(
        "\n{} Designing research strategy... ({})",
        "⚙".dimmed(),
        planner_backend.name().bold()
    );

    // Design completely custom strategy based on question
    let mut strategy = planner
        .design_research_strategy(
            question,
            hint,
            num_groups,
            agents_per_group,
            max_messages,
            role_diversity,
        )
        .await?;

    println!("  {} Strategy: {}", "+".bright_green(), strategy.name.bold());

    // Clamp to what the user requested — the planner sometimes ignores constraints
    match &mut strategy.topology {
        StreamTopology::Groups { groups } => {
            if groups.len() > num_groups {
                groups.truncate(num_groups);
            }
            for g in groups.iter_mut() {
                apply_group_runtime_controls(g, agents_per_group, agent_type, agent_mode);
            }
            strategy.execution.agent_count = groups.iter().map(|g| g.agents).sum();
        }
        StreamTopology::Rounds(RoundsConfig::Simple { instances_per_round, .. }) => {
            if *instances_per_round > num_groups {
                *instances_per_round = num_groups;
            }
            strategy.execution.agent_count = *instances_per_round;
        }
        StreamTopology::Rounds(RoundsConfig::Complex { rounds }) => {
            for r in rounds.iter_mut() {
                if r.groups.len() > num_groups {
                    r.groups.truncate(num_groups);
                }
                for g in r.groups.iter_mut() {
                    apply_group_runtime_controls(g, agents_per_group, agent_type, agent_mode);
                }
            }
            strategy.execution.agent_count = rounds.first()
                .map(|r| r.groups.iter().map(|g| g.agents).sum())
                .unwrap_or(1);
        }
        StreamTopology::Hierarchical { root_groups, .. } => {
            if root_groups.len() > num_groups {
                root_groups.truncate(num_groups);
            }
            for g in root_groups.iter_mut() {
                apply_group_runtime_controls(g, agents_per_group, agent_type, agent_mode);
            }
            strategy.execution.agent_count = root_groups.iter().map(|g| g.agents).sum();
        }
        StreamTopology::Custom { .. } => {}
    }
    strategy.execution.max_messages_per_agent = max_messages;

    crate::validation::validate_strategy(&strategy)?;

    // Show resource estimates
    let estimate = crate::validation::estimate_strategy_cost(&strategy);
    println!(
        "  {} Resources: {} agents, ~{} messages, ~{} min",
        "ℹ".dimmed(),
        estimate.total_agents,
        estimate.max_messages,
        estimate.estimated_duration_minutes
    );
    println!(
        "  {} Estimated API calls: ~{}",
        "ℹ".dimmed(),
        estimate.estimated_api_calls
    );

    // Apply hint to synthesis
    if let Some(h) = hint {
        strategy.synthesis.prompt_template = format!("USER HINT: {}\n\n{}", h, strategy.synthesis.prompt_template);
    }

    // Publish to S2 for join support (extract groups from topology)
    let strategy_groups: Vec<ResearchGroup> = match &strategy.topology {
        StreamTopology::Groups { groups } => groups.iter().map(|g| ResearchGroup {
            name: g.name.clone(),
            prompt: g.prompt.clone(),
            agents: g.agents,
            agent: None,
        }).collect(),
        _ => vec![],
    };

    let swarm_strategy = Strategy {
        swarm_id: swarm_id.0.clone(),
        goal: question.to_string(),
        strategy_type: StrategyType::Research {
            groups: strategy_groups,
            moderator_prompt: strategy.synthesis.prompt_template.clone(),
            technique: Some(strategy.name.clone()),
            rounds: match &strategy.topology {
                StreamTopology::Rounds(RoundsConfig::Simple { rounds, .. }) => Some(*rounds),
                StreamTopology::Rounds(RoundsConfig::Complex { rounds }) => Some(rounds.len()),
                _ => Some(1),
            },
        },
    };
    streams.publish_strategy(&swarm_id, &swarm_strategy).await?;

    // Emit RunStarted
    let _ = streams
        .emit_event(&Event::run_started(&swarm_id, question, strategy.execution.agent_count))
        .await;

    // Display header
    print_strategy_header(&strategy, &swarm_id);
    print_topology_diagram(&strategy);

    // Execute via generic engine
    let report = execute_strategy(
        &streams,
        &swarm_id,
        &strategy,
        &backend,
        max_dynamic_streams,
        max_phase_transitions,
        timeout_minutes,
    )
    .await?;

    // Display report
    print_strategy_report(&strategy, &report);

    // Save report to S2
    let synthesis_stream = format!("swarm/{}/synthesis", swarm_id.0);
    crate::chat::append_to_stream(&streams, &synthesis_stream, "Moderator", &report).await?;

    let _ = streams
        .emit_event(&Event::run_completed(&swarm_id))
        .await;

    println!(
        "\n{} Research {} complete!",
        "+".bright_green(),
        swarm_id.short()
    );

    Ok(())
}

// ───────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────

pub async fn run_single_research_agent(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    group_name: &str,
    group_prompt: &str,
    max_turns: usize,
    model: &str,
) -> Result<()> {
    let stream_name = format!("swarm/{}/group/{}", swarm_id.0, group_name);
    let agent_name = format!(
        "{}-{}",
        group_name,
        &uuid::Uuid::new_v4().to_string()[..8]
    );
    crate::chat::run_persistent_chat_agent(
        streams,
        &stream_name,
        &agent_name,
        group_prompt,
        &[],
        max_turns,
        model,
    )
    .await
}
