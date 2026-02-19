use colored::Colorize;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::error::{OrchestratorError, Result};
use crate::streams::{self, OrchestratorStreams};
use crate::types::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Strategy {
    pub swarm_id: String,
    pub goal: String,
    #[serde(rename = "type")]
    pub strategy_type: StrategyType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub enum StrategyType {
    /// Build/code tasks with DAG dependencies
    #[serde(rename = "tasks")]
    Tasks { tasks: Vec<TaskDef> },

    /// Group research with moderator synthesis
    #[serde(rename = "research")]
    Research {
        groups: Vec<ResearchGroup>,
        moderator_prompt: String,
        #[serde(default)]
        technique: Option<String>,
        #[serde(default)]
        rounds: Option<usize>,
    },

    /// Multi-agent conversation
    #[serde(rename = "chat")]
    Chat {
        topic: String,
        personas: Vec<Persona>,
    },

    /// Open-ended investigation (debugging, exploration)
    #[serde(rename = "investigate")]
    Investigate { focus: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchGroup {
    pub name: String,
    pub prompt: String,
    pub agents: usize,
    /// Agent backend override (e.g. "claude", "codex"). None = use swarm default.
    #[serde(default)]
    pub agent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Persona {
    pub name: String,
    pub stance: String,
    /// Agent backend override (e.g. "claude", "codex"). None = use swarm default.
    #[serde(default)]
    pub agent: Option<String>,
}

pub async fn join(
    swarm_id_str: &str,
    agent_type: &str,
    max_turns: usize,
    working_dir: Option<&std::path::Path>,
    join_context: Option<&str>,
    group: Option<&str>,
    config: &Config,
    basin_override: Option<&str>,
) -> Result<()> {
    let backend = crate::agent::AgentBackend::from_str(agent_type, &config.anthropic.agent_model);
    let agent_name = format!("{}-{}", backend.name(), hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".into()));
    let streams = streams::connect(config, basin_override)?;
    let swarm_id = RunId(swarm_id_str.to_string());

    let working_dir_str = working_dir
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| ".".into());

    let strategy = streams
        .read_strategy(&swarm_id)
        .await?
        .ok_or_else(|| OrchestratorError::Worker(format!("Swarm {} not found", swarm_id_str)))?;

    println!(
        "{} Joining swarm {} — \"{}\"",
        "▸".bright_blue(),
        swarm_id.short(),
        strategy.goal
    );
    display_strategy(&strategy);

    match &strategy.strategy_type {
        StrategyType::Tasks { .. } => {
            return Err(OrchestratorError::Worker(
                "Task mode is not supported in join command. The worker module has been removed.".to_string()
            ));
        }
        StrategyType::Chat { topic, personas, .. } => {
            let effective_topic = match join_context {
                Some(ctx) => format!("{topic}. My context: {ctx}"),
                None => topic.clone(),
            };
            println!(
                "\n{} Joining chat as {} ({})\n",
                "▸".bright_blue(),
                agent_name,
                agent_type
            );
            let stream_name = format!("swarm/{}/chat", swarm_id.0);

            if matches!(&backend, crate::agent::AgentBackend::Claude { .. }) {
                let s = streams.clone();
                let sn = stream_name.clone();
                let an = agent_name.clone();
                let et = effective_topic.clone();
                let peer_names: Vec<String> = personas.iter().map(|p| p.name.clone()).collect();
                let mt = max_turns;
                let mdl = backend.model().to_string();
                let agent_handle = tokio::spawn(async move {
                    let _ = crate::chat::run_persistent_chat_agent(
                        &s, &sn, &an, &et, &peer_names, mt, &mdl,
                    ).await;
                });

                let s2_stream = streams.stream(&stream_name)?;
                let mon_input = s2_sdk::types::ReadInput::new()
                    .with_start(s2_sdk::types::ReadStart::new()
                        .with_from(s2_sdk::types::ReadFrom::SeqNum(0)));
                let mut session = s2_stream.read_session(mon_input).await?;

                loop {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => break,
                        batch = session.next() => {
                            let Some(batch) = batch else { break };
                            if let Ok(batch) = batch {
                                for record in &batch.records {
                                    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&record.body) {
                                        let author = msg["author"].as_str().unwrap_or("?");
                                        let text = msg["message"].as_str().unwrap_or("");
                                        let to = msg["to"].as_str();
                                        if author != "System" && !text.is_empty() {
                                            if let Some(recipient) = to {
                                                println!(
                                                    "  {} {} {}",
                                                    format!("{}→{}:", author, recipient).bright_cyan().bold(),
                                                    text,
                                                    "(DM)".dimmed()
                                                );
                                            } else {
                                                println!("  {} {}", format!("{}:", author).bright_cyan().bold(), text);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                agent_handle.abort();
                // Wait for abort to complete to avoid orphaned processes
                let _ = tokio::time::timeout(
                    tokio::time::Duration::from_millis(500),
                    agent_handle
                ).await;
            } else {
                run_generic_agent_on_stream(
                    &streams,
                    &stream_name,
                    &agent_name,
                    &effective_topic,
                    backend.clone(),
                    max_turns,
                )
                .await?;
            }
        }
        StrategyType::Research { groups, technique, .. } => {
            if technique.as_deref() == Some("delphi") {
                return Err(OrchestratorError::Research(
                    "Delphi mode uses one-shot rounds and does not support join. \
                     Use `parallax research` to start a new Delphi session instead."
                        .into(),
                ));
            }

            let target_group = if let Some(gname) = group {
                groups.iter().find(|g| g.name == gname)
                    .ok_or_else(|| OrchestratorError::Research(format!(
                        "Group '{}' not found. Available: {}",
                        gname,
                        groups.iter().map(|g| g.name.as_str()).collect::<Vec<_>>().join(", ")
                    )))?
            } else {
                println!("\n{} Requesting group assignment from moderator...", "◆".bright_cyan().bold());

                let join_request_stream = format!("swarm/{}/join-requests", swarm_id.0);
                let context_desc = join_context.unwrap_or("general research");
                let request = format!(
                    "New agent joining: {}\nContext: {}\nPlease assign to a group.",
                    agent_name,
                    context_desc
                );
                crate::chat::append_to_stream(&streams, &join_request_stream, &agent_name, &request).await?;

                println!("  {} Waiting for moderator assignment (10s timeout)...", "→".dimmed());

                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

                let assignment_stream = format!("swarm/{}/join-assignments", swarm_id.0);
                let assigned_group = if let Ok(msgs) = crate::chat::read_stream_messages(&streams, &assignment_stream).await {
                    msgs.iter()
                        .filter(|(author, _)| author == "Moderator")
                        .last()
                        .and_then(|(_, text)| {
                            // Parse "ASSIGN: agent=X, group=Y"
                            text.find("group=").and_then(|start| {
                                let after = &text[start + 6..];
                                let end = after.find(&[',', '\n', ' '][..]).unwrap_or(after.len());
                                Some(after[..end].trim().to_string())
                            })
                        })
                } else {
                    None
                };

                if let Some(assigned) = assigned_group {
                    println!("  {} Moderator assigned you to: {}", "✓".bright_green(), assigned.bold());
                    groups.iter().find(|g| g.name == assigned).ok_or_else(|| {
                        OrchestratorError::Research(format!("Moderator assigned unknown group: {}", assigned))
                    })?
                } else {
                    println!("  {} Moderator unavailable, auto-assigning to least active group", "→".dimmed());

                    let mut group_activity: Vec<_> = Vec::new();
                    for g in groups {
                        let stream_name = format!("swarm/{}/group/{}", swarm_id.0, g.name);
                        let msg_count = streams.stream(&stream_name)?
                            .check_tail()
                            .await
                            .map(|pos| pos.seq_num)
                            .unwrap_or(0);
                        group_activity.push((g, msg_count));
                    }

                    group_activity.sort_by_key(|(_, count)| *count);
                    let assigned = group_activity.first().map(|(g, _)| *g)
                        .ok_or_else(|| OrchestratorError::Research("No groups available".into()))?;

                    println!("  {} Assigned to: {}", "✓".bright_green(), assigned.name.bold());
                    assigned
                }
            };
            {
                let group = target_group;
                // Use the group's agent override if the user didn't explicitly set --agent
                let backend = if agent_type == "claude" {
                    if let Some(ref agent_override) = group.agent {
                        crate::agent::AgentBackend::from_str(agent_override, &config.anthropic.agent_model)
                    } else {
                        backend.clone()
                    }
                } else {
                    backend.clone()
                };
                let gname = group.name.clone();
                let gprompt = match join_context {
                    Some(ctx) => format!("{}. My context: {}", group.prompt, ctx),
                    None => group.prompt.clone(),
                };
                let stream_name = format!("swarm/{}/group/{}", swarm_id.0, gname);

                println!(
                    "\n{} Joining research group '{}'\n",
                    "▸".bright_blue(),
                    gname
                );

                if agent_type == "human" {
                    println!("\n{} You are now in research group '{}'", "👤".bright_green(), gname.bright_cyan().bold());
                    println!("  {} {}", "Role:".dimmed(), gprompt.dimmed());
                    println!("  {} Type your messages and press Enter. Ctrl+C to leave.\n", "Tip:".dimmed());

                    run_human_research_agent(&streams, &swarm_id, &gname, &agent_name).await?;
                    return Ok(());
                } else if matches!(&backend, crate::agent::AgentBackend::Claude { .. }) {
                    let s = streams.clone();
                    let sid = swarm_id.clone();
                    let mt = max_turns;
                    let model = backend.model().to_string();
                    let agent_handle = tokio::spawn(async move {
                        let _ = crate::research::run_single_research_agent(&s, &sid, &gname, &gprompt, mt, &model).await;
                    });

                    let s2_stream = streams.stream(&stream_name)?;
                    let mon_input = s2_sdk::types::ReadInput::new()
                        .with_start(s2_sdk::types::ReadStart::new()
                            .with_from(s2_sdk::types::ReadFrom::SeqNum(0)));
                    let mut session = s2_stream.read_session(mon_input).await?;

                    loop {
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => break,
                            batch = session.next() => {
                                let Some(batch) = batch else { break };
                                if let Ok(batch) = batch {
                                    for record in &batch.records {
                                        if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&record.body) {
                                            let author = msg["author"].as_str().unwrap_or("?");
                                            let text = msg["message"].as_str().unwrap_or("");
                                            if author != "System" {
                                                println!("  {} {}", format!("{}:", author).bright_cyan().bold(), text);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    agent_handle.abort();
                // Wait for abort to complete to avoid orphaned processes
                let _ = tokio::time::timeout(
                    tokio::time::Duration::from_millis(500),
                    agent_handle
                ).await;
                } else {
                    run_generic_agent_on_stream(
                        &streams,
                        &stream_name,
                        &agent_name,
                        &gprompt,
                        backend.clone(),
                        max_turns,
                    )
                    .await?;
                }
            }
        }
        StrategyType::Investigate { focus } => {
            let effective_focus = match join_context {
                Some(ctx) => format!("{focus}. My context: {ctx}"),
                None => focus.clone(),
            };
            if matches!(&backend, crate::agent::AgentBackend::Claude { .. }) {
                run_single_investigate_agent(&streams, &swarm_id, &effective_focus, &working_dir_str, max_turns, backend.model())
                    .await?;
            } else {
                let topic = format!("{} (working in {})", effective_focus, &working_dir_str);
                run_generic_agent_on_stream(
                    &streams,
                    &format!("swarm/{}/findings", swarm_id.0),
                    &agent_name,
                    &topic,
                    backend.clone(),
                    max_turns,
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn run_human_research_agent(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    group_name: &str,
    agent_name: &str,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let stream_name = format!("swarm/{}/group/{}", swarm_id.0, group_name);
    let s2_stream = streams.stream(&stream_name)?;

    let input = s2_sdk::types::ReadInput::new()
        .with_start(s2_sdk::types::ReadStart::new().with_from(s2_sdk::types::ReadFrom::SeqNum(0)));
    let mut session = s2_stream.read_session(input).await?;

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    let mut last_seen_seq = 0u64;

    loop {
        tokio::select! {
            batch = session.next() => {
                let Some(batch) = batch else { break };
                if let Ok(batch) = batch {
                    for record in &batch.records {
                        if record.seq_num <= last_seen_seq {
                            continue;
                        }
                        last_seen_seq = record.seq_num;

                        if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&record.body) {
                            let author = msg["author"].as_str().unwrap_or("?");
                            let text = msg["message"].as_str().unwrap_or("");

                            if author != agent_name && author != "System" {
                                println!("  {} {}", format!("{}:", author).bright_cyan().bold(), text);
                            } else if author == "System" || author == "Moderator" {
                                println!("  {} {}", format!("{}:", author).bright_yellow(), text);
                            }
                        }
                    }
                }
            }

            line = lines.next_line() => {
                match line {
                    Ok(Some(input)) => {
                        if !input.trim().is_empty() {
                            crate::chat::append_to_stream(streams, &stream_name, agent_name, input.trim())
                                .await?;
                        }
                    }
                    Ok(None) | Err(_) => break,
                }
            }

            _ = tokio::signal::ctrl_c() => {
                println!("\n{} Left the research group.", "✓".bright_green());
                break;
            }
        }
    }

    Ok(())
}

async fn run_single_investigate_agent(
    streams: &OrchestratorStreams,
    swarm_id: &RunId,
    focus: &str,
    working_dir: &str,
    max_turns: usize,
    model: &str,
) -> Result<()> {
    let stream_name = format!("swarm/{}/findings", swarm_id.0);
    let agent_name = format!(
        "agent-{}-{}",
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".into()),
        uuid::Uuid::new_v4().to_string()[..4].to_string()
    );
    crate::chat::run_persistent_chat_agent(
        streams,
        &stream_name,
        &agent_name,
        &format!("{} (working in {})", focus, working_dir),
        &[],
        max_turns,
        model,
    )
    .await
}


fn display_strategy(strategy: &Strategy) {
    match &strategy.strategy_type {
        StrategyType::Tasks { tasks } => {
            println!("{}", "─".repeat(60));
            for (i, task) in tasks.iter().enumerate() {
                let deps = if task.depends_on.is_empty() {
                    String::new()
                } else {
                    format!(" (depends on: {})", task.depends_on.join(", "))
                };
                println!(
                    "  {} {}{} {}",
                    format!("{}.", i + 1).dimmed(),
                    task.title.bold(),
                    deps.dimmed(),
                    format!("[{}]", task.id).dimmed()
                );
            }
            println!("{}", "─".repeat(60));
        }
        StrategyType::Chat { personas, .. } => {
            for p in personas {
                println!("  {} {} — {}", "•".bright_cyan(), p.name.bold(), p.stance.dimmed());
            }
        }
        StrategyType::Research { groups, .. } => {
            for g in groups {
                println!(
                    "  {} {} ({} agents) — {}",
                    "◆".bright_yellow(),
                    g.name.bold(),
                    g.agents,
                    g.prompt.dimmed()
                );
            }
        }
        StrategyType::Investigate { focus } => {
            println!("  {} {}", "🔍".dimmed(), focus);
        }
    }
}

async fn run_generic_agent_on_stream(
    streams: &OrchestratorStreams,
    stream_name: &str,
    agent_name: &str,
    topic: &str,
    backend: crate::agent::AgentBackend,
    max_messages: usize,
) -> Result<()> {
    let system_prompt = format!(
        "You are {} in a collaborative session about: {}. \
         Keep responses concise and useful. Build on others' contributions. \
         Do not use tools or run commands; respond from your existing knowledge.",
        agent_name, topic
    );

    let intro = backend
        .prompt(
            &system_prompt,
            &format!("Introduce yourself and share your initial thoughts on: {}", topic),
        )
        .await?;

    if !intro.is_empty() {
        crate::chat::append_to_stream(streams, stream_name, agent_name, &intro).await?;
    }

    let s2_stream = streams.stream(stream_name)?;

    let mut total_messages = 0usize;

    let session_input = s2_sdk::types::ReadInput::new()
        .with_start(s2_sdk::types::ReadStart::new()
            .with_from(s2_sdk::types::ReadFrom::SeqNum(0)));
    let mut session = s2_stream.read_session(session_input).await?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            batch = session.next() => {
                let Some(batch) = batch else { break };
                let batch = match batch {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                let mut new_messages = Vec::new();
                for record in &batch.records {
                    if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&record.body) {
                        let author = msg["author"].as_str().unwrap_or("?");
                        let text = msg["message"].as_str().unwrap_or("");

                        if author != "System" {
                            println!("  {} {}", format!("{}:", author).bright_cyan().bold(), text);
                            total_messages += 1;
                        }

                        if author != agent_name && author != "System" {
                            new_messages.push(format!("{}: {}", author, text));
                        }
                    }
                }

                if total_messages >= max_messages { break; }

                if !new_messages.is_empty() {
                    let context = new_messages.join("\n");
                    let response = backend
                        .prompt(
                            &system_prompt,
                            &format!("New messages:\n{}\n\nRespond naturally.", context),
                        )
                        .await?;

                    if !response.is_empty() {
                        crate::chat::append_to_stream(streams, stream_name, agent_name, &response)
                            .await?;
                    }
                }
            }
        }
    }

    Ok(())
}
