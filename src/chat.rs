use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use crate::error::{OrchestratorError, Result};
use crate::streams::OrchestratorStreams;

use futures::StreamExt as _;
use s2_sdk::types::{
    AppendInput, AppendRecord, AppendRecordBatch, ReadFrom, ReadInput, ReadStart, S2Error,
};

/// Parse a DM from agent output. Returns (Some(recipient), message) for DMs, (None, message) for public.
fn parse_dm(text: &str, peers: &[String]) -> (Option<String>, String) {
    if let Some(rest) = text.strip_prefix('@') {
        if let Some((name, msg)) = rest.split_once(':') {
            let name = name.trim();
            // Case-insensitive match against peer list
            if let Some(peer) = peers.iter().find(|p| p.eq_ignore_ascii_case(name)) {
                return (Some(peer.clone()), msg.trim().to_string());
            }
        }
    }
    (None, text.to_string())
}

/// Run a single persistent Claude session that participates in the chat.
async fn run_persistent_agent(
    streams: &OrchestratorStreams,
    stream_name: &str,
    _agent_idx: usize,
    name: &str,
    topic: &str,
    peers: &[String],
    max_messages: usize,
    message_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    child_pids: std::sync::Arc<std::sync::Mutex<Vec<u32>>>,
    model: &str,
) -> Result<()> {
    let s2_stream = streams.stream(stream_name)?;

    let others: Vec<&String> = peers.iter().filter(|p| p.as_str() != name).collect();
    let peer_list = if others.is_empty() {
        String::new()
    } else {
        format!(" The other participants are: {}.", others.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "))
    };

    // Spawn persistent Claude session with bidirectional streaming
    let system_prompt = format!(
        "You are {} in a group chat about \"{}\".{} \
         Address others by name. Agree, disagree, challenge, ask follow-ups. \
         You can send a private message to one person with \"@Name: message\". \
         Keep messages to 1-3 sentences. Be natural and opinionated. \
         Reply ONLY with your chat message, nothing else.",
        name, topic, peer_list
    );

    let mut args = vec![
        "-p",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
        "--dangerously-skip-permissions",
        "--system-prompt", &system_prompt,
        "--tools", "default",
    ];
    if !model.is_empty() {
        args.extend_from_slice(&["--model", model]);
    }
    let mut child = Command::new("claude")
        .args(&args)
        .env_remove("CLAUDECODE")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| OrchestratorError::Worker(format!("Failed to spawn claude for {name}: {e}")))?;

    // Register PID for cleanup on Ctrl+C
    if let Some(pid) = child.id() {
        child_pids.lock().unwrap().push(pid);
    }

    let mut stdin = child.stdin.take().expect("stdin piped");
    let stdout = child.stdout.take().expect("stdout piped");

    let last_seen_seq = 0u64;
    let mut my_messages = 0usize;
    let max_per_agent = max_messages; // each agent can post up to max — global count stops the chat

    // Send initial message to kick off the conversation
    let init_msg = make_user_message(&format!(
        "The chat topic is: \"{}\". Introduce yourself as {} and share your initial take. Just your message, nothing else.",
        topic, name
    ));
    stdin.write_all(init_msg.as_bytes()).await.ok();
    stdin.flush().await.ok();

    let s2_for_writer = streams.clone();
    let sname_for_writer = stream_name.to_string();
    let name_for_writer = name.to_string();
    let peers_for_writer: Vec<String> = peers.to_vec();
    let count_for_writer = message_count.clone();

    let writer_handle = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) {
                if event["type"].as_str() == Some("result") {
                    if let Some(result) = event["result"].as_str() {
                        let clean = result.trim();
                        if !clean.is_empty() && clean.len() > 3 {
                            // Detect @Name: prefix for DMs
                            let (to, message) = parse_dm(clean, &peers_for_writer);
                            let _ = append_message_to(
                                &s2_for_writer,
                                &sname_for_writer,
                                &name_for_writer,
                                to.as_deref(),
                                &message,
                            )
                            .await;
                            count_for_writer.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    });

    let session_input = ReadInput::new()
        .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(last_seen_seq)));
    let mut session = match s2_stream.read_session(session_input).await {
        Ok(s) => s,
        Err(_) => {
            drop(stdin);
            writer_handle.abort();
            let _ = child.kill().await;
            return Ok(());
        }
    };

    loop {
        if message_count.load(std::sync::atomic::Ordering::Relaxed) >= max_messages {
            break;
        }

        // Check if the child process has exited
        if let Ok(Some(_)) = child.try_wait() {
            tracing::warn!("Claude process exited unexpectedly for agent {}", name);
            break;
        }

        let new_messages = tokio::select! {
            batch = session.next() => {
                match batch {
                    Some(Ok(batch)) => {
                        let mut msgs = Vec::new();
                        for record in &batch.records {
                            if let Ok(msg) = serde_json::from_slice::<serde_json::Value>(&record.body) {
                                let author = msg["author"].as_str().unwrap_or("?");
                                let text = msg["message"].as_str().unwrap_or("");
                                let to = msg["to"].as_str();

                                // Skip DMs not addressed to us
                                if let Some(recipient) = to {
                                    if recipient != name {
                                        continue;
                                    }
                                }

                                if author != name {
                                    let prefix = if to.is_some() { "(DM) " } else { "" };
                                    msgs.push(format!("{}{}: {}", prefix, author, text));
                                }
                            }
                        }
                        msgs
                    }
                    Some(Err(_)) => Vec::new(),
                    None => break,
                }
            }
        };

        if new_messages.is_empty() {
            continue;
        }

        let context = new_messages.join("\n");
        let line = make_user_message(&format!(
            "New messages in the chat:\n{}\n\nRespond naturally. Just your message, nothing else.",
            context
        ));

        if stdin.write_all(line.as_bytes()).await.is_err() {
            break; // Claude process died
        }
        if stdin.flush().await.is_err() {
            break;
        }

        my_messages += 1;
        if my_messages >= max_per_agent {
            break;
        }
    }

    drop(stdin);
    writer_handle.abort();

    let _ = child.start_kill();

    let timeout_duration = tokio::time::Duration::from_secs(2);
    match tokio::time::timeout(timeout_duration, child.wait()).await {
        Ok(_) => {
            tracing::debug!("Claude process terminated gracefully");
        }
        Err(_) => {
            tracing::warn!("Claude process did not exit within timeout, forcing kill");
            let _ = child.kill().await;
        }
    }

    Ok(())
}

/// Build a stream-json user message for Claude's stdin.
fn make_user_message(text: &str) -> String {
    let msg = serde_json::json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{"type": "text", "text": text}]
        }
    });
    format!("{}\n", serde_json::to_string(&msg).unwrap())
}

async fn append_message(
    streams: &OrchestratorStreams,
    stream_name: &str,
    author: &str,
    message: &str,
) -> Result<()> {
    append_message_to(streams, stream_name, author, None, message).await
}

async fn append_message_to(
    streams: &OrchestratorStreams,
    stream_name: &str,
    author: &str,
    to: Option<&str>,
    message: &str,
) -> Result<()> {
    let stream = streams.stream(stream_name)?;
    let mut record = serde_json::json!({
        "author": author,
        "message": message,
        "timestamp": chrono::Utc::now().timestamp_millis(),
    });
    if let Some(recipient) = to {
        record["to"] = serde_json::Value::String(recipient.to_string());
    }
    let json = serde_json::to_vec(&record)?;
    let record =
        AppendRecord::new(json).map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
    let batch = AppendRecordBatch::try_from_iter([record])
        .map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
    stream.append(AppendInput::new(batch)).await?;
    Ok(())
}

pub async fn append_to_stream(
    streams: &OrchestratorStreams,
    stream_name: &str,
    author: &str,
    message: &str,
) -> Result<()> {
    append_message(streams, stream_name, author, message).await
}

/// Read all messages from a named stream (paginates through all batches).
pub async fn read_stream_messages(
    streams: &OrchestratorStreams,
    stream_name: &str,
) -> Result<Vec<(String, String)>> {
    let stream = streams.stream(stream_name)?;

    let mut msgs = Vec::new();
    let mut start_seq = 0u64;

    loop {
        let input = ReadInput::new().with_start(ReadStart::new().with_from(ReadFrom::SeqNum(start_seq)));

        match stream.read(input).await {
            Ok(batch) => {
                if batch.records.is_empty() {
                    break;
                }
                for record in batch.records {
                    start_seq = record.seq_num + 1;
                    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&record.body) {
                        let author = val["author"].as_str().unwrap_or("?").to_string();
                        let text = val["message"].as_str().unwrap_or("").to_string();
                        msgs.push((author, text));
                    }
                }
            }
            Err(S2Error::ReadUnwritten(_)) => break,
            Err(e) => return Err(e.into()),
        }
    }

    Ok(msgs)
}

/// Run a persistent chat agent on a named stream (used by all swarm modes).
pub async fn run_persistent_chat_agent(
    streams: &OrchestratorStreams,
    stream_name: &str,
    name: &str,
    topic: &str,
    peers: &[String],
    max_turns: usize,
    model: &str,
) -> Result<()> {
    run_persistent_agent(
        streams,
        stream_name,
        0,
        name,
        topic,
        peers,
        max_turns,
        std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        model,
    )
    .await
}

/// Like `run_persistent_chat_agent` but registers child PIDs into the provided kill list.
pub async fn run_persistent_chat_agent_with_kill_list(
    streams: &OrchestratorStreams,
    stream_name: &str,
    name: &str,
    topic: &str,
    peers: &[String],
    max_turns: usize,
    model: &str,
    child_pids: std::sync::Arc<std::sync::Mutex<Vec<u32>>>,
) -> Result<()> {
    run_persistent_agent(
        streams,
        stream_name,
        0,
        name,
        topic,
        peers,
        max_turns,
        std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        child_pids,
        model,
    )
    .await
}

