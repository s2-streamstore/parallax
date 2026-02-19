use colored::Colorize;
use futures::StreamExt;
use s2_sdk::types::{ReadFrom, ReadInput, ReadStart};

use crate::config::Config;
use crate::error::{OrchestratorError, Result};
use crate::streams;
use crate::types::*;

pub async fn watch(
    config: &Config,
    basin_override: Option<&str>,
    swarm_id_filter: Option<&str>,
) -> Result<()> {
    let streams = streams::connect(config, basin_override)?;

    let run_filter = swarm_id_filter.map(|id| RunId(id.to_string()));

    println!(
        "\n{} Watching events in real-time (Ctrl+C to stop)...\n",
        "◆".bright_cyan()
    );

    let events_stream = streams
        .basin
        .stream("events".parse().map_err(|e| OrchestratorError::S2Init(format!("{e:?}")))?);

    let input = ReadInput::new()
        .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(0)));

    let mut session = events_stream.read_session(input).await?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n{} Stopped watching.", "◆".bright_cyan());
                break;
            }
            batch = session.next() => {
                let Some(batch) = batch else { break };
                let batch = match batch {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!("Watch stream error: {e}");
                        continue;
                    }
                };

                for record in &batch.records {
                    let event: Event = match serde_json::from_slice(&record.body) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    if let Some(ref rid) = run_filter {
                        if event.run_id != *rid {
                            continue;
                        }
                    }

                    let ts = chrono::DateTime::from_timestamp_millis(event.timestamp as i64)
                        .map(|dt| dt.format("%H:%M:%S").to_string())
                        .unwrap_or_default();

                    match &event.event_type {
                        EventType::RunStarted { goal, task_count } => {
                            println!(
                                "  {} {} {} — \"{}\" ({} tasks)",
                                ts.dimmed(),
                                "◆".bright_green(),
                                event.run_id.short(),
                                goal,
                                task_count
                            );
                        }
                        EventType::RunCompleted => {
                            println!(
                                "  {} {} Run {} complete!",
                                ts.dimmed(),
                                "✓".bright_green().bold(),
                                event.run_id.short()
                            );
                        }
                        EventType::RunFailed { error } => {
                            println!(
                                "  {} {} Run {} failed: {}",
                                ts.dimmed(),
                                "✗".bright_red().bold(),
                                event.run_id.short(),
                                error
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
