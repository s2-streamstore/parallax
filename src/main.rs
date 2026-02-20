mod agent;
mod chat;
mod cli;
mod code_review;
mod config;
mod error;
mod planner;
mod research;
mod status;
mod streams;
mod swarm;
mod types;
mod validation;

use clap::Parser;
use cli::{Cli, Command};
use miette::IntoDiagnostic;
use colored::Colorize;

async fn init_basin(config: &config::Config, basin_name: &str) -> error::Result<()> {
    let token = config.s2_access_token()?;
    let mut s2_config = s2_sdk::types::S2Config::new(token);

    if let (Some(account_ep), Some(basin_ep)) =
        (&config.s2.account_endpoint, &config.s2.basin_endpoint)
    {
        let endpoints = s2_sdk::types::S2Endpoints::new(
            s2_sdk::types::AccountEndpoint::new(account_ep)
                .map_err(|e| error::OrchestratorError::S2Init(e.to_string()))?,
            s2_sdk::types::BasinEndpoint::new(basin_ep)
                .map_err(|e| error::OrchestratorError::S2Init(e.to_string()))?,
        )
        .map_err(|e| error::OrchestratorError::S2Init(e.to_string()))?;
        s2_config = s2_config.with_endpoints(endpoints);
    }

    let s2 =
        s2_sdk::S2::new(s2_config).map_err(|e| error::OrchestratorError::S2Init(e.to_string()))?;

    println!("Creating basin '{}'...", basin_name);

    let basin_config = s2_sdk::types::BasinConfig::new()
        .with_create_stream_on_append(true)
        .with_create_stream_on_read(true);

    let input = s2_sdk::types::CreateBasinInput::new(
        basin_name
            .parse()
            .map_err(|e| error::OrchestratorError::S2Init(format!("{e:?}")))?,
    )
    .with_config(basin_config);

    let basin_name_parsed: s2_sdk::types::BasinName = basin_name
        .parse()
        .map_err(|e| error::OrchestratorError::S2Init(format!("{e:?}")))?;

    match s2.create_basin(input).await {
        Ok(_) => {
            println!(
                "{} Basin '{}' created!",
                "+".bright_green(),
                basin_name
            );
        }
        Err(s2_sdk::types::S2Error::Server(ref resp))
            if resp.code == "resource_already_exists" =>
        {
            println!("Basin '{}' exists, reconfiguring...", basin_name);
            let reconfig = s2_sdk::types::BasinReconfiguration::new()
                .with_create_stream_on_append(true)
                .with_create_stream_on_read(true);
            let reconfig_input =
                s2_sdk::types::ReconfigureBasinInput::new(basin_name_parsed, reconfig);
            s2.reconfigure_basin(reconfig_input).await?;
            println!(
                "{} Basin '{}' reconfigured",
                "+".bright_green(),
                basin_name
            );
        }
        Err(e) => {
            return Err(error::OrchestratorError::S2Init(format!(
                "Failed to create basin: {e}"
            )));
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    // Second Ctrl+C = hard exit (first is caught by the research loop for graceful cleanup)
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await; // first — handled by research loop
        let _ = tokio::signal::ctrl_c().await; // second — force exit
        eprintln!("\nForce quit.");
        std::process::exit(130);
    });

    let cli = Cli::parse();
    let log_to_file = matches!(cli.command, Command::Join { .. } | Command::Research { .. });

    if log_to_file {
        let log_dir = dirs::cache_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
            .join("parallax");
        std::fs::create_dir_all(&log_dir).ok();
        let log_file = std::fs::File::create(log_dir.join("parallax.log"))
            .expect("Failed to create log file");
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .with_writer(std::sync::Mutex::new(log_file))
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .init();
    }

    let config = config::Config::load().into_diagnostic()?;

    match cli.command {
        Command::Join {
            swarm_id,
            agent,
            dir,
            max_turns,
            context,
            group,
        } => {
            swarm::join(
                &swarm_id,
                &agent,
                max_turns,
                dir.as_deref(),
                context.as_deref(),
                group.as_deref(),
                &config,
                cli.basin.as_deref(),
            )
            .await
            .into_diagnostic()?;
        }

        Command::Watch { id } => {
            status::watch(&config, cli.basin.as_deref(), id.as_deref())
                .await
                .into_diagnostic()?;
        }

        Command::Message {
            swarm_id,
            message,
            to,
        } => {
            let streams = streams::connect(&config, cli.basin.as_deref()).into_diagnostic()?;
            let run_id = types::RunId(swarm_id.clone());

            let strategy = streams.read_strategy(&run_id).await.into_diagnostic()?;
            let is_research = strategy.as_ref()
                .map(|s| matches!(s.strategy_type, swarm::StrategyType::Research { .. }))
                .unwrap_or(false);

            if is_research {
                if let Some(group_name) = to {
                    if group_name == "moderator" {
                        let mod_stream = format!("swarm/{}/moderator/commands", swarm_id);
                        chat::append_to_stream(&streams, &mod_stream, "Human", &message)
                            .await
                            .into_diagnostic()?;
                        println!("Command sent to moderator.");
                    } else {
                        let group_stream = format!("swarm/{}/group/{}", swarm_id, group_name);
                        chat::append_to_stream(&streams, &group_stream, "Human", &message)
                            .await
                            .into_diagnostic()?;
                        println!("Message sent to group '{}'.", group_name);
                    }
                } else {
                    if let Some(swarm::Strategy {
                        strategy_type: swarm::StrategyType::Research { groups, .. },
                        ..
                    }) = strategy
                    {
                        for group in &groups {
                            let group_stream = format!("swarm/{}/group/{}", swarm_id, group.name);
                            chat::append_to_stream(&streams, &group_stream, "Human", &message)
                                .await
                                .into_diagnostic()?;
                        }
                        println!("Message broadcast to all groups.");
                    }
                }
            } else {
                let msg = types::SwarmMessage::steer(&message, to.as_deref());
                streams.send_message(&run_id, &msg).await.into_diagnostic()?;
                println!("Message sent.");
            }
        }

        Command::Research {
            question,
            hint,
            groups,
            agents_per_group,
            max_messages,
            max_dynamic_streams,
            max_phase_transitions,
            timeout,
            agent,
            model,
        } => {
            research::start_research(
                &question,
                hint.as_deref(),
                groups,
                agents_per_group,
                max_messages,
                max_dynamic_streams,
                max_phase_transitions,
                timeout,
                &agent,
                model.as_deref(),
                &config,
                cli.basin.as_deref(),
            )
            .await
            .into_diagnostic()?;
        }

        Command::Init { basin } => {
            init_basin(&config, &basin)
                .await
                .into_diagnostic()?;
        }

        Command::CodeReview {
            task,
            max_iterations,
        } => {
            code_review::start_code_review(&task, max_iterations, &config, cli.basin.as_deref())
                .await
                .into_diagnostic()?;
        }
    }

    Ok(())
}
