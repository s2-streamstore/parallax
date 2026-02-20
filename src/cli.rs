use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "parallax",
    about = "Distributed agent research tool backed by S2 streams",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// S2 basin name
    #[arg(long, global = true, env = "PARALLAX_BASIN")]
    pub basin: Option<String>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Join an existing swarm from this machine
    Join {
        /// Swarm ID to join
        swarm_id: String,

        /// Agent backend: claude, codex, gemini, human, or a custom command
        #[arg(long, default_value = "claude")]
        agent: String,

        /// Working directory for this agent
        #[arg(long)]
        dir: Option<PathBuf>,

        /// Max turns
        #[arg(long, default_value = "100")]
        max_turns: usize,

        /// Context about what this machine has access to (injected into agent prompts)
        #[arg(long, short = 'c')]
        context: Option<String>,

        /// Join a specific research group
        #[arg(long)]
        group: Option<String>,
    },

    /// Real-time tail of a swarm's events
    Watch {
        /// Swarm ID (default: latest)
        #[arg(long)]
        id: Option<String>,
    },

    /// Send a steering message to agents in a running swarm
    Message {
        /// Swarm ID
        swarm_id: String,

        /// Message content
        message: String,

        /// Target a specific worker (default: broadcast to all)
        #[arg(long)]
        to: Option<String>,
    },

    /// Deep multi-agent research with dynamic AI-designed strategies
    Research {
        /// The research question or topic
        question: String,

        /// Strategy hint - guide the AI's methodology design
        /// Examples: "red team vs blue team", "delphi forecasting with 5 rounds",
        ///           "expert panel debate", "pre-mortem failure analysis"
        #[arg(long)]
        hint: Option<String>,

        /// Number of groups/perspectives (AI may adjust based on question)
        #[arg(long, short = 'g', default_value = "4")]
        groups: usize,

        /// Agents per group (AI may adjust based on strategy)
        #[arg(long, short = 'n', default_value = "3")]
        agents_per_group: usize,

        /// Max messages per agent
        #[arg(long, default_value = "30")]
        max_messages: usize,

        /// Optional cap on moderator-created streams (0 disables dynamic stream creation)
        #[arg(long)]
        max_dynamic_streams: Option<usize>,

        /// Maximum number of moderator phase transitions before forced conclude
        #[arg(long, default_value = "3")]
        max_phase_transitions: usize,

        /// Wall-clock timeout in minutes for the research run (force conclude on expiry)
        #[arg(long)]
        timeout: Option<u64>,

        /// Agent backend
        #[arg(long, default_value = "claude")]
        agent: String,

        /// Model to use (e.g. claude-sonnet-4-5-20250929, opus-4). Overrides PARALLAX_AGENT_MODEL.
        #[arg(long)]
        model: Option<String>,
    },

    /// Initialize S2 basin
    Init {
        /// Basin name
        basin: String,
    },

    /// Code review mode: Claude writes code, Codex reviews with continuous feedback
    CodeReview {
        /// The coding task or feature to implement
        task: String,

        /// Maximum review iterations
        #[arg(long, default_value = "5")]
        max_iterations: usize,
    },
}
