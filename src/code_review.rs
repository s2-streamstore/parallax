use colored::Colorize;

use crate::agent::AgentBackend;
use crate::config::Config;
use crate::error::Result;
use crate::types::*;

pub async fn start_code_review(
    task: &str,
    max_iterations: usize,
    config: &Config,
    basin_override: Option<&str>,
) -> Result<()> {
    let streams = crate::streams::connect(config, basin_override)?;
    let swarm_id = RunId::generate();

    println!("\n{} Starting code review session...", "[REVIEW]".bright_cyan());
    println!("  Task: {}", task);
    println!("  Writer: Claude (code generation)");
    println!("  Reviewer: Codex (code review)");
    println!("  Max iterations: {}", max_iterations);
    println!("  Swarm ID: {}\n", swarm_id.short());

    let code_stream = format!("swarm/{}/code", swarm_id.0);
    let review_stream = format!("swarm/{}/reviews", swarm_id.0);
    let feedback_stream = format!("swarm/{}/feedback", swarm_id.0);

    let claude = AgentBackend::Claude {
        model: config.anthropic.agent_model.clone(),
    };
    let codex = AgentBackend::Codex;

    println!("{} Iteration 1: Claude writing initial code...\n", ">".bright_blue());

    let initial_prompt = format!(
        "Write clean, production-quality code for this task:\n\n{}\n\n\
         Provide complete, working code with comments. Output ONLY the code, no explanation.",
        task
    );

    let mut current_code = claude
        .prompt("You are an expert software engineer.", &initial_prompt)
        .await?;

    crate::chat::append_to_stream(&streams, &code_stream, "Claude", &current_code).await?;

    println!("{}", "─".repeat(80).dimmed());
    println!("{}", current_code);
    println!("{}", "─".repeat(80).dimmed());

    for iteration in 1..=max_iterations {
        println!("\n{} Iteration {}: Codex reviewing...\n", ">".bright_yellow(), iteration);

        let review_prompt = format!(
            "Review this code for the task: \"{}\"\n\n\
             CODE:\n```\n{}\n```\n\n\
             Provide:\n\
             1. APPROVED or NEEDS_REVISION\n\
             2. Specific issues found (bugs, style, performance, safety)\n\
             3. Concrete suggestions for improvement\n\n\
             Format:\n\
             STATUS: [APPROVED/NEEDS_REVISION]\n\
             ISSUES:\n- [list issues]\n\
             SUGGESTIONS:\n- [specific improvements]",
            task, current_code
        );

        let review = codex
            .prompt("You are a senior code reviewer.", &review_prompt)
            .await?;

        crate::chat::append_to_stream(&streams, &review_stream, "Codex", &review).await?;

        println!("{} Review:", "[REVIEW]".dimmed());
        println!("{}", review);

        if review.contains("STATUS: APPROVED") {
            println!("\n{} Code approved by Codex!", "+".bright_green().bold());
            println!("\n{} Final code:", "[FINAL]".bright_green());
            println!("{}", "═".repeat(80).bright_green());
            println!("{}", current_code);
            println!("{}", "═".repeat(80).bright_green());

            let artifact_stream = format!("swarm/{}/artifacts/final-code", swarm_id.0);
            crate::chat::append_to_stream(&streams, &artifact_stream, "System", &current_code).await?;

            break;
        }

        if iteration == max_iterations {
            println!("\n{} Max iterations reached. Code not fully approved.", "⚠".bright_yellow());
            break;
        }

        println!("\n{} Iteration {}: Claude revising based on feedback...\n", ">".bright_blue(), iteration + 1);

        let revision_prompt = format!(
            "Here's your code:\n```\n{}\n```\n\n\
             Code review feedback:\n{}\n\n\
             Revise the code to address ALL the issues and suggestions. \
             Output ONLY the revised code, no explanation.",
            current_code, review
        );

        current_code = claude
            .prompt("You are an expert software engineer incorporating code review feedback.", &revision_prompt)
            .await?;

        crate::chat::append_to_stream(&streams, &code_stream, "Claude", &format!("Revision {}: {}", iteration + 1, current_code)).await?;

        println!("{}", "─".repeat(80).dimmed());
        println!("{}", current_code);
        println!("{}", "─".repeat(80).dimmed());

        let feedback_summary = format!(
            "Iteration {}: Codex identified issues, Claude revised",
            iteration + 1
        );
        crate::chat::append_to_stream(&streams, &feedback_stream, "System", &feedback_summary).await?;
    }

    println!("\n{} Code review session complete!", "+".bright_green());
    println!("  View full history: parallax watch --id {}", swarm_id.short());
    println!("  Code stream: {}", code_stream);
    println!("  Review stream: {}", review_stream);

    Ok(())
}

