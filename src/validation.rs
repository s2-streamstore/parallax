use crate::error::{OrchestratorError, Result};
use crate::research::ResearchStrategy;

pub fn validate_strategy(strategy: &ResearchStrategy) -> Result<()> {
    if strategy.name.is_empty() {
        return Err(OrchestratorError::Research(
            "Strategy name cannot be empty".into(),
        ));
    }

    if strategy.question.is_empty() {
        return Err(OrchestratorError::Research(
            "Strategy question cannot be empty".into(),
        ));
    }

    match &strategy.topology {
        crate::research::StreamTopology::Groups { groups } => {
            if groups.is_empty() {
                return Err(OrchestratorError::Research(
                    "Strategy must have at least one group".into(),
                ));
            }
            for group in groups {
                if group.name.is_empty() {
                    return Err(OrchestratorError::Research(
                        "Group name cannot be empty".into(),
                    ));
                }
                if group.agents == 0 {
                    return Err(OrchestratorError::Research(
                        format!("Group '{}' must have at least 1 agent", group.name),
                    ));
                }
                if group.agents > 20 {
                    return Err(OrchestratorError::Research(
                        format!("Group '{}' has too many agents ({}). Maximum is 20 per group for stability.", group.name, group.agents),
                    ));
                }
            }
        }
        crate::research::StreamTopology::Rounds(rounds_config) => {
            use crate::research::RoundsConfig;
            let (rounds, instances_per_round) = match rounds_config {
                RoundsConfig::Simple { rounds, instances_per_round } => (rounds, instances_per_round),
                RoundsConfig::Complex { rounds } => (&rounds.len(), &8usize),  // Default for complex
            };
            if *rounds == 0 {
                return Err(OrchestratorError::Research(
                    "Must have at least 1 round".into(),
                ));
            }
            if *instances_per_round == 0 {
                return Err(OrchestratorError::Research(
                    "Must have at least 1 instance per round".into(),
                ));
            }
            if *rounds > 10 {
                return Err(OrchestratorError::Research(
                    "Too many rounds. Maximum is 10 for practical convergence.".into(),
                ));
            }
        }
        _ => {} // Other topologies validated during execution
    }

    let total_agents = strategy.execution.agent_count;
    if total_agents == 0 {
        return Err(OrchestratorError::Research(
            "Strategy must have at least 1 agent".into(),
        ));
    }
    if total_agents > 100 {
        return Err(OrchestratorError::Research(
            format!("Too many total agents ({}). Maximum is 100 for stability and cost control.", total_agents),
        ));
    }

    if strategy.execution.max_messages_per_agent == 0 {
        return Err(OrchestratorError::Research(
            "max_messages_per_agent must be at least 1".into(),
        ));
    }

    if strategy.execution.max_messages_per_agent > 200 {
        return Err(OrchestratorError::Research(
            "max_messages_per_agent too high (>200). This could lead to excessive costs.".into(),
        ));
    }

    if strategy.synthesis.input_streams.is_empty() {
        return Err(OrchestratorError::Research(
            "Synthesis must read from at least one input stream".into(),
        ));
    }

    if strategy.synthesis.prompt_template.is_empty() {
        return Err(OrchestratorError::Research(
            "Synthesis prompt template cannot be empty".into(),
        ));
    }

    Ok(())
}

pub struct StrategyEstimate {
    pub total_agents: usize,
    pub max_messages: usize,
    pub estimated_api_calls: usize,
    pub estimated_duration_minutes: usize,
}

pub fn estimate_strategy_cost(strategy: &ResearchStrategy) -> StrategyEstimate {
    let total_agents = strategy.execution.agent_count;
    let max_messages_per_agent = strategy.execution.max_messages_per_agent;
    let max_messages = total_agents * max_messages_per_agent;

    // Estimate API calls (each message = 1 call, plus synthesis calls)
    let estimated_api_calls = max_messages + strategy.aggregation.len() + 1; // +1 for final synthesis

    // Estimate duration (rough: 1 message per agent per 10 seconds)
    let estimated_duration_minutes = (max_messages_per_agent * 10) / 60;

    StrategyEstimate {
        total_agents,
        max_messages,
        estimated_api_calls,
        estimated_duration_minutes,
    }
}
