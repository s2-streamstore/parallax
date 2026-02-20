use crate::agent::AgentBackend;
use crate::error::{OrchestratorError, Result};
use crate::research::ResearchStrategy;

pub struct Planner {
    backend: AgentBackend,
}

impl Planner {
    pub fn new(backend: AgentBackend) -> Self {
        Self { backend }
    }

    pub async fn design_research_strategy(
        &self,
        question: &str,
        hint: Option<&str>,
        num_groups: usize,
        agents_per_group: usize,
        max_messages: usize,
    ) -> Result<ResearchStrategy> {
        let system_prompt = r#"You are a research methodology architect with unlimited creativity.

Design SOPHISTICATED, COMPLEX multi-agent strategies for any question. Don't be constrained by traditional methods.

You can invent:
- Multi-phase strategies (exploration → deep-dive → synthesis → validation)
- Hierarchical structures (groups that spawn subgroups)
- Adversarial setups (red team vs blue team with judges)
- Convergence mechanisms (rounds with feedback and adaptation)
- Meta-coordination (observer agents that guide others)
- Evidence chains (claims → verification → synthesis)
- Parallel + sequential hybrid approaches
- Recursive investigation (agents spawn sub-research)
- Dynamic resource allocation (spawn more agents in promising areas)
- Cross-pollination (groups exchange insights mid-research)
- Confidence tracking and uncertainty quantification
- Or ANYTHING ELSE that fits the question

Think BIG. Design strategies that would be impressive, thorough, and novel.

Return JSON with this structure:
{
  "name": "Strategy name (e.g. 'Red Team / Blue Team', 'Expert Panel with Devil's Advocate')",
  "question": "The research question",
  "topology": {
    "type": "groups",  // or "rounds", "hierarchical", "custom"
    "groups": [
      {
        "name": "Group name (e.g. 'Security Experts', 'Cost Analysts')",
        "prompt": "Detailed prompt for this group's agents",
        "agents": 3,
        "agent": "claude"  // optional: "claude" (default, has tools), "codex" (full-permission local executor)
      }
    ]
  },
  "execution": {
    "agent_mode": {
      "type": "persistent_chat",
      "system_prompt": "Base instructions for all agents"
    },
    "agent_count": 12,
    "distribution": {
      "type": "per_stream",  // MUST BE: "per_stream", "even_split", or "custom"
      "count": 3             // Required for per_stream
    },
    // OR for even split across all streams:
    // "distribution": { "type": "even_split" },
    // OR for custom allocation:
    // "distribution": {
    //   "type": "custom",
    //   "allocations": [["stream1", 4], ["stream2", 3]]
    // },
    "max_messages_per_agent": 30
  },
  "aggregation": [
    {
      "trigger": {
        "type": "budget_percent",
        "percent": 0.7
      },
      "sources": ["group/*"],
      "method": {
        "type": "llm_synthesis",
        "prompt": "Wrap-up prompt for mid-point synthesis"
      },
      "destination": "display"
    }
  ],
  "synthesis": {
    "input_streams": ["group/GroupName1", "group/GroupName2"],
    "prompt_template": "Final synthesis prompt with {findings} placeholder"
  }
}

PATTERNS YOU CAN USE (not constraints, just possibilities):

SIMPLE PATTERNS:
- Parallel expert groups → moderator synthesis
- Rounds with convergence (Delphi-style)
- Red team vs Blue team with judges

ADVANCED PATTERNS:
- Multi-phase: Phase1 (explore) → Phase2 (deep-dive winners) → Phase3 (validate)
- Hierarchical: Root groups → each spawns 2-3 subgroups for deep investigation
- Adversarial + Synthesis: Attack/Defend groups + Neutral analysts + Meta-reviewers
- Evidence Chains: Claim-makers → Fact-checkers → Synthesis → Peer review
- Adaptive: Start broad (6 groups) → moderator focuses resources on 2 most promising → deep dive

COMPLEX PATTERNS:
- Recursive: Groups can spawn sub-research questions with their own strategies
- Cross-stream: Some agents read from multiple streams and synthesize
- Meta-coordination: Observer agents that allocate resources dynamically
- Confidence cascade: Each layer increases confidence through validation
- Parallel scenarios: Explore 4 futures in parallel → cross-compare → synthesize

TOPOLOGIES:
- "groups": Parallel cohorts (most common)
- "rounds": Sequential rounds with feedback (forecasting, convergence)
- "hierarchical": Tree structure with spawning (deep investigation)
- "custom": Define any arbitrary graph of streams

CRITICAL JSON SCHEMA RULES:
- topology.type MUST BE: "groups", "rounds", "hierarchical", or "custom"
- agent_mode.type MUST BE: "persistent_chat" or "one_shot"
- distribution.type MUST BE: "per_stream", "even_split", or "custom"
- aggregation.trigger.type MUST BE: "budget_percent", "time_elapsed", "round_complete", etc.
- aggregation.method.type MUST BE: "llm_synthesis" or "statistical"

AGENT BACKENDS:
- "claude" (default): full tool access (WebSearch, WebFetch, Read, Bash). Best for research, analysis, tool-using investigation.
- "codex": full-permission local executor in this integration. Best for verification, code review, and shell-driven investigation.
- You can mix backends in the same strategy — e.g. Claude groups do research, a Codex group reviews and verifies.
- Set "agent" on any group to override the default. Omit it to use the session default (claude).

DESIGN PRINCIPLES:
- Design for the SPECIFIC question, not generic templates
- Use complexity when it adds value, simplicity when appropriate
- Name everything meaningfully (groups, streams, roles)
- Think about information flow and synthesis points
- Keep agent_count reasonable (10-30 for most questions, up to 100 for complex)

Return ONLY valid JSON matching the EXACT schema shown above. No markdown, no explanation, no comments in the JSON."#;

        let hint_text = hint
            .map(|h| format!("\n\nUSER HINT: {}", h))
            .unwrap_or_default();

        let user_prompt = format!(
            r#"Design a research strategy for this question:

QUESTION: {}

CONSTRAINTS (must obey):
- EXACTLY {} groups (do not add more)
- EXACTLY {} agents per group (do not exceed)
- Max messages per agent: {}

{}

CRITICAL PROMPT DESIGN RULES:
- Give EVERY agent a PROACTIVE initial task, not just reactive roles
- Reactive roles (critics, reviewers, judges) need ACTIVE starting points
- Agents should start working immediately, not wait for others

TOOL USAGE (CRITICAL):
- Agents have access to tools (WebSearch, WebFetch, Read, Bash, etc.)
- For questions requiring current/factual data: Prompts should instruct agents to use tools
- Tell agents to look up real data before analyzing, not rely on training data
- This prevents hallucination and grounds research in facts

DESIGN PROCESS:
1. Analyze the question's complexity, scope, and nature
2. Decide the appropriate sophistication level:
   - Simple question? → Straightforward parallel groups
   - Complex question? → Multi-phase, hierarchical, or adaptive
   - Forecasting? → Rounds with convergence
   - Controversial? → Adversarial with judges
   - Technical? → Evidence chains with validation
   - Exploratory? → Broad then narrow approach

3. Design the optimal strategy considering:
   - What perspectives/expertise are needed?
   - What's the information flow? (parallel, sequential, hierarchical, graph)
   - How should insights be synthesized? (single synthesis, multi-layer, iterative)
   - Would phases help? (explore → deep-dive → validate)
   - Should resources be dynamic? (spawn more agents where needed)
   - Are there adversarial elements? (challenge, critique, devil's advocate)

4. Create meaningful names for groups/streams that reflect their roles

5. Design synthesis that produces actionable insights


Return the JSON strategy now (ONLY JSON, no explanation):"#,
            question, num_groups, agents_per_group, max_messages, hint_text
        );

        let json_text_owned = self
            .backend
            .prompt(system_prompt, &user_prompt)
            .await
            .map_err(|e| {
                if self.backend.name() == "claude" {
                    OrchestratorError::Planner(format!(
                        "Planner backend 'claude' failed. Is `claude` installed and authenticated? You can switch with --planner-agent codex. Error: {e}"
                    ))
                } else {
                    OrchestratorError::Planner(format!(
                        "Planner backend 'codex' failed. Is `codex` installed and authenticated? You can switch with --planner-agent claude. Error: {e}"
                    ))
                }
            })?;

        if json_text_owned.is_empty() {
            return Err(OrchestratorError::Planner(format!(
                "Local {} planner returned empty output",
                self.backend.name()
            )));
        }
        let json_text = json_text_owned.as_str();

        // Try to parse as JSON directly, or extract from markdown
        let strategy: ResearchStrategy = if let Ok(s) = serde_json::from_str(json_text) {
            s
        } else {
            // Try to extract JSON from markdown code blocks
            let json_str = extract_json_from_markdown(json_text)
                .ok_or_else(|| OrchestratorError::Planner(format!("No valid JSON found in response: {}", json_text)))?;

            // Try to parse, and if it fails, show the JSON for debugging
            match serde_json::from_str::<ResearchStrategy>(&json_str) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("\n[DEBUG] Failed to parse strategy JSON:");
                    eprintln!("{}", json_str);
                    eprintln!("\n[DEBUG] Parse error: {}", e);
                    return Err(OrchestratorError::Planner(format!("Failed to parse strategy JSON: {}", e)));
                }
            }
        };

        Ok(strategy)
    }
}

/// Extract JSON from markdown code blocks or raw text
fn extract_json_from_markdown(text: &str) -> Option<String> {
    // Try to find ```json ... ``` blocks
    if let Some(start) = text.find("```json") {
        if let Some(end) = text[start + 7..].find("```") {
            return Some(text[start + 7..start + 7 + end].trim().to_string());
        }
    }

    // Try to find ``` ... ``` blocks without language
    if let Some(start) = text.find("```") {
        let after_start = start + 3;
        // Skip the language identifier if present
        let json_start = text[after_start..]
            .find('\n')
            .map(|i| after_start + i + 1)
            .unwrap_or(after_start);
        if let Some(end) = text[json_start..].find("```") {
            return Some(text[json_start..json_start + end].trim().to_string());
        }
    }

    // Try to find raw JSON (starts with { or [)
    let trimmed = text.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return Some(trimmed.to_string());
    }

    None
}
