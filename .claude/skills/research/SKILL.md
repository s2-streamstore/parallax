---
name: research
description: Run a Parallax multi-agent research session. Handles pre-checks, builds the binary, and executes the research with sensible defaults.
argument-hint: "<question>" [--hint "..."] [--groups N] [--agents-per-group N] [--max-messages N] [--max-dynamic-streams N] [--max-phase-transitions N] [--timeout M] [--agent claude|codex] [--planner-agent claude|codex] [--model NAME]
---

# Parallax Research Skill

You are launching a Parallax multi-agent research session. The user's request is: **$ARGUMENTS**

## Step 1: Pre-Flight Checks

Run ALL of these checks before proceeding. If any fail, stop and tell the user how to fix it.

### 1a. Environment Variables

Check that these are set (or configured in `~/.config/parallax/config.toml`):

```bash
echo "S2_ACCESS_TOKEN: ${S2_ACCESS_TOKEN:+SET}"
echo "PARALLAX_BASIN: ${PARALLAX_BASIN:+SET}"
echo "PARALLAX_MODEL: ${PARALLAX_MODEL:+SET}"
```

- `S2_ACCESS_TOKEN` - **REQUIRED**. Without this, Parallax cannot connect to S2 streams.
- `PARALLAX_BASIN` - **REQUIRED** (unless `--basin` is passed). The S2 basin for storing research streams.
- `PARALLAX_MODEL` - Optional model override for local Claude backend.
- Backend CLIs (`claude` and/or `codex`) must be installed for whichever planner/agent backend the run uses.

If any are missing, tell the user:
```
Missing: S2_ACCESS_TOKEN
Set it with: export S2_ACCESS_TOKEN="your-token"
Or add to ~/.config/parallax/config.toml under [s2] access_token = "..."
```

### 1b. Binary Build Check

```bash
ls -la ./target/release/parallax 2>/dev/null
```

If the binary doesn't exist or is older than any `src/*.rs` file, rebuild:

```bash
cargo build --release 2>&1
```

If the build fails, show the errors and stop.

### 1c. Process Check

```bash
# Make sure no existing parallax research processes are running
pgrep -af "parallax research" || true
```

If there are existing parallax research processes, **WARN the user**:
> There's already a parallax research process running (PID XXXX). Running another one simultaneously can spawn many agent processes and may be expensive. Kill it first with `kill <pid>` or wait for it to finish.

## Step 2: Parse User Intent

From the user's `$ARGUMENTS`, determine:

1. **Question** (required) - The research topic/question
2. **Hint** (optional) - Strategy guidance for the planner
3. **Groups** (default: 4) - Number of research groups
4. **Agents per group** (default: 3) - Agents in each group
5. **Max messages** (default: 25) - Per-agent message budget
6. **Timeout** (default: 20 minutes) - Wall-clock timeout
7. **Max phase transitions** (default: 3) - Moderator phase limit
8. **Max dynamic streams** (optional) - Cap on moderator-created streams
9. **Model** (optional) - Override agent model
10. **Output file** (optional) - Where to save the report

If the user only provided a bare question with no flags, use sensible defaults and ASK if they want to customize anything before launching.

## Step 3: Cost & Scale Warnings

Before launching, calculate and display estimates:

| Parameter | Value | Estimated Impact |
|---|---|---|
| Groups | N | N streams created |
| Agents/group | M | N*M total agent processes |
| Max messages | X | Up to N*M*X total model turns |
| Timeout | T min | Hard stop after T minutes |

### Warning Thresholds:

- **Total agents > 20**: Warn "This will spawn {N} agent processes simultaneously. Each consumes memory and model credits. Proceed?"
- **Total agents > 40**: Strong warning "This is a large research run ({N} agents). Estimated cost could be significant. Consider reducing groups or agents-per-group."
- **No timeout set**: Warn "No timeout specified. Adding --timeout 20 as a safety net to prevent runaway costs."
- **Max messages > 30**: Warn "High message budget ({X}/agent). Total possible messages: {total}. Consider --max-messages 20 for a lighter run."

## Step 4: Execute

Run the research in the background so the user can continue working:

```bash
./target/release/parallax research \
  "<question>" \
  [--hint "<hint>"] \
  -g <groups> \
  -n <agents_per_group> \
  --max-messages <max_messages> \
  [--max-dynamic-streams <N>] \
  --max-phase-transitions <max_phase_transitions> \
  [--timeout <timeout_minutes>] \
  [--agent <claude|codex>] \
  [--planner-agent <claude|codex>] \
  [--model <model>] \
  2>&1
```

Run this as a **background task** and periodically check on progress by tailing the output.

## Step 5: Post-Run

When the research completes:

1. **Check exit code** - 0 = success, non-zero = check for errors
2. **Extract the report** - The final synthesis is between the last pair of `━━━━` separator lines
3. **Save to file** - Save the report as a `.md` file in the project root. Name it based on the topic (kebab-case, e.g., `kubernetes-migration-analysis.md`)
4. **Show summary** - Display a brief summary of what was produced (number of groups, continents covered, key findings)

## Common Patterns & Examples

### Quick research (small, fast):
```
/research "What are the best practices for Rust error handling?" --groups 3 --agents 2 --timeout 10
```

### Deep research (thorough, expensive):
```
/research "Compare Kafka vs S2 vs Redpanda for event streaming" --hint "Create pro/con groups for each technology plus a neutral evaluation group" --groups 6 --agents 3 --timeout 30
```

### Continental research pattern (like the remedies run):
```
/research "Traditional cooking techniques by continent" --hint "Create pairs of groups per continent: research (claude) + fact-check (codex)" --groups 12 --agents 2 --timeout 30
```

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Planner error: No valid JSON found` | Strategy too complex for token limit | Reduce groups or simplify hint |
| `S2 connection failed` | Missing/invalid S2_ACCESS_TOKEN | Check token: `echo $S2_ACCESS_TOKEN` |
| `Basin not found` | Basin doesn't exist | Run `parallax init <basin-name>` first |
| `Failed to run claude` | Claude CLI not installed/in PATH | Install Claude Code CLI: `npm install -g @anthropic-ai/claude-code` |
| Process hangs with no output | Local planner CLI is slow or waiting on auth | Run `claude` or `codex` directly and verify it's authenticated; retry with `--planner-agent` |
| `Codex CLI not found` | Hint requested codex agents but codex isn't installed | Install codex or remove codex agent references from hint |

## Important Notes

- Each Claude agent is a separate `claude` CLI process with tool access.
- Codex agents run with full local permissions in this integration.
- Planner uses a local CLI backend (`claude` by default, switch with `--planner-agent codex`).
- The `--timeout` flag is a **wall-clock safety net** - always set one to prevent runaway sessions
- Research output is stored durably in S2 streams even after the process exits. Use `parallax watch` to see events.
- Shutdown signals (`Ctrl+C`, terminate/quit/stop signals) trigger coordinated cleanup of spawned agent process groups.
