<div align="center">
  <p>
    <a href="https://s2.dev">
      <img src="logo.svg" alt="parallax" height="60"/>
    </a>
  </p>

  <p>
    <a href="https://discord.gg/vTCs7kMkAf"><img src="https://img.shields.io/discord/1209937852528599092?logo=discord&label=discord" /></a>
    <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" /></a>
    <a href="https://s2.dev"><img src="https://img.shields.io/badge/powered%20by-S2-6366f1" /></a>
  </p>

  <p>Multi-agent research on S2 streams for independent reasoning and structured convergence.</p>
</div>

> **Note:** This is a vibecoded proof of concept. Expect rough edges.

---

Most AI research tools run one model, in one context, asking itself to consider multiple perspectives. Parallax spawns independent agent cohorts on isolated S2 streams. Each group reasons without seeing what the others are doing, then synthesizes after the fact. S2 streams can be dynamically created to execute any reasoning strategy.

## Getting started

1. Get an S2 access token from [s2.dev/dashboard](https://s2.dev/dashboard) and create a basin.

2. Build and install:
   ```bash
   cargo build --release
   cp target/release/parallax ~/.local/bin/parallax
   ```

3. Configure:
   ```bash
   mkdir -p ~/.config/parallax
   cat > ~/.config/parallax/config.toml << EOF
   [s2]
   access_token = "your-s2-token"
   basin = "your-basin"

   [anthropic]
   # optional model override used by local Claude planner
   model = "claude-sonnet-4-5-20250929"
   EOF
   ```

   Or via env vars: `S2_ACCESS_TOKEN`, `PARALLAX_BASIN`, `PARALLAX_MODEL`.

   > The planner now runs through a local CLI backend.
   > It defaults to `claude`; use `--planner-agent codex` only when you want Codex for planning.

4. Run your first research session:
   ```bash
   parallax research "What are the biggest risks for a coffee shop business in 2026?" \
     --hint "adversarial: bulls vs bears vs analysts" \
     --groups 3 --agents-per-group 2 --max-messages 15
   ```

If planning fails because a backend CLI is missing, either install it (`claude` or `codex`) or switch planner backend:

```bash
parallax research "..." --planner-agent codex
parallax research "..." --planner-agent claude
```

## Usage

### Adversarial cohorts

```bash
parallax research "Should we expand our business outside of SF?" \
  --hint "adversarial: bulls vs bears vs market analysts" \
  --groups 3 --agents-per-group 2 --max-messages 15
```

Three independent groups research the same question. No group sees another's findings until synthesis. The planner designs the methodology and the moderator drives it.

### Delphi forecasting

```bash
parallax research "What % of production AI agent deployments will require \
  durable stream infrastructure by 2028?" \
  --hint "delphi forecasting, 3 rounds" \
  --groups 5 --agents-per-group 1 --max-messages 5
```

Five independent panelists estimate. The moderator aggregates, feeds the aggregate back as context, and runs another round. Estimates converge.

### Join a running swarm from another machine

```bash
parallax join <swarm-id>
parallax join <swarm-id> --group "bears" --agent codex
parallax join <swarm-id> --agent human  # participate yourself
```

Any machine with credentials can join. The strategy is read from S2.

### Watch live

```bash
parallax watch
parallax watch --id <swarm-id>
```

### Steer agents mid-run

```bash
parallax message <swarm-id> "focus on the regulatory angle" --to "bears"
parallax message <swarm-id> "wrap up and synthesize"
```

## How it works

```
parallax research "..."
       |
       v
  Planner (Claude)
  designs strategy JSON
  ────────────────────────────────────────────────────
  topology: groups / rounds / hierarchical / custom
  agent_mode: persistent_chat / one_shot
  aggregation: when and how to synthesize
  ────────────────────────────────────────────────────
       |
       v
  Executor spawns agents on S2 streams

  group/bulls  ---- Agent ---- Agent -------------------► tail
  group/bears  ---- Agent ---- Agent -------------------► tail
  group/macro  ---- Agent ---- Agent -------------------► tail

  (streams are isolated - no cross-reading during generation)
       |
       v
  Autonomous moderator reads all streams
  decides: steer / spawn breakout / start next phase / conclude
       |
       v
  Synthesis - final report
```

Agents are persistent `claude` or `codex` sessions with bidirectional `stream-json` I/O. They read from their stream in real time, respond, and the response is appended back. All state lives in S2 - crash and resume from the tail.

## Mix Claude and Codex

```bash
parallax research "Audit the auth module for security issues" \
  --hint "use codex for code review, claude for threat modeling" \
  --groups 2 --agents-per-group 2
```

The planner assigns backends per group. Claude agents have full tool access (WebSearch, WebFetch, Bash). Codex agents run with full permissions in this integration and are useful for verification and implementation-heavy workflows.

## Commands

| Command | Description |
|---|---|
| `parallax research <question>` | Start a research session |
| `parallax join <swarm-id>` | Join an existing session |
| `parallax watch` | Tail the events stream |
| `parallax message <swarm-id> <msg>` | Steer agents mid-run |
| `parallax code-review <task>` | Claude writes, Codex reviews |
| `parallax init <basin>` | Initialize an S2 basin |

**research flags:** `--hint`, `--groups`, `--agents-per-group`, `--max-messages`, `--agent`, `--planner-agent`, `--model`

**join flags:** `--group`, `--agent`, `--max-turns`, `--context`, `--dir`

## Feedback

Use [GitHub Issues](https://github.com/s2-streamstore/parallax/issues) to report bugs or request features.

## Reach out

Join the [Discord](https://discord.gg/vTCs7kMkAf) or email [hi@s2.dev](mailto:hi@s2.dev).

## License

[MIT](./LICENSE)
