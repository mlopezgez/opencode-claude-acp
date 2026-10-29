# claude-code-acp

OpenCode V2 plugin that uses opencode as a TUI for Claude Code. It runs Claude Code through its
ACP adapter (`@agentclientprotocol/claude-agent-acp`) and registers it as the `claude-code` provider.

## Use

Pick a model with `/models`, or pass one on the command line:

```sh
opencode run -m claude-code/opus "…"
opencode run -m 'claude-code/sonnet#high' "…"   # variants set Claude Code's effort: low, medium, high, xhigh, max
```

Models: `claude-code/default`, `opus`, `sonnet`, `fable`, `haiku`.

To make it the default, set `"model": "claude-code/opus"` in `~/.config/opencode/opencode.jsonc`.

## How it works

- Each opencode session maps to one Claude Code session. The mapping is kept in plugin storage, so
  continuing a session after a restart resumes the same Claude Code session (`session/resume`).
- Only the newest user message is sent: Claude Code keeps its own history and context. The first turn of a
  fresh Claude Code session (for example after switching from another model) also gets the earlier transcript.
- Claude Code runs its own tools (Bash, Read, Edit, …). They show in opencode as tool calls, using opencode's
  shell/read/write/edit/grep/glob/todo views where one exists.
- Edits show as diffs. opencode only draws diffs for tools it runs itself, so Claude Code's Edit/MultiEdit calls
  are reported as opencode `edit` calls. The plugin wraps opencode's `edit` tool: for those calls it waits for
  Claude Code to make the change and returns the diff; calls from other models reach the original tool.
  A step that reports an edit ends with "tool-calls" while Claude Code keeps working; opencode runs the edit,
  asks the model again, and that request picks up the same Claude Code turn. Only the turn's real end finishes
  a step with "stop", so each turn gets one footer. Edit permission prompts also show the diff.
- Claude Code's permission requests go through opencode's permission rules and prompt. Bash maps to the
  `shell` action, file changes to `edit`, reads to `read`, web access to `webfetch`/`websearch`, and paths
  outside the project to `external_directory`. Commands that Claude Code already allows (its own settings,
  read-only commands) don't ask.
- Esc / abort sends `session/cancel` to Claude Code.
- Claude Code compacts its own context. The models report a very large context limit so opencode doesn't
  summarize underneath it.
- Authentication is your existing Claude Code login.

## Options

The plugin loads automatically from `~/.config/opencode/plugins/`. To change its defaults, add an entry for
it to `~/.config/opencode/opencode.jsonc`:

```jsonc
"plugins": [{ "package": "./plugins/claude-code-acp", "options": { "mode": "acceptEdits" } }]
```

| Option    | Default                       | Meaning                                                                      |
| --------- | ----------------------------- | ---------------------------------------------------------------------------- |
| `mode`    | Claude Code's default         | Permission mode for new sessions: `default`, `acceptEdits`, `plan`, `auto`, `bypassPermissions` |
| `command` | `node <bundled adapter>`      | Command that starts the ACP agent, as an array                               |
| `env`     | –                             | Extra environment variables for the agent                                   |

## Files

- `index.ts` — the plugin: provider registration, ACP client, stream and permission translation.
- `sdk.js` — stub provider package. opencode loads a provider package before plugin hooks run; the real model
  comes from the plugin's `language` hook.
