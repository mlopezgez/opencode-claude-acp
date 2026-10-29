// OpenCode V2 plugin: run Claude Code (through its ACP adapter) as an opencode model.
//
// Registers a `claude-code` provider. Selecting one of its models routes every turn to a
// Claude Code session over the Agent Client Protocol: Claude Code runs its own tools, and this
// plugin streams its text, thinking and tool calls into the opencode TUI. Claude Code's
// permission requests go through opencode's permission rules and prompt.

import { spawn, type ChildProcess } from "node:child_process"
import { readFile } from "node:fs/promises"
import { createRequire } from "node:module"
import path from "node:path"
import { Readable, Writable } from "node:stream"
import * as acp from "@agentclientprotocol/sdk"
import { createTwoFilesPatch, diffLines } from "diff"
import type {
  LanguageModelV3,
  LanguageModelV3CallOptions,
  LanguageModelV3Message,
  LanguageModelV3StreamPart,
  LanguageModelV3Usage,
} from "@ai-sdk/provider"

const PROVIDER_ID = "claude-code"
const PACKAGE = `aisdk:${new URL("./sdk.js", import.meta.url).href}`
const EFFORTS = ["low", "medium", "high", "xhigh", "max"] as const

// Claude Code model aliases, as its ACP adapter offers them in the `model` config option.
const MODELS = [
  { id: "default", name: "Claude Code (default model)" },
  { id: "opus", name: "Claude Code Opus" },
  { id: "sonnet", name: "Claude Code Sonnet" },
  { id: "fable", name: "Claude Code Fable" },
  { id: "haiku", name: "Claude Code Haiku" },
]

type Options = {
  /** Command that starts the ACP agent. Defaults to `node <bundled claude-agent-acp>`. */
  command?: string[]
  /** Claude Code permission mode for new sessions: default, acceptEdits, plan, auto, bypassPermissions. */
  mode?: string
  /** Extra environment for the agent process. */
  env?: Record<string, string>
}

// Claude Code edits shown through opencode's own `edit` tool, whose view draws the diff.
const LOCAL_EDITS = new Set(["Edit", "MultiEdit"])

type FileDiff = { file: string; patch: string; additions: number; deletions: number; status: "added" | "modified" }

type PendingEdit = PromiseWithResolvers<{ output: unknown; content: string; metadata: { files: FileDiff[] } }>

type Session = {
  acpID: string
  opencodeID: string
  cwd: string
  config: Record<string, string>
  turn?: Turn
}

// The live agent for each location directory. opencode caches language models and keeps them across plugin
// reloads, so models look the agent up here instead of holding the one that created them.
const AGENTS: Map<string, Agent> = ((globalThis as any).__opencodeClaudeCodeAgents ??= new Map())

function liveAgent(cwd: string) {
  const agent = AGENTS.get(cwd)
  if (!agent) throw new Error(`The claude-code-acp plugin is not loaded for ${cwd}`)
  return agent
}

export default {
  id: "claude-code-acp",
  setup: async (ctx: any) => {
    const options: Options = ctx.options ?? {}
    const cwd: string = ctx.location.directory
    const agent = new Agent(options, cwd, ctx)
    AGENTS.get(cwd)?.dispose()
    AGENTS.set(cwd, agent)

    await ctx.provider.transform((editor: any) => {
      editor.add({
        info: { id: PROVIDER_ID, name: "Claude Code (ACP)", activation: "enabled", package: PACKAGE },
        models: MODELS.map((model) => ({
          id: model.id,
          modelID: model.id,
          providerID: PROVIDER_ID,
          name: model.name,
          capabilities: { tools: true, input: ["text", "image"], output: ["text"] },
          variants: EFFORTS.map((effort) => ({ id: effort, settings: { effort } })),
          time: { released: 0 },
          cost: [],
          status: "active",
          enabled: true,
          // Claude Code compacts its own context, so keep opencode from summarizing underneath it.
          limit: { context: 10_000_000, output: 128_000 },
        })),
      })
    })

    await ctx.aisdk.hook("language", (event: any) => {
      if (event.model.providerID !== PROVIDER_ID) return
      event.language = new ClaudeCodeLanguageModel(cwd, event.model.modelID ?? event.model.id, event.options ?? {})
    })

    // opencode only draws diffs for tools it runs itself. Claude Code's edits are reported as `edit` calls
    // that opencode runs; for those, the wrapped `edit` waits for Claude Code to make the change and returns
    // its diff. Calls from other models reach the original tool.
    await ctx.tool.transform((editor: any) => {
      const edit = editor.list().find((tool: any) => tool.name === "edit")
      if (!edit) return
      editor.update(edit.id, (tool: any) => {
        const original = tool.execute
        tool.execute = (input: unknown, context: any) =>
          AGENTS.get(cwd)?.edits.get(context.id)?.promise ?? original(input, context)
      })
    })

    return () => {
      agent.dispose()
      if (AGENTS.get(cwd) === agent) AGENTS.delete(cwd)
    }
  },
}

class ClaudeCodeLanguageModel implements LanguageModelV3 {
  readonly specificationVersion = "v3"
  readonly provider = PROVIDER_ID
  readonly supportedUrls = {}

  constructor(
    private readonly cwd: string,
    readonly modelId: string,
    private readonly settings: Record<string, any>,
  ) {}

  async doGenerate(options: LanguageModelV3CallOptions): Promise<any> {
    const { stream } = await this.doStream(options)
    const content: any[] = []
    let finishReason: any = { unified: "stop", raw: undefined }
    let usage = emptyUsage()
    for await (const part of stream as any as AsyncIterable<LanguageModelV3StreamPart>) {
      if (part.type === "text-delta") {
        const last = content.at(-1)
        if (last?.type === "text") last.text += part.delta
        else content.push({ type: "text", text: part.delta })
      }
      if (part.type === "finish") {
        finishReason = part.finishReason
        usage = part.usage
      }
      if (part.type === "error") throw part.error
    }
    return { content, finishReason, usage, warnings: [] }
  }

  async doStream(options: LanguageModelV3CallOptions) {
    const headers = options.headers ?? {}
    const opencodeID = headers["x-opencode-session-id"] ?? undefined
    const effort = findEffort(this.settings, options.providerOptions)
    const stream = new ReadableStream<LanguageModelV3StreamPart>({
      start: (controller) => {
        controller.enqueue({ type: "stream-start", warnings: [] })
        Promise.resolve()
          .then(() => liveAgent(this.cwd))
          .then((agent) =>
            agent.run({ opencodeID, model: this.modelId, effort, prompt: options.prompt, signal: options.abortSignal, controller }),
          )
          .catch((error) => controller.enqueue({ type: "error", error }))
          .finally(() => {
            try {
              controller.close()
            } catch {}
          })
      },
    })
    return { stream }
  }
}

/** One Claude Code ACP agent process, shared by every opencode session of this location. */
class Agent {
  /** Claude Code edits that opencode runs through the wrapped `edit` tool, by tool call ID. */
  readonly edits = new Map<string, PendingEdit>()
  /** Every call ID reported that way, to recognize opencode's follow-up request after running them. */
  readonly localCalls = new Set<string>()
  private connection?: Promise<acp.ClientConnection>
  private child?: ChildProcess
  private readonly sessions = new Map<string, Session>()
  private readonly byACP = new Map<string, Session>()

  constructor(
    private readonly options: Options,
    private readonly cwd: string,
    private readonly ctx: any,
  ) {}

  /** Stops the agent process. Its sessions are gone with it; the next turn resumes them from storage. */
  dispose(error = new Error("Claude Code ACP agent stopped")) {
    this.child?.kill()
    this.child = undefined
    this.connection = undefined
    for (const session of this.byACP.values()) session.turn?.fail(error)
    this.byACP.clear()
    this.sessions.clear()
  }

  private connect() {
    if (this.connection) return this.connection
    const command = this.options.command ?? [
      "node",
      createRequire(import.meta.url).resolve("@agentclientprotocol/claude-agent-acp/dist/index.js"),
    ]
    const child = spawn(command[0], command.slice(1), {
      cwd: this.cwd,
      env: { ...process.env, ...this.options.env },
      stdio: ["pipe", "pipe", "ignore"],
    })
    this.child = child
    const reset = (error: Error) => {
      if (this.child !== child) return
      this.dispose(error)
    }
    child.on("error", (error) => reset(error))
    child.on("exit", (code) => reset(new Error(`Claude Code ACP agent exited with code ${code}`)))

    const stream = acp.ndJsonStream(
      Writable.toWeb(child.stdin!) as WritableStream<Uint8Array>,
      Readable.toWeb(child.stdout!) as ReadableStream<Uint8Array>,
    )
    const connection = acp
      .client({ name: "opencode" })
      .onNotification("session/update", ({ params }) => {
        this.byACP.get(params.sessionId)?.turn?.update(params.update)
      })
      .onRequest("session/request_permission", ({ params }) => this.permission(params))
      .connect(stream)
    this.connection = connection.agent
      .request("initialize", {
        protocolVersion: acp.PROTOCOL_VERSION,
        clientCapabilities: {},
        clientInfo: { name: "opencode", version: "2" },
      })
      .then(() => connection)
    this.connection.catch(() => reset(new Error("Claude Code ACP agent failed to initialize")))
    return this.connection
  }

  /**
   * Streams one opencode step. A Claude Code turn can span several steps: it pauses at each edit that opencode
   * runs itself, and opencode's next request for the session picks the same turn up again.
   */
  async run(input: {
    opencodeID?: string
    model: string
    effort?: string
    prompt: LanguageModelV3Message[]
    signal?: AbortSignal
    controller: ReadableStreamDefaultController<LanguageModelV3StreamPart>
  }) {
    const opencodeID = input.opencodeID ?? `ephemeral-${crypto.randomUUID()}`
    const active = this.sessions.get(opencodeID)?.turn
    if (this.followUp(input.prompt)) {
      if (active && !active.complete) return active.attach(input.controller, input.signal)
      // Nothing left to show: the turn ended while opencode ran the edit.
      input.controller.enqueue({ type: "finish", finishReason: { unified: "stop", raw: "follow-up" }, usage: emptyUsage() })
      return
    }
    if (active && !active.complete) await active.supersede()

    const turn = new Turn(this, this.cwd)
    const step = turn.attach(input.controller, input.signal)
    void this.drive(turn, input, opencodeID)
    return step
  }

  /** Runs the Claude Code prompt behind a turn, attaching the session first. */
  private async drive(turn: Turn, input: Parameters<Agent["run"]>[0], opencodeID: string) {
    const ephemeral = !input.opencodeID
    let session: Session | undefined
    let connection: acp.ClientConnection | undefined
    try {
      for (let attempt = 0; ; attempt++) {
        connection = await this.connect()
        const attached = await this.session(connection, opencodeID)
        session = attached.session
        session.turn = turn
        turn.cancel = () => connection!.agent.notify("session/cancel", { sessionId: attached.session.acpID }).catch(() => {})
        try {
          await this.configure(connection, session, "model", input.model)
          if (input.effort) await this.configure(connection, session, "effort", input.effort)
          const response = await connection.agent.request("session/prompt", {
            sessionId: session.acpID,
            prompt: toPrompt(input.prompt, attached.fresh),
          })
          turn.finish(response)
          return
        } catch (error: any) {
          // The agent no longer knows the session, e.g. after it restarted. Reattach once, if nothing was shown yet.
          if (error?.code !== -32002 || turn.started || attempt > 0) throw error
          this.drop(opencodeID)
        }
      }
    } catch (error: any) {
      turn.fail(error instanceof Error ? error : new Error(String(error?.message ?? error)))
    } finally {
      if (ephemeral && connection && session) this.forget(connection, session)
    }
  }

  private followUp(prompt: LanguageModelV3Message[]) {
    const lastAssistant = prompt.findLastIndex((message) => message.role === "assistant")
    if (lastAssistant === -1) return false
    const after = prompt.slice(lastAssistant + 1)
    return (
      after.length > 0 &&
      after.every((message) => message.role === "tool") &&
      after.some((message) =>
        message.content.some((part) => part.type === "tool-result" && this.localCalls.has(part.toolCallId)),
      )
    )
  }

  private async session(connection: acp.ClientConnection, opencodeID: string) {
    const existing = this.sessions.get(opencodeID)
    if (existing) return { session: existing, fresh: false }

    // Reattach a Claude Code session from an earlier opencode run.
    const stored = await this.ctx.storage.get(storageKey(opencodeID)).catch(() => undefined)
    if (typeof stored === "string") {
      const resumed = await connection.agent
        .request("session/resume", { sessionId: stored, cwd: this.cwd, mcpServers: [] })
        .then((response) => this.track(opencodeID, stored, response))
        .catch(() => undefined)
      if (resumed) return { session: resumed, fresh: false }
    }

    const response = await connection.agent.request("session/new", { cwd: this.cwd, mcpServers: [] })
    const session = this.track(opencodeID, response.sessionId, response)
    if (this.options.mode) await this.configure(connection, session, "mode", this.options.mode)
    if (!opencodeID.startsWith("ephemeral-"))
      await this.ctx.storage.set(storageKey(opencodeID), response.sessionId).catch(() => {})
    return { session, fresh: true }
  }

  private track(opencodeID: string, acpID: string, response: { configOptions?: any[] | null }) {
    const config = Object.fromEntries(
      (response.configOptions ?? []).flatMap((option: any) =>
        typeof option.currentValue === "string" ? [[option.id, option.currentValue]] : [],
      ),
    )
    const session: Session = { acpID, opencodeID, cwd: this.cwd, config }
    this.sessions.set(opencodeID, session)
    this.byACP.set(acpID, session)
    return session
  }

  private forget(connection: acp.ClientConnection, session: Session) {
    this.drop(session.opencodeID)
    connection.agent.request("session/close", { sessionId: session.acpID }).catch(() => {})
  }

  private drop(opencodeID: string) {
    const session = this.sessions.get(opencodeID)
    if (!session) return
    this.sessions.delete(opencodeID)
    this.byACP.delete(session.acpID)
  }

  private async configure(connection: acp.ClientConnection, session: Session, configId: string, value: string) {
    if (session.config[configId] === value) return
    await connection.agent.request("session/set_config_option", { sessionId: session.acpID, configId, value })
    session.config[configId] = value
  }

  // Claude Code asks before a tool runs. Answer through opencode's rules and permission prompt.
  private async permission(params: acp.RequestPermissionRequest): Promise<acp.RequestPermissionResponse> {
    const session = this.byACP.get(params.sessionId)
    const pick = (...kinds: acp.PermissionOptionKind[]) => {
      const option = kinds.map((kind) => params.options.find((item) => item.kind === kind)).find(Boolean)
      return option
        ? { outcome: { outcome: "selected" as const, optionId: option.optionId } }
        : { outcome: { outcome: "cancelled" as const } }
    }
    if (!session) return pick("reject_once", "reject_always")
    session.turn?.call(params.toolCall)

    const tool = toolName(params.toolCall)
    const input = (params.toolCall.rawInput ?? {}) as Record<string, any>
    const checks = permissionChecks(tool, params.toolCall, input, session.cwd)
    const files = await previewDiff(tool, input, session.cwd).catch(() => undefined)
    try {
      for (const check of checks)
        await this.ctx.permission.assert({
          sessionID: session.opencodeID,
          action: check.action,
          resources: check.resources,
          metadata: { tool, title: params.toolCall.title, input, ...(files ? { files } : {}) },
        })
      return pick("allow_once")
    } catch (error: any) {
      // A rejection with feedback or a deny rule tells Claude Code no; a plain rejection also stops the turn,
      // as it would for opencode's own tools.
      const tagged = error?._tag === "Tool.Error" || error?.name === "Tool.Error"
      if (!tagged) session.turn?.cancel?.()
      return pick("reject_once", "reject_always")
    }
  }
}

/**
 * Translates one Claude Code prompt turn's ACP updates into AI SDK stream parts.
 *
 * opencode draws diffs only for edits it runs itself, and after running a tool it asks the model again. So a
 * step that reports Claude Code edits ends with "tool-calls"; the rest of the turn is held until opencode's next
 * request attaches to it. Only the turn's real end finishes with Claude Code's stop reason, so the TUI shows one
 * footer per turn.
 */
class Turn {
  cancel?: () => void
  /** Output has been produced, so the prompt can't be retried. */
  started = false
  /** The final finish (or error) has been delivered to opencode. */
  complete = false
  private sink?: { controller: ReadableStreamDefaultController<LanguageModelV3StreamPart>; release: () => void; signal?: AbortSignal }
  private readonly held: LanguageModelV3StreamPart[] = []
  private text?: { id: string; kind: "text" | "reasoning" }
  private readonly tools = new Map<string, TurnTool>()
  /** Tools reported in the current step. */
  private step: TurnTool[] = []
  private ended = false
  private readonly settled = Promise.withResolvers<void>()

  constructor(
    private readonly agent: Agent,
    private readonly cwd: string,
  ) {}

  /** Streams this turn into an opencode step. Resolves when the step ends. */
  attach(controller: ReadableStreamDefaultController<LanguageModelV3StreamPart>, signal?: AbortSignal) {
    const released = Promise.withResolvers<void>()
    const abort = () => this.cancel?.()
    signal?.addEventListener("abort", abort, { once: true })
    this.sink = {
      controller,
      signal,
      release: () => {
        signal?.removeEventListener("abort", abort)
        this.sink = undefined
        released.resolve()
      },
    }
    while (this.sink && this.held.length) this.deliver(this.held.shift()!)
    return released.promise
  }

  /** Stops this turn before a new prompt starts in its session. */
  async supersede() {
    this.cancel?.()
    await Promise.race([this.settled.promise, new Promise((resolve) => setTimeout(resolve, 10_000))])
  }

  private deliver(part: LanguageModelV3StreamPart) {
    const sink = this.sink!
    try {
      sink.controller.enqueue(part)
    } catch {}
    if (part.type !== "finish" && part.type !== "error") return
    if (this.ended && this.held.length === 0) this.complete = true
    sink.release()
  }

  private emit(part: LanguageModelV3StreamPart) {
    if (this.complete) return
    this.started = true
    if (this.sink && this.held.length === 0) this.deliver(part)
    else this.held.push(part)
  }

  /** Ends the current step once its edits are reported and no Claude Code tool in it is still running. */
  private breakStep() {
    if (!this.step.some((tool) => tool.local && tool.called)) return
    if (this.step.some((tool) => !tool.local && !tool.done)) return
    this.closeText()
    this.step = []
    this.emit({ type: "finish", finishReason: { unified: "tool-calls", raw: "tool-calls" }, usage: emptyUsage() })
  }

  private closeText() {
    if (!this.text) return
    this.emit({ type: this.text.kind === "text" ? "text-end" : "reasoning-end", id: this.text.id })
    this.text = undefined
  }

  private delta(kind: "text" | "reasoning", messageID: string | null | undefined, delta: string) {
    if (!delta) return
    const id = `${kind}-${messageID ?? "msg"}`
    if (this.text?.id !== id) {
      this.closeText()
      this.text = { id, kind }
      this.emit({ type: kind === "text" ? "text-start" : "reasoning-start", id })
    }
    this.emit({ type: kind === "text" ? "text-delta" : "reasoning-delta", id, delta })
  }

  update(update: acp.SessionUpdate) {
    switch (update.sessionUpdate) {
      case "agent_message_chunk":
        if (update.content.type === "text") this.delta("text", update.messageId, update.content.text)
        return
      case "agent_thought_chunk":
        if (update.content.type === "text") this.delta("reasoning", update.messageId, update.content.text)
        return
      case "tool_call":
      case "tool_call_update":
        return this.tool(update)
    }
  }

  private tool(update: acp.ToolCall | acp.ToolCallUpdate) {
    let tool = this.tools.get(update.toolCallId)
    if (!tool) {
      this.closeText()
      const name = toolName(update)
      const local = LOCAL_EDITS.has(name)
      tool = { name, display: local ? "edit" : displayName(name), local, input: {}, called: false, done: false }
      this.tools.set(update.toolCallId, tool)
      this.step.push(tool)
      this.emit({ type: "tool-input-start", id: update.toolCallId, toolName: tool.display, providerExecuted: !local })
    }
    if (update.rawInput && typeof update.rawInput === "object") tool.input = update.rawInput as Record<string, any>
    const response = (update._meta as any)?.claudeCode?.toolResponse
    if (response && typeof response.originalFile === "string") tool.original = response.originalFile
    if (update.status === "in_progress") this.call(update)
    if (update.status === "completed" || update.status === "failed") {
      this.call(update)
      if (tool.done) return
      tool.done = true
      if (tool.local) return void this.settleEdit(update.toolCallId, tool, update)
      this.emit({
        type: "tool-result",
        toolCallId: update.toolCallId,
        toolName: tool.display,
        result: resultText(update),
        isError: update.status === "failed",
      })
      this.breakStep()
    }
  }

  /** Reports the call with its final input. Claude Code streams the input before it asks or runs. */
  call(update: acp.ToolCall | acp.ToolCallUpdate) {
    if (!this.tools.has(update.toolCallId)) this.tool({ ...update, status: undefined })
    const tool = this.tools.get(update.toolCallId)!
    if (update.rawInput && typeof update.rawInput === "object") tool.input = update.rawInput as Record<string, any>
    if (tool.called) return
    tool.called = true
    if (tool.local) {
      // Registered before opencode sees the call, so the wrapped `edit` finds it.
      this.agent.edits.set(update.toolCallId, Promise.withResolvers())
      this.agent.localCalls.add(update.toolCallId)
      tool.file = typeof tool.input.file_path === "string" ? path.resolve(this.cwd, tool.input.file_path) : undefined
      tool.before = tool.file ? readText(tool.file) : Promise.resolve(undefined)
    }
    this.emit({
      type: "tool-call",
      toolCallId: update.toolCallId,
      toolName: tool.display,
      input: JSON.stringify(
        tool.local ? editInput(tool.input, this.cwd) : displayInput(tool.name, tool.input, update),
      ),
      providerExecuted: !tool.local,
    })
    if (tool.local) this.breakStep()
  }

  private async settleEdit(id: string, tool: TurnTool, update?: acp.ToolCall | acp.ToolCallUpdate) {
    const pending = this.agent.edits.get(id)
    if (!pending) return
    this.agent.edits.delete(id)
    if (!update || update.status === "failed") return pending.reject(new Error(update ? resultText(update) : "Interrupted"))
    const before = tool.original ?? (await tool.before) ?? ""
    const after = (tool.file && (await readText(tool.file))) ?? before
    const diff = fileDiff(relativePath(this.cwd, tool.file ?? "file"), before, after)
    pending.resolve({
      output: { files: [diff], replacements: 1 },
      content: resultText(update),
      metadata: { files: [diff] },
    })
  }

  finish(response: acp.PromptResponse) {
    this.closeText()
    // Settle tools Claude Code never reported an end for, such as those interrupted by a cancel.
    for (const [id, tool] of this.tools) {
      if (!tool.called) this.call({ toolCallId: id } as acp.ToolCallUpdate)
      if (tool.done) continue
      tool.done = true
      if (tool.local) void this.settleEdit(id, tool)
      else this.emit({ type: "tool-result", toolCallId: id, toolName: tool.display, result: "Interrupted", isError: true })
    }
    // Edits in this last step still need their own step, so opencode asks again before the turn ends.
    this.breakStep()
    const usage = (response as any).usage
    this.ended = true
    this.emit({
      type: "finish",
      finishReason: { unified: finishReason(response.stopReason), raw: response.stopReason },
      usage: usage
        ? {
            inputTokens: {
              total: (usage.inputTokens ?? 0) + (usage.cachedReadTokens ?? 0) + (usage.cachedWriteTokens ?? 0),
              noCache: usage.inputTokens,
              cacheRead: usage.cachedReadTokens,
              cacheWrite: usage.cachedWriteTokens,
            },
            outputTokens: { total: usage.outputTokens, text: undefined, reasoning: usage.thoughtTokens },
          }
        : emptyUsage(),
    })
    this.settled.resolve()
  }

  fail(error: Error) {
    for (const [id, tool] of this.tools) if (tool.local && !tool.done) void this.settleEdit(id, tool)
    this.ended = true
    this.emit({ type: "error", error })
    this.settled.resolve()
  }
}

type TurnTool = {
  /** Claude Code's tool name. */
  name: string
  /** The name opencode shows. */
  display: string
  /** Reported as a call opencode runs, through the wrapped `edit`. */
  local: boolean
  input: Record<string, any>
  called: boolean
  done: boolean
  file?: string
  before?: Promise<string | undefined>
  original?: string
}

// --- Prompt conversion -------------------------------------------------------------------------

/**
 * Claude Code keeps the conversation itself, so only the newest user input is sent. A fresh Claude Code
 * session (first turn, or after switching from another model) also gets the earlier transcript as context.
 */
function toPrompt(prompt: LanguageModelV3Message[], fresh: boolean): acp.ContentBlock[] {
  const lastAssistant = prompt.findLastIndex((message) => message.role === "assistant")
  const pending = prompt.slice(lastAssistant + 1).filter((message) => message.role === "user")
  const blocks: acp.ContentBlock[] = []

  if (fresh) {
    const history = transcript(prompt.slice(0, lastAssistant + 1))
    if (history)
      blocks.push({
        type: "text",
        text: `<previous_conversation>\nThis conversation started before you joined. Earlier turns:\n\n${history}\n</previous_conversation>`,
      })
  }

  for (const message of pending) {
    if (message.role !== "user") continue
    for (const part of message.content) {
      if (part.type === "text") blocks.push({ type: "text", text: part.text })
      if (part.type === "file") blocks.push(fileBlock(part))
    }
  }
  if (blocks.length === 0) blocks.push({ type: "text", text: "Continue." })
  return blocks
}

function transcript(messages: LanguageModelV3Message[]) {
  return messages
    .flatMap((message) => {
      if (message.role === "system") return []
      if (typeof message.content === "string") return [`${message.role}: ${message.content}`]
      const text = message.content
        .map((part: any) => {
          if (part.type === "text") return part.text
          if (part.type === "tool-call") return `[tool ${part.toolName}: ${JSON.stringify(part.input).slice(0, 500)}]`
          if (part.type === "tool-result") return `[tool result: ${JSON.stringify(part.output).slice(0, 500)}]`
          if (part.type === "file") return `[file ${part.filename ?? part.mediaType}]`
          return ""
        })
        .filter(Boolean)
        .join("\n")
      return text ? [`${message.role}: ${text}`] : []
    })
    .join("\n\n")
}

function fileBlock(part: Extract<Extract<LanguageModelV3Message, { role: "user" }>["content"][number], { type: "file" }>) {
  const data = part.data
  if (data instanceof URL) {
    if (data.protocol !== "data:")
      return { type: "resource_link", uri: data.href, name: part.filename ?? data.href, mimeType: part.mediaType } as const
  }
  const base64 =
    data instanceof URL
      ? data.href.slice(data.href.indexOf(",") + 1)
      : typeof data === "string"
        ? data
        : Buffer.from(data).toString("base64")
  if (part.mediaType.startsWith("image/")) return { type: "image", data: base64, mimeType: part.mediaType } as const
  const uri = `file://${part.filename ?? "attachment"}`
  if (part.mediaType.startsWith("text/") || part.mediaType === "application/json")
    return {
      type: "resource",
      resource: { uri, mimeType: part.mediaType, text: Buffer.from(base64, "base64").toString("utf8") },
    } as const
  return { type: "resource", resource: { uri, mimeType: part.mediaType, blob: base64 } } as const
}

// --- Tool display ------------------------------------------------------------------------------

function toolName(update: acp.ToolCall | acp.ToolCallUpdate) {
  const meta = (update._meta as any)?.claudeCode?.toolName
  return String(meta ?? update.name ?? update.title ?? "tool")
}

// opencode's TUI has dedicated views for its own tools; show Claude Code's equivalents in them.
const DISPLAY: Record<string, string> = {
  Bash: "bash",
  Read: "read",
  Write: "write",
  Edit: "edit",
  MultiEdit: "edit",
  NotebookEdit: "edit",
  Glob: "glob",
  Grep: "grep",
  WebFetch: "webfetch",
  WebSearch: "websearch",
  TodoWrite: "todowrite",
}

function displayName(name: string) {
  return DISPLAY[name] ?? name
}

function displayInput(name: string, input: Record<string, any>, update: acp.ToolCall | acp.ToolCallUpdate) {
  const file = input.file_path ?? input.notebook_path ?? update.locations?.[0]?.path
  switch (name) {
    case "Bash":
      return { command: input.command, description: input.description }
    case "Read":
      return { path: file, offset: input.offset, limit: input.limit }
    case "Write":
      return { path: file, content: input.content }
    case "Edit":
    case "MultiEdit":
    case "NotebookEdit":
      return { path: file, ...input }
    case "Glob":
    case "Grep":
      return { ...input, pattern: input.pattern, path: input.path }
    case "TodoWrite":
      return {
        todos: (input.todos ?? []).map((todo: any) => ({ content: todo.content, status: todo.status, priority: "medium" })),
      }
    default:
      return input
  }
}

/** Input for opencode's `edit` schema: path, oldString, newString, replaceAll. */
function editInput(input: Record<string, any>, cwd: string) {
  const edits: any[] = Array.isArray(input.edits) ? input.edits : [input]
  return {
    path: relativePath(cwd, path.resolve(cwd, String(input.file_path ?? ""))),
    oldString: edits.map((edit) => String(edit.old_string ?? "")).join("\n…\n"),
    newString: edits.map((edit) => String(edit.new_string ?? "")).join("\n…\n"),
    replaceAll: edits.some((edit) => edit.replace_all === true),
  }
}

/** The change a Write, Edit or MultiEdit would make, for the permission prompt's diff. */
async function previewDiff(tool: string, input: Record<string, any>, cwd: string) {
  if (typeof input.file_path !== "string") return
  const file = path.resolve(cwd, input.file_path)
  const before = await readText(file)
  const replace = (text: string, edit: any) =>
    edit.replace_all
      ? text.replaceAll(String(edit.old_string), String(edit.new_string))
      : text.replace(String(edit.old_string), () => String(edit.new_string))
  let after: string
  if (tool === "Write" && typeof input.content === "string") after = input.content
  else if (tool === "Edit" && before !== undefined) after = replace(before, input)
  else if (tool === "MultiEdit" && before !== undefined && Array.isArray(input.edits))
    after = input.edits.reduce(replace, before)
  else return
  return [fileDiff(relativePath(cwd, file), before ?? "", after, before === undefined ? "added" : "modified")]
}

function fileDiff(file: string, before: string, after: string, status: FileDiff["status"] = "modified"): FileDiff {
  const counts = diffLines(before, after).reduce(
    (result, item) => ({
      additions: result.additions + (item.added ? (item.count ?? 0) : 0),
      deletions: result.deletions + (item.removed ? (item.count ?? 0) : 0),
    }),
    { additions: 0, deletions: 0 },
  )
  return { file, patch: createTwoFilesPatch(file, file, before, after), status, ...counts }
}

function readText(file: string) {
  return readFile(file, "utf8").catch(() => undefined)
}

function relativePath(cwd: string, file: string) {
  const relative = path.relative(cwd, file)
  return relative.startsWith("..") ? file : relative
}

function resultText(update: acp.ToolCall | acp.ToolCallUpdate): string {
  const text = (update.content ?? [])
    .map((item) => {
      if (item.type === "content" && item.content.type === "text") return item.content.text
      if (item.type === "diff") return `${item.path}\n${item.newText}`
      return ""
    })
    .filter(Boolean)
    .join("\n")
  if (text) return text
  if (typeof update.rawOutput === "string") return update.rawOutput
  if (update.rawOutput !== undefined) return JSON.stringify(update.rawOutput)
  return update.status === "failed" ? "Failed" : "Done"
}

// --- Permissions -------------------------------------------------------------------------------

/** Maps a Claude Code permission request onto opencode permission actions, so the user's rules apply. */
function permissionChecks(tool: string, update: acp.ToolCallUpdate, input: Record<string, any>, cwd: string) {
  const paths = [
    ...(update.locations ?? []).map((location) => location.path),
    ...[input.file_path, input.notebook_path, input.path].filter((value) => typeof value === "string"),
  ].map((file) => path.resolve(cwd, file))
  const unique = [...new Set(paths)]
  const outside = unique.filter((file) => path.relative(cwd, file).startsWith(".."))
  const relative = unique.map((file) => (outside.includes(file) ? file : path.relative(cwd, file)))

  const checks: { action: string; resources: string[] }[] = []
  if (outside.length) checks.push({ action: "external_directory", resources: outside.map((file) => path.dirname(file)) })
  switch (update.kind) {
    case "execute":
      checks.push({ action: "shell", resources: [String(input.command ?? update.title ?? "")] })
      break
    case "edit":
    case "delete":
    case "move":
      checks.push({ action: "edit", resources: relative.length ? relative : ["*"] })
      break
    case "read":
      checks.push({ action: "read", resources: relative.length ? relative : ["*"] })
      break
    case "fetch":
      if (tool === "WebSearch") checks.push({ action: "websearch", resources: [String(input.query ?? "*")] })
      else checks.push({ action: "webfetch", resources: [String(input.url ?? "*")] })
      break
    default:
      checks.push({ action: tool.toLowerCase(), resources: relative.length ? relative : ["*"] })
  }
  return checks
}

// --- Helpers -----------------------------------------------------------------------------------

function findEffort(settings: Record<string, any>, providerOptions: LanguageModelV3CallOptions["providerOptions"]) {
  const values = [settings, ...Object.values(providerOptions ?? {})]
  const effort = values.map((value: any) => value?.effort).find((value) => typeof value === "string")
  return effort as string | undefined
}

function finishReason(reason: acp.StopReason) {
  if (reason === "max_tokens") return "length" as const
  if (reason === "refusal") return "content-filter" as const
  return "stop" as const
}

function emptyUsage(): LanguageModelV3Usage {
  return {
    inputTokens: { total: undefined, noCache: undefined, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: undefined, text: undefined, reasoning: undefined },
  }
}

function storageKey(opencodeID: string) {
  return `session/${opencodeID}`
}
