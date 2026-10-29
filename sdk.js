// Provider package stub. opencode loads a provider's AI SDK package before plugin hooks run; the real
// language model comes from the plugin's `language` hook in index.ts, which needs the running ACP agent.
export function createClaudeCode() {
  return {
    languageModel(id) {
      throw new Error(`claude-code/${id}: the claude-code-acp plugin did not provide a language model`)
    },
  }
}
