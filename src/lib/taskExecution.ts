import type { Task, ExecutionState } from '@/types/api'

const TERMINAL_STATES: ExecutionState[] = ['completed', 'failed', 'canceled', 'timeout']
const ERROR_TERMINAL_STATES: ExecutionState[] = ['failed', 'canceled', 'timeout']

export function getCanonicalTerminalState(task: Task): ExecutionState | null {
  if (task.executionState && TERMINAL_STATES.includes(task.executionState)) return task.executionState
  const transitions = task.executionTransitions || []
  for (let i = transitions.length - 1; i >= 0; i -= 1) {
    const to = transitions[i]?.to
    if (to && TERMINAL_STATES.includes(to)) return to
  }
  return null
}

export function getTaskDisplayState(task: Task): ExecutionState | null {
  const terminal = getCanonicalTerminalState(task)
  if (terminal) return terminal
  if (task.executionState) return task.executionState
  if (task.status === 'in-progress') return 'in-progress'
  if (task.status === 'done') return task.error ? 'failed' : 'completed'
  return null
}

export function isErrorState(state: ExecutionState | null): boolean {
  return !!state && ERROR_TERMINAL_STATES.includes(state)
}

export function getDisplayMessage(task: Task, state: ExecutionState | null): string | null {
  if (isErrorState(state)) {
    return task.error || (task.executionTransitions || []).slice().reverse().find(t => t.to === state)?.message || null
  }
  return task.result || null
}
