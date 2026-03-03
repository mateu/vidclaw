import fs from 'fs';
import { execFile } from 'child_process';

const AGENT_TIMEOUT_MS = 15 * 60 * 1000;

function resolveOpenclawBin() {
  const fromEnv = process.env.OPENCLAW_BIN;
  if (fromEnv) return fromEnv;

  const candidates = [
    '/home/hunter/openclaw-app/node_modules/.bin/openclaw',
    '/home/linuxbrew/.linuxbrew/bin/openclaw',
    '/usr/local/bin/openclaw',
    '/usr/bin/openclaw',
  ];

  for (const candidate of candidates) {
    try {
      if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
        return candidate;
      }
    } catch {
      // continue
    }
  }

  return 'openclaw'; // PATH fallback
}

const OPENCLAW_BIN = resolveOpenclawBin();

let deps = null;
let dispatchScheduled = false;
let dispatchRunning = false;

export function isTaskRunnable(task, now = new Date()) {
  if (!task) return false;
  if (task.status === 'in-progress' && !task.pickedUp) return true;
  if (task.status !== 'todo') return false;
  if (task.schedule && task.scheduleEnabled === false) return false;
  if (!task.schedule) return true;
  if (task.schedule === 'asap' || task.schedule === 'next-heartbeat') return true;
  if (task.scheduledAt) return new Date(task.scheduledAt) <= now;
  if (task.schedule !== 'asap' && task.schedule !== 'next-heartbeat') {
    return new Date(task.schedule) <= now;
  }
  return true;
}

function sortRunnableTasks(a, b) {
  const oa = a.order ?? 999999;
  const ob = b.order ?? 999999;
  if (oa !== ob) return oa - ob;
  return (a.createdAt || '').localeCompare(b.createdAt || '');
}

function makeRunPrompt(task) {
  const skillLine = Array.isArray(task.skills) && task.skills.length
    ? `Skills to use if relevant: ${task.skills.join(', ')}`
    : (task.skill ? `Skill hint: ${task.skill}` : null);
  return [
    'Execute this VidClaw task now and return a concise completion summary.',
    `Task title: ${task.title || 'Untitled'}`,
    task.description ? `Task description:\n${task.description}` : null,
    skillLine,
    'Return only the result summary (no markdown headers).',
  ].filter(Boolean).join('\n\n');
}

export function parseAgentResult(stdout, stderr) {
  const out = (stdout || '').trim();
  if (!out) return (stderr || '').trim() || 'Agent completed with no output.';
  try {
    const parsed = JSON.parse(out);
    if (typeof parsed === 'string') return parsed;

    // Preferred: OpenClaw run envelope payload text
    const payloadText = parsed?.result?.payloads?.find?.(p => typeof p?.text === 'string')?.text;
    if (typeof payloadText === 'string' && payloadText.trim()) return payloadText.trim();

    // Common alternates
    if (typeof parsed?.reply === 'string') return parsed.reply;
    if (typeof parsed?.reply?.text === 'string') return parsed.reply.text;
    if (typeof parsed?.text === 'string') return parsed.text;
    if (typeof parsed?.summary === 'string') return parsed.summary;
  } catch {
    // fall through
  }
  return out;
}

function runTaskAgent(task, callback) {
  const prompt = makeRunPrompt(task);
  execFile(OPENCLAW_BIN, ['agent', '--agent', 'main', '--json', '--message', prompt], {
    timeout: AGENT_TIMEOUT_MS,
    maxBuffer: 2 * 1024 * 1024,
  }, (err, stdout, stderr) => {
    if (err) {
      const detail = stderr?.trim() || stdout?.trim() || err.message;
      callback(new Error(detail));
      return;
    }
    callback(null, parseAgentResult(stdout, stderr));
  });
}

function applyTransition(task, toState, meta = {}) {
  if (typeof deps.recordExecutionTransition === 'function') {
    deps.recordExecutionTransition(task, toState, meta);
    return;
  }
  if (!Array.isArray(task.executionTransitions)) task.executionTransitions = [];
  const fromState = task.executionState || null;
  const at = meta.at || new Date().toISOString();
  task.executionTransitions.push({
    at,
    from: fromState,
    to: toState,
    reason: meta.reason || null,
    message: meta.message || null,
    actor: meta.actor || 'system',
    subagentId: meta.subagentId || task.subagentId || null,
  });
  task.executionState = toState;
  task.updatedAt = at;
}

function completeRun(taskId, runId, result, error) {
  const { readTasks, writeTasks, broadcast, logActivity, computeNextRun } = deps;
  const tasks = readTasks();
  const idx = tasks.findIndex(t => t.id === taskId);
  if (idx === -1) return;

  const task = tasks[idx];
  if (task.subagentId !== runId) return;

  const now = new Date().toISOString();
  const hasError = !!error;
  const message = hasError ? error : (result || null);

  if (task.schedule && task.scheduleEnabled !== false) {
    if (!Array.isArray(task.runHistory)) task.runHistory = [];
    task.runHistory.push({
      completedAt: now,
      startedAt: task.startedAt,
      updatedAt: now,
      status: hasError ? 'failed' : 'completed',
      reason: hasError ? 'task_error' : 'task_completed',
      message,
      result: hasError ? null : (result || null),
      error: hasError ? (error || null) : null,
      subagentId: runId,
      sessionId: runId,
    });
    applyTransition(task, hasError ? 'failed' : 'completed', {
      at: now,
      actor: 'system',
      reason: hasError ? 'task_error' : 'task_completed',
      message,
      subagentId: runId,
    });
    task.status = 'todo';
    task.scheduledAt = computeNextRun(task.schedule);
    task.result = hasError ? null : (result || null);
    task.error = hasError ? (error || null) : null;
    task.startedAt = null;
    task.completedAt = null;
  } else {
    task.status = 'done';
    task.completedAt = now;
    task.result = hasError ? null : (result || null);
    task.error = hasError ? (error || null) : null;
    applyTransition(task, hasError ? 'failed' : 'completed', {
      at: now,
      actor: 'system',
      reason: hasError ? 'task_error' : 'task_completed',
      message,
      subagentId: runId,
    });
  }

  task.subagentId = null;
  task.pickedUp = false;
  task.updatedAt = now;

  writeTasks(tasks);
  logActivity('system', hasError ? 'task_failed' : 'task_completed', {
    taskId,
    title: task.title,
    subagentId: runId,
    error: hasError ? (error || null) : null,
  });
  broadcast('tasks', tasks.filter(t => t.status !== 'archived'));
}

function startTaskRun(taskId) {
  const { readTasks, writeTasks, broadcast, logActivity } = deps;
  const tasks = readTasks();
  const idx = tasks.findIndex(t => t.id === taskId);
  if (idx === -1) return false;

  const task = tasks[idx];
  if (!(task.status === 'in-progress' && !task.pickedUp) && task.status !== 'todo') return false;

  const now = new Date().toISOString();
  const runId = `dispatcher-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;

  task.status = 'in-progress';
  task.startedAt = task.startedAt || now;
  task.pickedUp = true;
  task.subagentId = runId;
  applyTransition(task, 'in-progress', {
    at: now,
    actor: 'system',
    reason: 'dispatcher_pickup',
    message: 'Task picked up by server dispatcher',
    subagentId: runId,
  });

  writeTasks(tasks);
  logActivity('system', 'task_pickup', { taskId: task.id, title: task.title, subagentId: runId, source: 'dispatcher' });
  broadcast('tasks', tasks.filter(t => t.status !== 'archived'));

  runTaskAgent(task, (err, result) => {
    completeRun(task.id, runId, result || null, err ? err.message : null);
    requestTaskDispatch('runner_complete');
  });

  return true;
}

export function selectRunnableTasks(tasks, { maxConcurrent = 1, now = new Date() } = {}) {
  const activeCount = tasks.filter(t => t.status === 'in-progress' && t.pickedUp).length;
  const remainingSlots = Math.max(0, maxConcurrent - activeCount);
  if (remainingSlots <= 0) return [];
  return tasks
    .filter(t => isTaskRunnable(t, now))
    .sort(sortRunnableTasks)
    .slice(0, remainingSlots);
}

function dispatchOnce() {
  if (!deps) return;
  const { readTasks, readSettings } = deps;
  const tasks = readTasks();
  const settings = readSettings();
  const maxConcurrent = settings.maxConcurrent || 1;

  const runnable = selectRunnableTasks(tasks, { maxConcurrent, now: new Date() });

  for (const task of runnable) {
    startTaskRun(task.id);
  }
}

function runDispatchLoop() {
  if (dispatchRunning) return;
  dispatchRunning = true;
  try {
    dispatchOnce();
  } finally {
    dispatchRunning = false;
    dispatchScheduled = false;
  }
}

export function configureTaskDispatcher(options) {
  deps = options;
}

export function requestTaskDispatch() {
  if (!deps || dispatchScheduled) return;
  dispatchScheduled = true;
  setImmediate(runDispatchLoop);
}
