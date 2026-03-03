import fs from 'fs';
import path from 'path';
import { readTasks, writeTasks, logActivity, readSettings } from '../lib/fileStore.js';
import { broadcast } from '../broadcast.js';
import { isoToDateInTz } from '../lib/timezone.js';
import { WORKSPACE, __dirname } from '../config.js';
import { computeNextRun, computeFutureRuns } from '../lib/schedule.js';
import { recoverStaleTasks } from '../lib/taskRecovery.js';
import { requestTaskDispatch } from '../lib/taskDispatcher.js';
import { getKnownChannelIds } from './channels.js';

export function listTasks(req, res) {
  let tasks = readTasks();
  for (const task of tasks) normalizeExecutionFields(task);
  const includeArchived = req.query.includeArchived === 'true';
  if (!includeArchived) tasks = tasks.filter(t => t.status !== 'archived');
  // Filter by channel if ?channel= query param is provided
  if (req.query.channel !== undefined) {
    const ch = req.query.channel || null;
    tasks = tasks.filter(t => (t.channel || null) === ch);
  }
  res.json(tasks);
}

/** Validate channel value against known channel IDs. Returns error string or null. */
function validateChannel(channel) {
  if (channel === null || channel === undefined || channel === '') return null;
  const known = getKnownChannelIds();
  if (!known.includes(channel)) return `Unknown channel "${channel}". Valid: ${known.join(', ')}`;
  return null;
}

function recoverTasksIfNeeded(tasks) {
  const recovery = recoverStaleTasks(tasks, { logActivity, recordExecutionTransition });
  if (!recovery.changed) return recovery;
  writeTasks(tasks);
  broadcast('tasks', tasks.filter(t => t.status !== 'archived'));
  return recovery;
}

const EXECUTION_STATES = ['queued', 'in-progress', 'completed', 'failed', 'canceled', 'timeout'];
const TERMINAL_EXECUTION_STATES = new Set(['completed', 'failed', 'canceled', 'timeout']);
const BLOCKING_TERMINAL_STATES = new Set(['failed', 'canceled', 'timeout']);

function normalizeExecutionFields(task) {
  if (!Array.isArray(task.executionTransitions)) task.executionTransitions = [];
  if (task.executionState && !EXECUTION_STATES.includes(task.executionState)) task.executionState = null;
  if (task.executionState === undefined) task.executionState = null;
}

function getCanonicalTerminalState(task) {
  normalizeExecutionFields(task);
  if (task.executionState && TERMINAL_EXECUTION_STATES.has(task.executionState)) return task.executionState;
  for (let i = task.executionTransitions.length - 1; i >= 0; i -= 1) {
    const to = task.executionTransitions[i]?.to;
    if (to && TERMINAL_EXECUTION_STATES.has(to)) return to;
  }
  return null;
}

function shouldIgnoreCompletion(task) {
  const terminal = getCanonicalTerminalState(task);
  return !!terminal && BLOCKING_TERMINAL_STATES.has(terminal);
}

function recordExecutionTransition(task, toState, meta = {}) {
  if (!EXECUTION_STATES.includes(toState)) return;
  normalizeExecutionFields(task);
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

export function createTask(req, res) {
  const channelErr = validateChannel(req.body.channel);
  if (channelErr) return res.status(400).json({ error: channelErr });
  const tasks = readTasks();
  const task = {
    id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
    title: req.body.title || 'Untitled',
    description: req.body.description || '',
    priority: req.body.priority || 'medium',
    skill: req.body.skill || '',
    skills: Array.isArray(req.body.skills) ? req.body.skills : (req.body.skill ? [req.body.skill] : []),
    status: req.body.schedule ? 'todo' : (req.body.status || 'backlog'),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    completedAt: null,
    schedule: req.body.schedule || null,
    scheduledAt: req.body.schedule ? (req.body.scheduledAt || computeNextRun(req.body.schedule)) : (req.body.scheduledAt || null),
    scheduleEnabled: req.body.schedule ? true : false,
    runHistory: [],
    result: null,
    startedAt: null,
    error: null,
    channel: req.body.channel || null,
    order: req.body.order ?? tasks.filter(t => t.status === (req.body.status || 'backlog')).length,
    source: req.body.source || null,
    sourceMessageId: req.body.sourceMessageId || null,
    executionState: null,
    executionTransitions: [],
  };
  tasks.push(task);
  writeTasks(tasks);
  logActivity('user', 'task_created', { taskId: task.id, title: task.title });
  broadcast('tasks', tasks);
  res.json(task);
}

export function createTaskFromConversation(req, res) {
  const tasks = readTasks();
  const now = new Date().toISOString();
  const autoStart = req.body.autoStart === true;
  const task = {
    id: Date.now().toString(36) + Math.random().toString(36).slice(2, 6),
    title: req.body.title || 'Untitled',
    description: req.body.description || '',
    priority: req.body.priority || 'medium',
    skill: '',
    skills: [],
    status: autoStart ? 'in-progress' : 'backlog',
    createdAt: now,
    updatedAt: now,
    completedAt: null,
    schedule: null,
    scheduledAt: null,
    scheduleEnabled: false,
    runHistory: [],
    result: null,
    startedAt: autoStart ? now : null,
    error: null,
    order: tasks.filter(t => t.status === (autoStart ? 'in-progress' : 'backlog')).length,
    source: req.body.source || null,
    sourceMessageId: req.body.sourceMessageId || null,
    subagentId: req.body.subagentId || null,
    pickedUp: autoStart ? true : false,
    executionState: autoStart ? 'in-progress' : null,
    executionTransitions: autoStart ? [{
      at: now,
      from: null,
      to: 'in-progress',
      reason: 'auto_start',
      message: 'Task auto-started from conversation',
      actor: 'bot',
      subagentId: req.body.subagentId || null,
    }] : [],
  };
  tasks.push(task);
  writeTasks(tasks);
  logActivity('bot', 'task_created', { taskId: task.id, title: task.title, source: task.source });
  broadcast('tasks', tasks);
  res.json(task);
}

export function updateTask(req, res) {
  if (req.body.channel !== undefined) {
    const channelErr = validateChannel(req.body.channel);
    if (channelErr) return res.status(400).json({ error: channelErr });
  }
  const tasks = readTasks();
  const idx = tasks.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  const wasNotDone = tasks[idx].status !== 'done';
  const allowedFields = ['title', 'description', 'priority', 'skill', 'skills', 'status', 'schedule', 'scheduledAt', 'scheduleEnabled', 'result', 'startedAt', 'completedAt', 'error', 'order', 'subagentId', 'channel', 'source', 'sourceMessageId'];
  const updates = {};
  for (const k of allowedFields) { if (req.body[k] !== undefined) updates[k] = req.body[k]; }
  tasks[idx] = { ...tasks[idx], ...updates, updatedAt: new Date().toISOString() };
  // Recompute scheduledAt when schedule changes
  if (updates.schedule !== undefined) {
    if (updates.schedule) {
      tasks[idx].scheduledAt = computeNextRun(updates.schedule);
      tasks[idx].scheduleEnabled = true;
    } else {
      tasks[idx].scheduledAt = null;
      tasks[idx].scheduleEnabled = false;
    }
  }
  if (wasNotDone && tasks[idx].status === 'done') tasks[idx].completedAt = new Date().toISOString();
  if (tasks[idx].status !== 'done') tasks[idx].completedAt = null;
  writeTasks(tasks);
  const actor = req.body._actor || 'user';
  logActivity(actor, 'task_updated', {
    taskId: req.params.id, title: tasks[idx].title, changes: Object.keys(updates),
    ...(updates.status && { newStatus: updates.status }),
    ...(updates.priority && { newPriority: updates.priority }),
  });
  broadcast('tasks', tasks);
  res.json(tasks[idx]);
}

export function reorderTasks(req, res) {
  const { status, order } = req.body;
  if (!status || !Array.isArray(order)) return res.status(400).json({ error: 'status and order[] required' });
  const tasks = readTasks();
  for (let i = 0; i < order.length; i++) {
    const idx = tasks.findIndex(t => t.id === order[i]);
    if (idx !== -1) tasks[idx].order = i;
  }
  writeTasks(tasks);
  broadcast('tasks', tasks);
  res.json({ ok: true });
}

export function runTask(req, res) {
  const tasks = readTasks();
  const idx = tasks.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  const now = new Date().toISOString();
  tasks[idx].status = 'in-progress';
  tasks[idx].startedAt = now;
  recordExecutionTransition(tasks[idx], 'queued', { at: now, actor: 'user', reason: 'run_requested', message: 'Task queued for execution' });
  writeTasks(tasks);
  logActivity('user', 'task_run', { taskId: req.params.id, title: tasks[idx].title });
  broadcast('tasks', tasks);
  requestTaskDispatch();
  res.json({ success: true, message: 'Task queued; dispatcher triggered' });
}

export function getTaskQueue(req, res) {
  const tasks = readTasks();
  const recovery = recoverTasksIfNeeded(tasks);
  const now = new Date();
  const queue = tasks.filter(t => {
    if (t.status === 'in-progress' && !t.pickedUp) return true;
    if (t.status !== 'todo') return false;
    // Paused recurring tasks shouldn't enter queue
    if (t.schedule && t.scheduleEnabled === false) return false;
    if (!t.schedule) return true;
    if (t.schedule === 'asap' || t.schedule === 'next-heartbeat') return true;
    // Check scheduledAt for recurring tasks
    if (t.scheduledAt) return new Date(t.scheduledAt) <= now;
    if (t.schedule !== 'asap' && t.schedule !== 'next-heartbeat') {
      return new Date(t.schedule) <= now;
    }
    return true;
  });
  queue.sort((a, b) => {
    const oa = a.order ?? 999999;
    const ob = b.order ?? 999999;
    if (oa !== ob) return oa - ob;
    return (a.createdAt || '').localeCompare(b.createdAt || '');
  });

  const settings = readSettings();
  const maxConcurrent = settings.maxConcurrent || 1;
  const activeCount = tasks.filter(t => t.status === 'in-progress' && t.pickedUp).length;
  const remainingSlots = Math.max(0, maxConcurrent - activeCount);

  const staleCount = queue.filter(t => t.scheduledAt && new Date(t.scheduledAt) <= now).length;
  const limitedQueue = req.query.limit === 'capacity' ? queue.slice(0, remainingSlots) : queue;
  res.json({
    tasks: limitedQueue,
    maxConcurrent,
    activeCount,
    remainingSlots,
    staleCount,
    recoveredOrphans: recovery.recoveredOrphans || 0,
    recoveredStale: recovery.recoveredStale || 0,
  });
}

export function pickupTask(req, res) {
  const tasks = readTasks();
  recoverTasksIfNeeded(tasks);
  const idx = tasks.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });

  if (tasks[idx].status === 'in-progress' && tasks[idx].pickedUp) {
    return res.status(409).json({ error: 'Task is already picked up by an active worker' });
  }

  const now = new Date().toISOString();
  tasks[idx].pickedUp = true;
  tasks[idx].status = 'in-progress';
  tasks[idx].startedAt = tasks[idx].startedAt || now;
  if (req.body.subagentId) tasks[idx].subagentId = req.body.subagentId;
  recordExecutionTransition(tasks[idx], 'in-progress', {
    at: now,
    actor: 'bot',
    reason: 'picked_up',
    message: 'Worker picked up task',
    subagentId: req.body.subagentId || tasks[idx].subagentId || null,
  });
  writeTasks(tasks);
  logActivity('bot', 'task_pickup', { taskId: req.params.id, title: tasks[idx].title, subagentId: req.body.subagentId || null });
  broadcast('tasks', tasks);
  res.json(tasks[idx]);
}

export function completeTask(req, res) {
  const tasks = readTasks();
  const idx = tasks.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  const now = new Date().toISOString();
  const hasError = !!req.body.error;

  if (!hasError && shouldIgnoreCompletion(tasks[idx])) {
    const terminalState = getCanonicalTerminalState(tasks[idx]);
    recordExecutionTransition(tasks[idx], terminalState, {
      at: now,
      actor: 'bot',
      reason: 'stale_completion_ignored',
      message: 'Ignored late completion after terminal state: ' + terminalState,
    });
    writeTasks(tasks);
    logActivity('bot', 'task_completed', {
      taskId: req.params.id,
      title: tasks[idx].title,
      hasError: false,
      result: null,
      error: null,
      ignored: true,
      ignoredReason: 'stale_completion_after_' + terminalState,
    });
    broadcast('tasks', tasks);
    return res.json(tasks[idx]);
  }

  // If recurring, save run to history and reschedule instead of marking done
  if (tasks[idx].schedule && tasks[idx].scheduleEnabled !== false) {
    if (!Array.isArray(tasks[idx].runHistory)) tasks[idx].runHistory = [];
    tasks[idx].runHistory.push({
      completedAt: now,
      startedAt: tasks[idx].startedAt,
      updatedAt: now,
      status: hasError ? 'failed' : 'completed',
      reason: hasError ? 'task_error' : 'task_completed',
      message: req.body.error || req.body.result || null,
      result: req.body.result || null,
      error: req.body.error || null,
      subagentId: tasks[idx].subagentId || null,
      sessionId: tasks[idx].subagentId || null,
    });
    recordExecutionTransition(tasks[idx], hasError ? 'failed' : 'completed', {
      at: now,
      actor: 'bot',
      reason: hasError ? 'task_error' : 'task_completed',
      message: req.body.error || req.body.result || null,
    });
    tasks[idx].status = 'todo';
    tasks[idx].scheduledAt = computeNextRun(tasks[idx].schedule);
    tasks[idx].result = hasError ? null : (req.body.result || null);
    tasks[idx].error = hasError ? (req.body.error || null) : null;
    tasks[idx].startedAt = null;
    tasks[idx].completedAt = null;
    tasks[idx].subagentId = null;
    tasks[idx].pickedUp = false;
    tasks[idx].updatedAt = now;
  } else {
    tasks[idx].status = 'done';
    tasks[idx].completedAt = now;
    tasks[idx].result = hasError ? null : (req.body.result || null);
    tasks[idx].error = hasError ? (req.body.error || null) : null;
    tasks[idx].subagentId = null;
    tasks[idx].pickedUp = false;
    recordExecutionTransition(tasks[idx], hasError ? 'failed' : 'completed', {
      at: now,
      actor: 'bot',
      reason: hasError ? 'task_error' : 'task_completed',
      message: req.body.error || req.body.result || null,
    });
  }
  writeTasks(tasks);
  const resultSnippet = (req.body.result || '').slice(0, 500) || null;
  const errorSnippet = (req.body.error || '').slice(0, 500) || null;
  logActivity('bot', 'task_completed', { taskId: req.params.id, title: tasks[idx].title, hasError, result: resultSnippet, error: errorSnippet });
  broadcast('tasks', tasks);
  requestTaskDispatch();
  res.json(tasks[idx]);
}

export function cancelTask(req, res) {
  const tasks = readTasks();
  const idx = tasks.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Task not found' });

  const task = tasks[idx];
  if (task.status !== 'in-progress' && !task.pickedUp && !task.subagentId) {
    return res.status(400).json({ error: 'Task is not currently running' });
  }

  const now = new Date().toISOString();
  if (!Array.isArray(task.runHistory)) task.runHistory = [];
  task.runHistory.push({
    completedAt: now,
    startedAt: task.startedAt || null,
    updatedAt: now,
    status: 'canceled',
    reason: 'user_canceled',
    message: 'Cancelled by user',
    result: null,
    error: 'Cancelled by user',
    subagentId: task.subagentId || null,
    sessionId: task.subagentId || null,
  });

  recordExecutionTransition(task, 'canceled', {
    at: now,
    actor: 'user',
    reason: 'user_canceled',
    message: 'Cancelled by user',
  });
  task.status = task.schedule ? 'todo' : 'backlog';
  if (task.schedule && task.scheduleEnabled !== false) {
    task.scheduledAt = computeNextRun(task.schedule);
  }
  task.error = 'Cancelled by user';
  task.result = null;
  task.startedAt = null;
  task.completedAt = null;
  task.subagentId = null;
  task.pickedUp = false;

  writeTasks(tasks);
  logActivity('user', 'task_cancelled', { taskId: task.id, title: task.title });
  broadcast('tasks', tasks);
  requestTaskDispatch();
  res.json(task);
}

export function bulkDeleteTasks(req, res) {
  const tasks = readTasks();
  const { status, ids } = req.body;
  let targets;
  if (Array.isArray(ids) && ids.length) {
    targets = tasks.filter(t => ids.includes(t.id) && t.status !== 'archived');
  } else if (status) {
    targets = tasks.filter(t => t.status === status);
  } else {
    return res.status(400).json({ error: 'Provide status or ids[]' });
  }
  const now = new Date().toISOString();
  for (const t of targets) {
    t.previousStatus = t.status;
    t.status = 'archived';
    t.archivedAt = now;
    t.updatedAt = now;
  }
  writeTasks(tasks);
  for (const t of targets) {
    logActivity('user', 'task_archived', { taskId: t.id, title: t.title });
  }
  broadcast('tasks', tasks.filter(t => t.status !== 'archived'));
  res.json({ ok: true, archived: targets.length });
}

export function deleteTask(req, res) {
  const tasks = readTasks();
  const task = tasks.find(t => t.id === req.params.id);
  if (!task) return res.status(404).json({ error: 'Task not found' });
  task.status = 'archived';
  task.archivedAt = new Date().toISOString();
  task.updatedAt = new Date().toISOString();
  writeTasks(tasks);
  logActivity('user', 'task_archived', { taskId: task.id, title: task.title });
  // Cleanup attachments directory
  const attDir = path.join(__dirname, 'data', 'attachments', req.params.id);
  try { fs.rmSync(attDir, { recursive: true, force: true }); } catch {}
  broadcast('tasks', tasks.filter(t => t.status !== 'archived'));
  res.json({ ok: true });
}

export function getCalendar(req, res) {
  const memoryDir = path.join(WORKSPACE, 'memory');
  const data = {};
  const initDay = (d) => { data[d] = data[d] || { memory: false, tasks: [], scheduled: [] }; };
  try {
    const files = fs.readdirSync(memoryDir).filter(f => /^\d{4}-\d{2}-\d{2}\.md$/.test(f));
    for (const f of files) {
      const date = f.replace('.md', '');
      initDay(date);
      try {
        const content = fs.readFileSync(path.join(memoryDir, f), 'utf8').trim();
        const firstLine = content.split('\n').find(l => l.trim() && !l.startsWith('#')) || content.split('\n')[0] || '';
        data[date].memory = firstLine.replace(/^[#\-*>\s]+/, '').trim().slice(0, 120) || true;
      } catch {
        data[date].memory = true;
      }
    }
  } catch {}
  const tasks = readTasks();
  for (const t of tasks) {
    // Completed tasks (including recurring run history)
    if (t.completedAt) {
      const date = isoToDateInTz(t.completedAt);
      initDay(date);
      data[date].tasks.push(t.title);
    }
    // Run history entries from recurring tasks
    if (Array.isArray(t.runHistory)) {
      for (const run of t.runHistory) {
        if (run.completedAt) {
          const date = isoToDateInTz(run.completedAt);
          initDay(date);
          data[date].tasks.push(t.title + (run.error ? ' ⚠' : ''));
        }
      }
    }
    // Scheduled / upcoming tasks — project future runs for recurring schedules
    if (t.schedule && t.scheduleEnabled !== false && t.status !== 'done' && t.status !== 'archived') {
      try {
        const runs = computeFutureRuns(t.schedule, 90);
        for (const run of runs) {
          const date = isoToDateInTz(run);
          initDay(date);
          if (!data[date].scheduled.find(s => s.id === t.id)) data[date].scheduled.push({ id: t.id, title: t.title });
        }
      } catch {}
      // Also include the immediate next run if not covered
      if (t.scheduledAt) {
        try {
          const date = isoToDateInTz(new Date(t.scheduledAt).toISOString());
          initDay(date);
          if (!data[date].scheduled.find(s => s.id === t.id)) data[date].scheduled.push({ id: t.id, title: t.title });
        } catch {}
      }
    } else if (t.scheduledAt && t.status !== 'done' && t.status !== 'archived') {
      // One-off scheduledAt without recurring schedule
      try {
        const date = isoToDateInTz(new Date(t.scheduledAt).toISOString());
        initDay(date);
        data[date].scheduled.push({ id: t.id, title: t.title });
      } catch {}
    }
  }
  res.json(data);
}

export function getRunHistory(req, res) {
  const tasks = readTasks();
  const task = tasks.find(t => t.id === req.params.id);
  if (!task) return res.status(404).json({ error: 'Task not found' });
  res.json(task.runHistory || []);
}

export function getCapacity(req, res) {
  const tasks = readTasks();
  recoverTasksIfNeeded(tasks);
  const settings = readSettings();
  const maxConcurrent = settings.maxConcurrent || 1;
  const activeCount = tasks.filter(t => t.status === 'in-progress' && t.pickedUp).length;
  const remainingSlots = Math.max(0, maxConcurrent - activeCount);
  res.json({ maxConcurrent, activeCount, remainingSlots });
}

export function reportStatusCheck(req, res) {
  const tasks = readTasks();
  const idx = tasks.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });

  const { status, message } = req.body;
  const normalized = status === 'running' ? 'in-progress' : status;
  const validStatuses = ['in-progress', 'completed', 'failed', 'timeout'];
  if (!normalized || !validStatuses.includes(normalized)) {
    return res.status(400).json({ error: 'status must be one of: ' + validStatuses.join(', ') });
  }

  logActivity('bot', 'task_status_check', { taskId: req.params.id, title: tasks[idx].title, status: normalized, message: message || null });

  if (normalized === 'completed' || normalized === 'failed' || normalized === 'timeout') {
    const now = new Date().toISOString();

    if (normalized === 'completed' && shouldIgnoreCompletion(tasks[idx])) {
      const terminalState = getCanonicalTerminalState(tasks[idx]);
      recordExecutionTransition(tasks[idx], terminalState, {
        at: now,
        actor: 'bot',
        reason: 'stale_completion_ignored',
        message: message || ('Ignored late completion after terminal state: ' + terminalState),
      });
      writeTasks(tasks);
      broadcast('tasks', tasks);
      return res.json(tasks[idx]);
    }

    const errorMsg = normalized === 'completed' ? null : (message || (normalized === 'timeout' ? 'Task timed out' : 'Task failed'));

    if (tasks[idx].schedule && tasks[idx].scheduleEnabled !== false) {
      if (!Array.isArray(tasks[idx].runHistory)) tasks[idx].runHistory = [];
      tasks[idx].runHistory.push({
        completedAt: now,
        startedAt: tasks[idx].startedAt,
        updatedAt: now,
        status: normalized,
        reason: normalized === 'timeout' ? 'task_timeout' : (normalized === 'failed' ? 'task_failed' : 'task_completed'),
        message: message || errorMsg,
        result: normalized === 'completed' ? (message || null) : null,
        error: errorMsg,
        subagentId: tasks[idx].subagentId || null,
        sessionId: tasks[idx].subagentId || null,
      });
      recordExecutionTransition(tasks[idx], normalized, {
        at: now,
        actor: 'bot',
        reason: normalized === 'timeout' ? 'task_timeout' : (normalized === 'failed' ? 'task_failed' : 'task_completed'),
        message: message || errorMsg,
      });
      tasks[idx].status = 'todo';
      tasks[idx].scheduledAt = computeNextRun(tasks[idx].schedule);
      tasks[idx].result = normalized === 'completed' ? (message || null) : null;
      tasks[idx].error = errorMsg;
      tasks[idx].startedAt = null;
      tasks[idx].completedAt = null;
      tasks[idx].subagentId = null;
      tasks[idx].pickedUp = false;
    } else {
      tasks[idx].status = 'done';
      tasks[idx].completedAt = now;
      tasks[idx].result = normalized === 'completed' ? (message || null) : null;
      tasks[idx].error = errorMsg;
      tasks[idx].subagentId = null;
      tasks[idx].pickedUp = false;
      recordExecutionTransition(tasks[idx], normalized, {
        at: now,
        actor: 'bot',
        reason: normalized === 'timeout' ? 'task_timeout' : (normalized === 'failed' ? 'task_failed' : 'task_completed'),
        message: message || errorMsg,
      });
    }
    writeTasks(tasks);
    if (normalized !== 'completed') logActivity('bot', 'task_timeout', { taskId: req.params.id, title: tasks[idx].title, message: message || null });
    broadcast('tasks', tasks);
    requestTaskDispatch();
    return res.json(tasks[idx]);
  }

  // in-progress heartbeat/status check
  const now = new Date().toISOString();
  recordExecutionTransition(tasks[idx], 'in-progress', {
    at: now,
    actor: 'bot',
    reason: 'status_check',
    message: message || 'Still running',
  });
  writeTasks(tasks);
  broadcast('tasks', tasks);
  res.json({ ok: true, status: 'in-progress' });
}

export function toggleSchedule(req, res) {
  const tasks = readTasks();
  const idx = tasks.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Task not found' });
  tasks[idx].scheduleEnabled = !tasks[idx].scheduleEnabled;
  tasks[idx].updatedAt = new Date().toISOString();
  writeTasks(tasks);
  logActivity('user', 'schedule_toggled', { taskId: tasks[idx].id, title: tasks[idx].title, enabled: tasks[idx].scheduleEnabled });
  broadcast('tasks', tasks.filter(t => t.status !== 'archived'));
  res.json(tasks[idx]);
}
