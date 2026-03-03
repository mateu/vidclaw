import { computeNextRun } from './schedule.js';

const HARD_STALE_MS = 30 * 60 * 1000;
const ORPHAN_PICKUP_MS = 2 * 60 * 1000;

function toMs(value) {
  const ts = value ? Date.parse(value) : NaN;
  return Number.isFinite(ts) ? ts : null;
}

export function recoverStaleTasks(tasks, opts = {}) {
  const now = opts.now instanceof Date ? opts.now : new Date();
  const nowIso = now.toISOString();
  const logActivity = typeof opts.logActivity === 'function' ? opts.logActivity : null;
  const recordExecutionTransition = typeof opts.recordExecutionTransition === 'function' ? opts.recordExecutionTransition : null;

  let changed = false;
  let recoveredOrphans = 0;
  let recoveredStale = 0;

  for (const task of tasks) {
    if (task.status !== 'in-progress' || !task.pickedUp) continue;

    const startedMs = toMs(task.startedAt) ?? toMs(task.updatedAt) ?? toMs(task.createdAt);
    if (!startedMs) continue;

    const elapsedMs = now.getTime() - startedMs;

    if (elapsedMs >= HARD_STALE_MS) {
      if (task.schedule && task.scheduleEnabled !== false) {
        if (!Array.isArray(task.runHistory)) task.runHistory = [];
        task.runHistory.push({
          completedAt: nowIso,
          startedAt: task.startedAt || null,
          updatedAt: nowIso,
          status: 'timeout',
          reason: 'stale_auto_recovered',
          message: 'Stale task auto-recovered',
          result: null,
          error: 'Stale task auto-recovered',
          subagentId: task.subagentId || null,
          sessionId: task.subagentId || null,
        });
        recordExecutionTransition?.(task, 'timeout', {
          at: nowIso,
          actor: 'system',
          reason: 'stale_auto_recovered',
          message: 'Auto-recovered after 30min stale',
        });
        task.status = 'todo';
        task.scheduledAt = computeNextRun(task.schedule);
        task.error = 'Stale task auto-recovered';
      } else {
        task.status = 'done';
        task.completedAt = nowIso;
        task.error = 'Stale task auto-recovered — sub-agent unresponsive';
        recordExecutionTransition?.(task, 'timeout', {
          at: nowIso,
          actor: 'system',
          reason: 'stale_auto_recovered',
          message: 'Auto-recovered after 30min stale',
        });
      }
      task.result = null;
      task.startedAt = null;
      task.subagentId = null;
      task.pickedUp = false;
      task.updatedAt = nowIso;
      changed = true;
      recoveredStale += 1;
      logActivity?.('system', 'task_timeout', { taskId: task.id, title: task.title, message: 'Auto-recovered after 30min stale' });
      continue;
    }

    if (!task.subagentId && elapsedMs >= ORPHAN_PICKUP_MS) {
      task.status = task.schedule ? 'todo' : 'todo';
      task.startedAt = null;
      task.subagentId = null;
      task.pickedUp = false;
      task.updatedAt = nowIso;
      task.error = 'Task pickup lease expired; returned to queue';
      recordExecutionTransition?.(task, 'queued', {
        at: nowIso,
        actor: 'system',
        reason: 'pickup_lease_expired',
        message: 'Pickup lease expired; task re-queued',
      });
      changed = true;
      recoveredOrphans += 1;
      logActivity?.('system', 'task_timeout', { taskId: task.id, title: task.title, message: 'Pickup lease expired with no sub-agent claim; re-queued' });
    }
  }

  return { changed, recoveredOrphans, recoveredStale };
}
