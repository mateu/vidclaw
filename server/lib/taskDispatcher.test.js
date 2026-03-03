import test from 'node:test';
import assert from 'node:assert/strict';

import { parseAgentResult, selectRunnableTasks } from './taskDispatcher.js';

test('parseAgentResult extracts payload text from OpenClaw run envelope', () => {
  const stdout = JSON.stringify({
    runId: 'abc',
    status: 'ok',
    result: {
      payloads: [
        { text: 'hello from payload', mediaUrl: null },
      ],
      meta: { durationMs: 1000 },
    },
  });

  const result = parseAgentResult(stdout, '');
  assert.equal(result, 'hello from payload');
});

test('parseAgentResult falls back to plain output when not JSON', () => {
  const result = parseAgentResult('plain output text', '');
  assert.equal(result, 'plain output text');
});

test('selectRunnableTasks honors maxConcurrent capacity', () => {
  const now = new Date('2026-03-03T19:00:00.000Z');
  const tasks = [
    { id: 'active', status: 'in-progress', pickedUp: true, order: 0, createdAt: '2026-03-03T18:00:00.000Z' },
    { id: 'todo-1', status: 'todo', pickedUp: false, schedule: null, order: 1, createdAt: '2026-03-03T18:01:00.000Z' },
    { id: 'todo-2', status: 'todo', pickedUp: false, schedule: null, order: 2, createdAt: '2026-03-03T18:02:00.000Z' },
  ];

  const runnableAtCap = selectRunnableTasks(tasks, { maxConcurrent: 1, now });
  assert.equal(runnableAtCap.length, 0);

  const runnableWithSlot = selectRunnableTasks(tasks, { maxConcurrent: 2, now });
  assert.equal(runnableWithSlot.length, 1);
  assert.equal(runnableWithSlot[0].id, 'todo-1');
});
