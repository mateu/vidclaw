import express from 'express';
import http from 'http';
import { HOST, PORT } from './config.js';
import { setupWebSocket, broadcast } from './broadcast.js';
import { setupMiddleware } from './middleware.js';
import router from './routes.js';
import { readTasks, writeTasks, logActivity } from './lib/fileStore.js';
import { recoverStaleTasks } from './lib/taskRecovery.js';

const app = express();
const server = http.createServer(app);

setupWebSocket(server);
setupMiddleware(app);
app.use(router);

if (HOST !== '127.0.0.1' && HOST !== 'localhost') {
  console.warn(
    `[WARN] HOST is set to ${HOST}. This exposes the dashboard beyond localhost unless restricted by firewall/network policy.`
  );
}

server.listen(PORT, HOST, () => {
  console.log(`Dashboard running at http://${HOST}:${PORT}`);
});

// Task recovery sweep — every 1 minute for predictable pickup + stale recovery
setInterval(() => {
  const tasks = readTasks();
  const recovery = recoverStaleTasks(tasks, { logActivity });
  if (recovery.changed) {
    writeTasks(tasks);
    broadcast('tasks', tasks.filter(t => t.status !== 'archived'));
  }
}, 60 * 1000);
