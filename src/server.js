import ccxt from 'ccxt';
import { buildApp } from './app.js';
import { loadConfig } from './config.js';

const config = loadConfig();
const { fastify } = buildApp({ config, ccxt });

try {
  await fastify.listen({ host: config.host, port: config.port });
  fastify.log.info({ host: config.host, port: config.port }, 'CCXT gateway is listening');
} catch (error) {
  fastify.log.error({ errorType: error?.name ?? 'Error' }, 'Unable to start CCXT gateway');
  process.exitCode = 1;
}

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  fastify.log.info({ signal }, 'Shutting down');
  try {
    await fastify.close();
  } catch (error) {
    fastify.log.error({ errorType: error?.name ?? 'Error' }, 'Graceful shutdown failed');
    process.exitCode = 1;
  }
}

process.once('SIGINT', () => { void shutdown('SIGINT'); });
process.once('SIGTERM', () => { void shutdown('SIGTERM'); });
