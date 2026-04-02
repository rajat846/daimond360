'use strict';

const logger = require('./logger');

/**
 * Retry an async function up to `attempts` times with exponential back-off.
 *
 * @param {Function} fn           - Async function to execute
 * @param {number}   attempts     - Maximum number of tries (default 3)
 * @param {number}   delayMs      - Initial delay in ms between retries (default 2000)
 * @param {string}   [label='']   - Human-readable label for logging
 * @returns {Promise<any>}
 */
async function withRetry(fn, attempts = 3, delayMs = 2000, label = '') {
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      const tag = label ? `[${label}] ` : '';

      if (attempt < attempts) {
        const wait = delayMs * attempt; // simple linear back-off
        logger.warn(`${tag}Attempt ${attempt}/${attempts} failed — retrying in ${wait}ms. Error: ${err.message}`);
        await sleep(wait);
      } else {
        logger.error(`${tag}All ${attempts} attempts failed. Last error: ${err.message}`);
      }
    }
  }

  throw lastError;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = { withRetry, sleep };
