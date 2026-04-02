'use strict';

const fs     = require('fs');
const https  = require('https');
const http   = require('http');
const path   = require('path');
const logger = require('../utils/logger');
const { withRetry } = require('../utils/retry');

const TEMP_BASE = process.env.TEMP_DIR || '/tmp/diamond360';

/**
 * Ensures a directory exists, creating it recursively if necessary.
 */
function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * Downloads a single frame from <baseUrl>/<index>.jpg into the target directory.
 *
 * @param {string} baseUrl  - Base URL returned by the GraphQL API
 * @param {number} index    - Frame index (0-based)
 * @param {string} dir      - Local directory to save the file
 * @returns {Promise<string>} Absolute path to the downloaded file
 */
function downloadFrame(baseUrl, index, dir) {
  return withRetry(
    () =>
      new Promise((resolve, reject) => {
        const filePath   = path.join(dir, `${index}.jpg`);
        const frameUrl   = `${baseUrl}/${index}.jpg`;
        const transport  = frameUrl.startsWith('https') ? https : http;
        const fileStream = fs.createWriteStream(filePath);

        transport
          .get(frameUrl, (res) => {
            if (res.statusCode !== 200) {
              fileStream.destroy();
              fs.unlink(filePath, () => {});
              return reject(new Error(`Frame ${index} — HTTP ${res.statusCode} from ${frameUrl}`));
            }
            res.pipe(fileStream);
            fileStream.on('finish', () => fileStream.close(() => resolve(filePath)));
          })
          .on('error', (err) => {
            fileStream.destroy();
            fs.unlink(filePath, () => {});
            reject(new Error(`Frame ${index} download error: ${err.message}`));
          });
      }),
    3,
    1000,
    `downloadFrame(${index})`
  );
}

/**
 * Downloads all frames for a certificate sequentially.
 *
 * @param {string} certNumber   - Certificate number (used as folder name)
 * @param {string} baseUrl      - Base URL from GraphQL v360.url
 * @param {number} frameCount   - Total frames to download (raw frame_count)
 * @returns {Promise<string>}   - Path to the temp folder containing frames
 */
async function extractFrames(certNumber, baseUrl, frameCount) {
  const tempDir = path.join(TEMP_BASE, certNumber);
  ensureDir(tempDir);

  logger.info(`[${certNumber}] Downloading ${frameCount} frames into ${tempDir}`);

  for (let i = 0; i < frameCount; i++) {
    await downloadFrame(baseUrl, i, tempDir);
    if ((i + 1) % 20 === 0) {
      logger.debug(`[${certNumber}] Downloaded ${i + 1}/${frameCount} frames`);
    }
  }

  logger.info(`[${certNumber}] All ${frameCount} frames downloaded`);
  return tempDir;
}

module.exports = { extractFrames, ensureDir, TEMP_BASE };
