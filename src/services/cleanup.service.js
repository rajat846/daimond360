'use strict';

const fs   = require('fs/promises');
const path = require('path');
const logger = require('../utils/logger');
const { TEMP_BASE } = require('./frame.service');

/**
 * Removes the entire temporary folder for a certificate.
 * This includes:
 *  - downloaded frames  (0.jpg, 1.jpg, …)
 *  - generated strips   (__strips__/strip_0.jpg, …)
 *
 * Uses { recursive: true, force: true } so it never throws even if
 * the directory doesn't exist.
 *
 * @param {string} certNumber
 */
async function cleanupCertificate(certNumber) {
  const tempDir = path.join(TEMP_BASE, certNumber);
  try {
    await fs.rm(tempDir, { recursive: true, force: true });
    logger.info(`[${certNumber}] Temp files cleaned up: ${tempDir}`);
  } catch (err) {
    // Non-fatal — log and move on
    logger.warn(`[${certNumber}] Cleanup warning: ${err.message}`);
  }
}

/**
 * Removes the uploaded CSV file after processing is complete.
 *
 * @param {string} filePath
 */
async function cleanupUploadedCSV(filePath) {
  try {
    await fs.rm(filePath, { force: true });
    logger.info(`CSV file removed: ${filePath}`);
  } catch (err) {
    logger.warn(`CSV cleanup warning: ${err.message}`);
  }
}

module.exports = { cleanupCertificate, cleanupUploadedCSV };
