'use strict';

const fs   = require('fs');
const { parse } = require('csv-parse');
const { buildColumnMap, mapRow } = require('../utils/columnMapper');
const logger = require('../utils/logger');

/**
 * Parses a CSV file and returns an array of normalised diamond row objects.
 *
 * @param {string} filePath  - Absolute path to the CSV file on disk
 * @returns {Promise<{ rows: object[], columnMap: object }>}
 */
async function parseCSV(filePath) {
  return new Promise((resolve, reject) => {
    const rawRows = [];

    const parser = fs.createReadStream(filePath).pipe(
      parse({
        columns: true,        // Use first row as field names
        skip_empty_lines: true,
        trim: true,
        bom: true,            // Handle UTF-8 BOM if present
      })
    );

    parser.on('readable', () => {
      let record;
      while ((record = parser.read()) !== null) {
        rawRows.push(record);
      }
    });

    parser.on('error', (err) => {
      logger.error(`CSV parse error: ${err.message}`);
      reject(new Error(`Failed to parse CSV: ${err.message}`));
    });

    parser.on('end', () => {
      if (rawRows.length === 0) {
        return reject(new Error('CSV file is empty or has no data rows'));
      }

      const headers   = Object.keys(rawRows[0]);
      const columnMap = buildColumnMap(headers);

      logger.info(`CSV parsed — ${rawRows.length} rows detected`);
      logger.info(`Column map: ${JSON.stringify(columnMap)}`);

      const rows = rawRows.map((rawRow, idx) => {
        const mapped = mapRow(rawRow, columnMap);
        mapped._rowIndex = idx + 1; // 1-based for human-readable logging
        return mapped;
      });

      // Filter out rows with no certificate number (they cannot be processed)
      const valid   = rows.filter((r) => r.certificate_num);
      const skipped = rows.length - valid.length;

      if (skipped > 0) {
        logger.warn(`Skipped ${skipped} rows with missing certificate number`);
      }

      resolve({ rows: valid, columnMap });
    });
  });
}

module.exports = { parseCSV };
