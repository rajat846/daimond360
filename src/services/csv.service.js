'use strict';

const fs     = require('fs');
const { parse } = require('csv-parse');
const { buildColumnMap, mapRow } = require('../utils/columnMapper');
const logger = require('../utils/logger');

/**
 * Streams a CSV file row-by-row and invokes `onRow(mappedRow)` for each
 * valid row. Rows without a certificate number are skipped.
 *
 * CHANGE: Replaced full in-memory array accumulation with a streaming
 *   callback-based API (parseCSVStream).
 * WHY: The original parseCSV loaded ALL rows into rawRows[], then mapped
 *   them all, then returned the full array. For a 50k-row CSV at ~500 bytes
 *   per row that is ~25MB held in memory at once, plus the mapped copy.
 *   Streaming processes one row at a time, keeping memory flat regardless
 *   of CSV size.
 * IMPACT: Memory usage for CSV processing is O(1) in row count.
 * BREAK RISK: Callers that relied on receiving the full rows array must
 *   switch to the streaming API. The legacy parseCSV() wrapper is preserved
 *   for backward compatibility but is not recommended for large files.
 * FUTURE: If sorting or deduplication across rows is needed, introduce an
 *   explicit accumulation step rather than reverting to full in-memory load.
 *
 * CHANGE: Async onRow callbacks are awaited sequentially via a promise
 *   chain. This prevents unbounded parallelism when processRow() is the
 *   callback (which itself downloads frames and writes to DB).
 * WHY: Firing all row callbacks simultaneously without back-pressure would
 *   re-introduce the OOM problem at a higher level.
 * IMPACT: Rows are processed in order with full back-pressure.
 * BREAK RISK: Parser is paused while onRow runs — do not call parser.read()
 *   concurrently. Use the concurrency controls in the worker instead.
 *
 * @param {string}   filePath
 * @param {Function} onRow     - async (mappedRow) => void
 * @returns {Promise<{ totalRows: number, validRows: number, skippedRows: number, columnMap: object }>}
 */
async function parseCSVStream(filePath, onRow) {
  return new Promise((resolve, reject) => {
    let columnMap   = null;
    let totalRows   = 0;
    let validRows   = 0;
    let skippedRows = 0;
    let rowIndex    = 0;

    // Chain promise so each onRow awaits the previous before continuing
    let chain = Promise.resolve();
    let streamError = null;

    const parser = fs.createReadStream(filePath).pipe(
      parse({
        columns:          true,
        skip_empty_lines: true,
        trim:             true,
        bom:              true,
      })
    );

    parser.on('data', (record) => {
      totalRows++;

      if (!columnMap) {
        const headers = Object.keys(record);
        columnMap = buildColumnMap(headers);
        logger.info(`Column map: ${JSON.stringify(columnMap)}`);
      }

      rowIndex++;
      const mapped = mapRow(record, columnMap);
      mapped._rowIndex = rowIndex;

      if (!mapped.certificate_num) {
        skippedRows++;
        return;
      }

      validRows++;

      // Pause the stream while onRow runs — enforces back-pressure
      parser.pause();
      chain = chain
        .then(() => onRow(mapped))
        .then(() => {
          if (!streamError) parser.resume();
        })
        .catch((err) => {
          streamError = err;
          parser.destroy(err);
        });
    });

    parser.on('error', (err) => {
      logger.error(`CSV parse error: ${err.message}`);
      reject(new Error(`Failed to parse CSV: ${err.message}`));
    });

    parser.on('end', () => {
      // Wait for the last onRow to finish before resolving
      chain.then(() => {
        if (totalRows === 0) {
          return reject(new Error('CSV file is empty or has no data rows'));
        }

        if (skippedRows > 0) {
          logger.warn(`Skipped ${skippedRows} rows with missing certificate number`);
        }

        logger.info(`CSV streamed — ${validRows} valid rows, ${skippedRows} skipped`);
        resolve({ totalRows, validRows, skippedRows, columnMap });
      }).catch(reject);
    });
  });
}

/**
 * Legacy wrapper — loads ALL rows into memory.
 * Preserved for backward compatibility with existing callers and tests.
 * For large files prefer parseCSVStream().
 *
 * @param {string} filePath
 * @returns {Promise<{ rows: object[], columnMap: object }>}
 */
async function parseCSV(filePath) {
  const rows = [];
  const { columnMap } = await parseCSVStream(filePath, async (row) => {
    rows.push(row);
  });
  return { rows, columnMap };
}

module.exports = { parseCSV, parseCSVStream };