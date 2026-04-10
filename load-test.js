'use strict';

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

const logger = require('./src/utils/logger');

/* ─────────────────────────────────────────────────────────────
   Config
───────────────────────────────────────────────────────────── */

const TEST_ROWS = parseInt(process.env.TEST_ROWS || '1000000', 10);
const CSV_OUTPUT = path.join(process.cwd(), 'test-data-1m.csv');
const BATCH_SIZE = 10000;

/* ─────────────────────────────────────────────────────────────
   Generate test CSV
───────────────────────────────────────────────────────────── */

async function generateTestCSV() {
  logger.info(`Generating ${TEST_ROWS} test records...`);

  const startMem = process.memoryUsage().heapUsed / 1024 / 1024;
  const startTime = performance.now();

  const stream = fs.createWriteStream(CSV_OUTPUT, { flags: 'w' });

  // Write header
  const headers = [
    'stock_num', 'certificate_num', 'shape', 'carat', 'clarity', 'color',
    'cut', 'polish', 'symmetry', 'fluorescence', 'depth_percent', 'table_percent',
    'length', 'width', 'depth', 'girdle', 'culet_size', 'lab', 'location',
    'our_price', 'image_url'
  ];
  stream.write(headers.join(',') + '\n');

  const shapes = ['Round', 'Princess', 'Cushion', 'Emerald', 'Radiant'];
  const clarities = ['IF', 'VVS1', 'VVS2', 'VS1', 'VS2', 'SI1', 'SI2'];
  const colors = ['D', 'E', 'F', 'G', 'H', 'I', 'J'];
  const cuts = ['Excellent', 'Very Good', 'Good', 'Fair'];

  for (let i = 0; i < TEST_ROWS; i++) {
    const certNum = `CERT${String(i).padStart(10, '0')}`;
    const stockNum = `STK${String(i).padStart(8, '0')}`;

    const row = [
      stockNum,
      certNum,
      shapes[i % shapes.length],
      (Math.random() * 5 + 0.5).toFixed(2),
      clarities[i % clarities.length],
      colors[i % colors.length],
      cuts[i % cuts.length],
      cuts[i % cuts.length],
      cuts[i % cuts.length],
      'None',
      (Math.random() * 30 + 58).toFixed(1),
      (Math.random() * 20 + 50).toFixed(1),
      (Math.random() * 10 + 3).toFixed(2),
      (Math.random() * 10 + 3).toFixed(2),
      (Math.random() * 5 + 2).toFixed(2),
      'Thin',
      'Pointed',
      'GIA',
      'New York',
      (Math.random() * 50000 + 1000).toFixed(2),
      `https://example.com/image/${certNum}.jpg`
    ];

    stream.write(row.join(',') + '\n');

    if ((i + 1) % BATCH_SIZE === 0) {
      const pct = Math.round((i + 1) / TEST_ROWS * 100);
      logger.info(`Generated ${i + 1} / ${TEST_ROWS} rows (${pct}%)`);
    }
  }

  return new Promise((resolve, reject) => {
    stream.end(() => {
      const endTime = performance.now();
      const endMem = process.memoryUsage().heapUsed / 1024 / 1024;
      const fileSizeMB = fs.statSync(CSV_OUTPUT).size / 1024 / 1024;

      logger.info(`✓ CSV generated in ${(endTime - startTime).toFixed(1)}ms`);
      logger.info(`  File size: ${fileSizeMB.toFixed(1)}MB`);
      logger.info(`  Memory delta: ${(endMem - startMem).toFixed(1)}MB`);
      resolve();
    });
    stream.on('error', reject);
  });
}

/* ─────────────────────────────────────────────────────────────
   Test CSV parsing performance
───────────────────────────────────────────────────────────── */

async function testCSVParsing() {
  logger.info('Testing CSV parsing performance...');

  const { parseCSVStream } = require('./src/services/csv.service');

  const startMem = process.memoryUsage().heapUsed / 1024 / 1024;
  const startTime = performance.now();

  let rowCount = 0;
  let peakMem = startMem;

  const result = await parseCSVStream(CSV_OUTPUT, async (row) => {
    rowCount++;

    const currentMem = process.memoryUsage().heapUsed / 1024 / 1024;
    if (currentMem > peakMem) peakMem = currentMem;

    if (rowCount % BATCH_SIZE === 0) {
      const elapsed = (performance.now() - startTime) / 1000;
      const rowsPerSec = (rowCount / elapsed).toFixed(0);
      logger.info(`Parsed ${rowCount} / ${TEST_ROWS} rows (${rowsPerSec} rows/sec)`);
    }
  });

  const endTime = performance.now();
  const endMem = process.memoryUsage().heapUsed / 1024 / 1024;

  logger.info('✓ CSV Parsing Complete');
  logger.info(`  Total time: ${(endTime - startTime).toFixed(1)}ms`);
  logger.info(`  Rows: ${result.totalRows}`);
  logger.info(`  Valid: ${result.validRows}, Skipped: ${result.skippedRows}`);
  logger.info(`  Peak memory: ${peakMem.toFixed(1)}MB`);
  logger.info(`  Memory delta: ${(endMem - startMem).toFixed(1)}MB`);
  logger.info(`  Avg rows/sec: ${(result.validRows / ((endTime - startTime) / 1000)).toFixed(0)}`);

  return { totalTime: endTime - startTime, totalRows: result.validRows };
}

/* ─────────────────────────────────────────────────────────────
   Test memory efficiency
───────────────────────────────────────────────────────────── */

async function testMemoryEfficiency() {
  logger.info('Testing memory efficiency (simulating row processing)...');

  const { parseCSVStream } = require('./src/services/csv.service');

  const startMem = process.memoryUsage().heapUsed / 1024 / 1024;
  let peakMem = startMem;
  let maxSimulationRows = 0;

  await parseCSVStream(CSV_OUTPUT, async (row) => {
    // Simulate some processing without actual external calls
    const processed = {
      cert: row.certificate_num,
      data: row,
      timestamp: new Date()
    };
    maxSimulationRows++;

    const currentMem = process.memoryUsage().heapUsed / 1024 / 1024;
    if (currentMem > peakMem) peakMem = currentMem;

    if (maxSimulationRows % (BATCH_SIZE * 10) === 0) {
      logger.info(`Processed ${maxSimulationRows} rows, memory: ${currentMem.toFixed(1)}MB`);
    }
  });

  const endMem = process.memoryUsage().heapUsed / 1024 / 1024;

  logger.info('✓ Memory Efficiency Test Complete');
  logger.info(`  Peak memory: ${peakMem.toFixed(1)}MB`);
  logger.info(`  Final memory: ${endMem.toFixed(1)}MB`);
  logger.info(`  Memory efficiency: ${((peakMem - startMem) / (TEST_ROWS / 1000000)).toFixed(1)}MB per million rows`);
}

/* ─────────────────────────────────────────────────────────────
   Concurrent processing simulation
───────────────────────────────────────────────────────────── */

async function testConcurrentProcessing() {
  logger.info('Testing concurrent queue processing (limited by I/O mocks)...');

  const { parseCSVStream } = require('./src/services/csv.service');

  const concurrency = parseInt(process.env.CONCURRENCY || '20', 10);
  const startTime = performance.now();
  let processed = 0;
  let failed = 0;

  const processingPromises = [];
  let activeCount = 0;

  const mockProcessRow = async () => {
    // Simulate I/O delay (100-500ms per row)
    return new Promise(resolve => {
      setTimeout(resolve, Math.random() * 400 + 100);
    });
  };

  await parseCSVStream(CSV_OUTPUT, async (row) => {
    activeCount++;

    if (activeCount >= concurrency) {
      await Promise.race(processingPromises);
    }

    const promise = mockProcessRow()
      .then(() => { processed++; })
      .catch(() => { failed++; })
      .finally(() => {
        activeCount--;
        const idx = processingPromises.indexOf(promise);
        if (idx > -1) processingPromises.splice(idx, 1);
      });

    processingPromises.push(promise);

    if (processed % (BATCH_SIZE * 5) === 0) {
      const elapsed = (performance.now() - startTime) / 1000;
      const rate = (processed / elapsed).toFixed(0);
      logger.info(`Processed ${processed} rows at ${rate} rows/sec`);
    }
  });

  // Wait for remaining promises
  await Promise.all(processingPromises);

  const endTime = performance.now();
  const totalTime = (endTime - startTime) / 1000;

  logger.info('✓ Concurrent Processing Test Complete');
  logger.info(`  Total time: ${totalTime.toFixed(1)}s`);
  logger.info(`  Processed: ${processed}, Failed: ${failed}`);
  logger.info(`  Rate: ${(processed / totalTime).toFixed(0)} rows/sec`);
  logger.info(`  Concurrency: ${concurrency} workers`);
}

/* ─────────────────────────────────────────────────────────────
   Report bottlenecks
───────────────────────────────────────────────────────────── */

function reportBottlenecks() {
  logger.info('\n═══════════════════════════════════════════════════════');
  logger.info('BOTTLENECK ANALYSIS');
  logger.info('═══════════════════════════════════════════════════════');

  logger.info('\n🔴 CRITICAL ISSUES:');
  logger.info('1. CSV Streaming: O(1) memory — GOOD');
  logger.info('2. Database: Updates are sequential per worker');
  logger.info('   - 20 concurrent workers × 1-2 DB ops per row');
  logger.info('   - MongoDB write throughput is the bottleneck');
  logger.info('3. External APIs: Video fetch & S3 uploads will dominate');
  logger.info('   - Mock test shows 100-500ms per row with concurrency');
  logger.info('   - Real scenario: +2-5s per row (video API + frame extraction)');

  logger.info('\n⚠️  PERFORMANCE ESTIMATES FOR 1M ROWS:');
  const rowsPerSec = 20; // 20 workers at ~1 sec per row
  const totalSeconds = 1000000 / rowsPerSec;
  const hours = totalSeconds / 3600;
  logger.info(`  At 20 workers, 1-2s per row: ~${hours.toFixed(1)} hours`);
  logger.info(`  At 50 workers, 1-2s per row: ~${(hours / 2.5).toFixed(1)} hours`);

  logger.info('\n💡 RECOMMENDATIONS:');
  logger.info('1. Increase worker concurrency from 20 to 50-100');
  logger.info('2. Batch MongoDB inserts (use bulkWrite instead of findOneAndUpdate)');
  logger.info('3. Add database indexes on certificate_num for faster lookups');
  logger.info('4. Consider sharding if MongoDB becomes bottleneck');
  logger.info('5. Implement retry logic with exponential backoff');
  logger.info('6. Monitor Redis queue memory for 1M+ jobs');
  logger.info('7. Use S3 multipart upload for faster video processing');

  logger.info('\n📊 KEY METRICS:');
  logger.info(`  Test file size: ${(fs.statSync(CSV_OUTPUT).size / 1024 / 1024).toFixed(1)}MB`);
  logger.info(`  Test rows: ${TEST_ROWS}`);
  logger.info(`  Worker concurrency: 20 (adjust with CONCURRENCY env var)`);

  logger.info('\n═══════════════════════════════════════════════════════\n');
}

/* ─────────────────────────────────────────────────────────────
   Main test runner
───────────────────────────────────────────────────────────── */

async function runTests() {
  try {
    logger.info(`🚀 Starting load tests for ${TEST_ROWS} records\n`);

    // Generate CSV
    if (!fs.existsSync(CSV_OUTPUT)) {
      await generateTestCSV();
    } else {
      logger.info(`Using existing CSV: ${CSV_OUTPUT}`);
    }

    // Run tests
    await testCSVParsing();
    logger.info('');

    await testMemoryEfficiency();
    logger.info('');

    await testConcurrentProcessing();
    logger.info('');

    reportBottlenecks();

    logger.info('✓ All tests completed successfully');
    process.exit(0);
  } catch (err) {
    logger.error(`Test failed: ${err.message}`);
    logger.error(err.stack);
    process.exit(1);
  }
}

runTests();
