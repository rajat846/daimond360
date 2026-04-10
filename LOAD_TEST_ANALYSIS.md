# Diamond 360 Pipeline - Load Test Analysis (1M Records)

## Executive Summary

This document analyzes the **diamond360-pipeline** for handling **1 million records** and provides bottleneck identification with optimization recommendations.

---

## Architecture Overview

### Core Components
1. **Express API** - CSV upload endpoint
2. **BullMQ Queue** - Job management (20 concurrent workers)
3. **MongoDB** - Metadata storage
4. **Redis** - Job queue persistence
5. **External APIs** - Video data fetching, frame extraction
6. **S3** - Image/strip storage

### Data Flow
```
CSV Upload
  ↓
Job Enqueue (BullMQ) → Redis
  ↓
Worker Pool (20 concurrent)
  ├─ Fetch video data (API call)
  ├─ Extract frames (file I/O)
  ├─ Generate strips (image processing)
  ├─ Upload to S3
  └─ Save to MongoDB
```

---

## Performance Analysis

### 1. CSV Parsing ✅ OPTIMIZED
- **Implementation**: Streaming parser with row-by-row callback
- **Memory**: O(1) - constant memory regardless of file size
- **Throughput**: ~10,000-50,000 rows/sec (depends on callback complexity)
- **Assessment**: **EXCELLENT** - Streaming prevents OOM on large files

```javascript
// Current: Streaming with back-pressure
await parseCSVStream(filePath, async (row) => {
  // Process single row
  // Parser paused during callback to prevent buffering
});
```

### 2. Database Operations ⚠️ BOTTLENECK
- **Issue**: Sequential `findOneAndUpdate()` per row
- **Concurrency**: Only 1 DB op per worker at a time
- **Load**: 20 workers = 20 parallel MongoDB writes
- **Throughput**: MongoDB limits at 5,000-10,000 writes/sec

**For 1M records:**
- Time in DB ops: ~100-200 seconds
- Total throughput: 5,000-10,000 rows/sec (DB-limited)

### 3. External API Calls 🔴 CRITICAL BOTTLENECK
- **Video API**: ~500ms-2s per request
- **Frame Extraction**: ~1-3s per video
- **S3 Uploads**: ~500ms-1s per batch
- **Estimated**: 2-5s per row minimum

**For 1M records with 20 workers:**
- Time required: 50,000-250,000 seconds
- **Duration: 14-70 hours** ⚠️

### 4. Memory Usage ✅ GOOD
- **Per Worker**: ~50-100MB baseline
- **20 Workers**: ~1-2GB total
- **Streaming CSV**: No spike on large files
- **Assessment**: Acceptable for production

---

## Critical Issues

### Issue #1: External API Latency Dominates
**Problem**: Each row requires 2-5 seconds of external API calls
- Video fetch: 500ms-2s
- Frame extraction: 1-3s
- S3 upload: 500ms-1s

**Impact**: Even with 100 workers, limited by slowest external service

**Solution**:
- Add request timeout handling
- Implement retry with exponential backoff
- Cache video data responses
- Batch S3 uploads

### Issue #2: Sequential Database Writes Per Worker
**Problem**: Each worker serializes database operations
- 20 concurrent workers = 20 parallel writes
- Each write: 10-50ms
- Bottleneck: MongoDB throughput cap (~5-10k ops/sec)

**Impact**: Database becomes bottleneck for non-I/O-bound scenarios

**Solution**:
- Use `bulkWrite()` instead of `findOneAndUpdate()`
- Batch 100-1000 records per database operation
- Add database indexes on `certificate_num`

### Issue #3: Queue Size Growth
**Problem**: 1M jobs in Redis queue = memory explosion
- Average job size: ~500 bytes
- Total: ~500MB (acceptable)
- With retries: 1-2GB (problematic)

**Solution**:
- Implement job expiration
- Stream jobs directly without full queue load
- Use persistent storage backend (RabbitMQ/Kafka)

### Issue #4: No Batch Processing
**Problem**: Each row processed independently
- No aggregation of related operations
- Numerous database round-trips
- Wasteful API calls for same certificate

**Solution**:
- Group by certificate/video ID
- Batch frame extraction
- Consolidate DB writes

---

## Load Test Results Template

Run tests with:
```bash
npm run load-test:1m
```

Expected output:
```
✓ CSV Parsing Complete
  Total time: 45,000ms
  Rows: 1,000,000
  Peak memory: 250MB
  Memory delta: 50MB
  Avg rows/sec: 22,222

✓ Memory Efficiency Test Complete
  Peak memory: 280MB
  Final memory: 200MB
  Memory efficiency: 0.28MB per million rows

✓ Concurrent Processing Test Complete
  Total time: 50,000s
  Processed: 1,000,000
  Rate: 20 rows/sec (with 100-500ms simulated I/O)
```

---

## Recommendations

### Priority 1: Immediate (High Impact)
1. **Batch Database Writes**
   ```javascript
   // Instead of:
   await Diamond.findOneAndUpdate({...})

   // Use:
   const ops = rows.map(row => ({
     updateOne: {
       filter: { certificate_num: row.cert },
       update: { $set: row },
       upsert: true
     }
   }));
   await Diamond.bulkWrite(ops);
   ```

2. **Increase Worker Concurrency**
   ```javascript
   // processor.worker.js line 275
   concurrency: 50  // or 100 for smaller external payload
   ```

3. **Add Database Indexes**
   ```javascript
   // In diamond_v1.model.js
   certificate_num: { type: String, required: true, unique: true, index: true },
   processing_status: { type: String, ..., index: true },
   stock_num: { type: String, ..., index: true }
   ```

### Priority 2: Medium (Reliability)
4. **Implement Retry Logic**
   ```javascript
   // In processor.worker.js
   const maxRetries = 3;
   const retryDelay = 1000 * Math.pow(2, attemptNumber);
   ```

5. **Add Circuit Breaker for External APIs**
   - Fail fast if API is down
   - Prevent cascading failures

6. **Job Timeout Limits**
   ```javascript
   // queue.js
   defaultJobOptions: {
     timeout: 300000  // 5 minutes per row max
   }
   ```

### Priority 3: Optimization
7. **Implement Result Caching**
   - Cache video API responses (24h TTL)
   - Reduce duplicate requests

8. **Stream Job Processing**
   - Don't load entire queue into memory
   - Use Redis streams instead of full queue

9. **S3 Multipart Upload**
   - Parallelize frame uploads
   - Reduce upload latency

---

## Scaling Recommendations

### For 1M Records in Acceptable Time:

| Configuration | Estimated Time | Cost |
|---|---|---|
| 20 workers (current) | 14-70 hours | Low |
| 50 workers | 5-28 hours | Medium |
| 100 workers | 3-14 hours | High |
| + Batch DB writes | -50% | Low |
| + API caching | -30% | Low |
| + Dedicated DB | -40% | High |

**Recommended**: 50-100 workers + batched DB writes = **4-8 hours for 1M records**

---

## Testing Checklist

- [ ] Run load-test with 100k records (verify parsing performance)
- [ ] Monitor database connection pool
- [ ] Check Redis memory usage
- [ ] Verify worker CPU/memory usage
- [ ] Test API rate limiting behavior
- [ ] Verify S3 upload throughput
- [ ] Check for database connection leaks
- [ ] Test job cancellation at scale
- [ ] Verify error handling with mixed failures
- [ ] Monitor network bandwidth

---

## Monitoring Metrics to Track

1. **CSV Processing Rate**: rows/sec
2. **Database Write Latency**: p95, p99
3. **API Success Rate**: % of successful calls
4. **Queue Depth**: jobs pending
5. **Worker CPU/Memory**: per process
6. **Job Duration**: min/max/avg per row
7. **Error Rate**: % of failed rows
8. **Network I/O**: MB/s to S3 and external APIs

---

## Conclusion

The **diamond360-pipeline** is well-architected for streaming large CSV files with O(1) memory usage. However, **processing 1M records will take 14+ hours** due to external API latency being the dominant factor.

**Key Optimization**: Focus on **external API optimization** (caching, parallelization) rather than database, as API calls dominate the processing time at 2-5 seconds per row.

Implementing the Priority 1 recommendations (batch DB writes + increased concurrency) can reduce this to 5-8 hours with minimal code changes.
