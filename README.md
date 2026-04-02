# Diamond 360 Video Processing Pipeline

A production-ready Node.js backend that processes diamond 360-video data from a CSV file — fetching frame data via GraphQL, downloading individual frames, generating image strips with strict equal distribution, uploading to AWS S3, and persisting structured records in MongoDB.

---

## Architecture

```
POST /upload-csv
      │
      ▼
  Multer saves CSV to disk
      │
      ▼
  Job record created in MongoDB
      │
      ▼
  BullMQ enqueues job → responds immediately "Processing started"
      │
      ▼
┌─────────────────────────────────────────────┐
│         processor.worker.js (background)    │
│                                             │
│  for each row (strictly sequential):        │
│    1. fetchVideoData (GraphQL, 3 retries)   │
│    2. extractFrames  (download 0.jpg…N.jpg) │
│    3. generateStrips (strict equal, 10×)    │
│    4. uploadStripsToS3 (AWS SDK v3)         │
│    5. upsertDiamond  (MongoDB)              │
│    6. cleanupCertificate (rm -rf /tmp/…)   │
│                                             │
│  Log: "Processing 25 / 500"                 │
└─────────────────────────────────────────────┘
```

---

## Strip Generation Logic (Strict Equal)

```
raw frame_count = 103

Step 1 — make even:      103 → 102
Step 2 — divisible by 10: 102 % 10 = 2 → 102 - 2 = 100
Step 3 — 10 strips:       100 / 10 = 10 frames per strip

Result:
  total_frames     = 103
  used_frames      = 100
  ignored_frames   = 3
  frames_per_strip = 10
  total_strips     = 10
```

No remainders are distributed. No strip ever has more or fewer frames than any other.

---

## Quick Start

### Prerequisites

| Tool       | Version |
|------------|---------|
| Node.js    | ≥ 18    |
| MongoDB    | ≥ 6     |
| Redis      | ≥ 7     |
| ffmpeg     | any     |

### 1. Install dependencies

```bash
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your real credentials:

```env
MONGODB_URI=mongodb://localhost:27017/diamond360
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
AWS_ACCESS_KEY_ID=YOUR_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET
AWS_S3_BUCKET=your-bucket
AWS_REGION=us-east-1
```

### 3. Start the API server

```bash
npm start
# or for development with auto-reload:
npm run dev
```

### 4. Start the background worker (separate terminal)

```bash
npm run worker
```

> The API server and the worker are **separate processes**. Both must be running for end-to-end processing.

---

## API Reference

### `POST /upload-csv`

Upload a CSV file to start a background processing job.

**Request** — `multipart/form-data`

| Field | Type | Description            |
|-------|------|------------------------|
| `csv` | file | The diamond CSV file   |

**Response `202 Accepted`**

```json
{
  "success": true,
  "message": "Processing started",
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "statusUrl": "/job-status/550e8400-e29b-41d4-a716-446655440000"
}
```

---

### `GET /job-status/:id`

Poll the progress of a running or completed job.

**Response `200 OK`**

```json
{
  "success": true,
  "jobId": "550e8400-...",
  "status": "processing",
  "filename": "diamonds.csv",
  "total_rows": 500,
  "processed": 125,
  "succeeded": 123,
  "failed": 2,
  "progress_pct": 25,
  "errors": [
    {
      "row_index": 14,
      "certificate_num": "7245XXXXX",
      "message": "No 360 data returned for certificate",
      "timestamp": "2024-03-26T10:00:00Z"
    }
  ],
  "started_at": "2024-03-26T09:55:00Z",
  "completed_at": null
}
```

`status` values: `queued` → `processing` → `completed` | `failed`

---

### `GET /health`

Liveness probe.

```json
{ "status": "ok", "ts": "2024-03-26T10:00:00.000Z" }
```

---

## CSV Column Mapping

The pipeline auto-detects columns regardless of header capitalisation or minor naming differences. Supported aliases per field:

| Canonical field   | Accepted CSV headers                                          |
|-------------------|---------------------------------------------------------------|
| `certificate_num` | ReportNo, report_no, CertificateNo, cert_no, ReportNumber … |
| `stock_num`       | stockId, stock_id, StockNum, ID …                            |
| `video_url`       | video, video_url, videoUrl, v360url …                        |
| `shape`           | shape                                                         |
| `carat`           | carats, carat, weight                                         |
| `color`           | col, color, colour                                            |
| `clarity`         | clar, clarity                                                 |
| `cut`             | cut                                                           |
| `polish`          | pol, polish                                                   |
| `symmetry`        | symm, symmetry, sym                                           |
| `fluorescence`    | flo, fluorescence, fluor                                      |
| `depth_percent`   | depth, depth_percent, depth%                                  |
| `table_percent`   | table, table_percent, table%                                  |
| `location`        | country, location, loc                                        |
| `our_price`       | price, our_price, deliveredPrice                              |

---

## MongoDB Schema

```js
{
  stock_num:       String,
  certificate_num: String,   // unique index
  shape:           String,
  carat:           Number,
  clarity:         String,
  color:           String,
  cut:             String,
  polish:          String,
  symmetry:        String,
  fluorescence:    String,
  depth_percent:   Number,
  table_percent:   Number,
  length:          Number,
  width:           Number,
  depth:           Number,
  l_w_ratio:       Number,
  girdle:          String,
  culet_size:      String,
  lab:             String,
  location:        String,
  our_price:       Number,
  image_url:       String,
  strips:          [String],   // 10 S3 URLs
  strip_meta: {
    total_frames:     Number,
    used_frames:      Number,
    ignored_frames:   Number,
    frames_per_strip: Number,
    total_strips:     Number   // always 10
  },
  processing_status: 'pending' | 'processing' | 'completed' | 'failed',
  error_message:   String,
  processed_at:    Date
}
```

---

## Project Structure

```
diamond360/
├── src/
│   ├── app.js                          # Express entry point
│   ├── controllers/
│   │   └── upload.controller.js        # Route handlers
│   ├── middleware/
│   │   └── upload.middleware.js        # Multer CSV upload
│   ├── models/
│   │   ├── diamond.model.js            # Mongoose Diamond schema
│   │   └── job.model.js                # Mongoose Job schema
│   ├── queues/
│   │   └── queue.js                    # BullMQ queue definition
│   ├── routes/
│   │   └── index.js                    # Express router
│   ├── services/
│   │   ├── csv.service.js              # CSV parsing + column mapping
│   │   ├── video.service.js            # GraphQL API calls
│   │   ├── frame.service.js            # Frame downloading
│   │   ├── strip.service.js            # Strip generation (strict equal)
│   │   ├── s3.service.js               # AWS S3 uploads
│   │   └── cleanup.service.js          # Temp file removal
│   ├── utils/
│   │   ├── logger.js                   # Winston logger
│   │   ├── retry.js                    # Retry with back-off
│   │   └── columnMapper.js             # CSV header normalisation
│   └── workers/
│       └── processor.worker.js         # BullMQ background worker
├── uploads/                            # Temp CSV storage (git-ignored)
├── logs/                               # Log files (git-ignored)
├── .env.example
├── .gitignore
├── package.json
└── README.md
```

---

## Error Handling

- Every GraphQL / S3 / download call retries **3 times** with linear back-off (2 s, 4 s, 6 s).
- If a row still fails after all retries, the error is logged, the row is skipped, and the **next row continues**.
- The `errors[]` array on the Job document records every per-row failure with its certificate number, message, and timestamp.
- Failed rows set `processing_status: 'failed'` on the Diamond document, preserving a full audit trail.

---

## Security Notes

- Credentials are loaded exclusively from environment variables — never hardcoded.
- `.env` is in `.gitignore`.
- Uploaded files are validated to `.csv` extension only (Multer file filter).
- File size is capped at 50 MB.

---

## Scaling Notes

- To process multiple CSV jobs concurrently, increase the BullMQ worker `concurrency` setting in `processor.worker.js`. Rows within each job remain strictly sequential.
- Spin up additional worker processes on separate machines — they all share the same Redis queue and MongoDB.
- S3 uploads use `@aws-sdk/lib-storage`'s multipart `Upload` helper, which handles large files efficiently.
#   d a i m o n d 3 6 0  
 #   d a i m o n d 3 6 0  
 