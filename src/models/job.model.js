'use strict';

const mongoose = require('mongoose');

const jobSchema = new mongoose.Schema(
  {
    jobId: { type: String, required: true, unique: true, index: true },

    // Overall state
    status: {
      type: String,
      enum: ['queued', 'processing', 'completed', 'failed', 'cancelled'],
      default: 'queued',
    },

    // Cancellation flag — worker polls this after every row
    is_cancelled: { type: Boolean, default: false },

    // File info
    filename:   { type: String },
    total_rows: { type: Number, default: 0 },

    // Progress counters
    processed:  { type: Number, default: 0 },
    succeeded:  { type: Number, default: 0 },
    failed:     { type: Number, default: 0 },
    skipped:    { type: Number, default: 0 }, // already-completed diamonds

    // Per-row failure log
    row_errors: [
      {
        row_index:       Number,
        certificate_num: String,
        message:         String,
        timestamp:       { type: Date, default: Date.now },
      },
    ],

    started_at:    { type: Date, default: null },
    completed_at:  { type: Date, default: null },
    cancelled_at:  { type: Date, default: null },
  },
  {
    timestamps: true,
    collection: 'processing_jobs',
  }
);

module.exports = mongoose.model('Job', jobSchema);