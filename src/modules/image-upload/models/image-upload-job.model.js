'use strict';

const mongoose = require('mongoose');

const imageUploadJobSchema = new mongoose.Schema(
  {
    jobId: { type: String, required: true, unique: true, index: true },

    status: {
      type: String,
      enum: ['pending', 'running', 'completed', 'failed', 'cancelled'],
      default: 'pending',
      index: true,
    },

    collections: [
      {
        collection: { type: String, required: true },
        s3Folder:   { type: String, required: true },
        total:      { type: Number, default: 0 },
        processed:  { type: Number, default: 0 },
        success:    { type: Number, default: 0 },
        skipped:    { type: Number, default: 0 },
        partial:    { type: Number, default: 0 },  // image_url_old exists but image_url missing
        failed:     { type: Number, default: 0 },
        status: {
          type: String,
          enum: ['pending', 'running', 'completed', 'failed'],
          default: 'pending',
        },
      },
    ],

    total_docs:      { type: Number, default: 0 },
    total_processed: { type: Number, default: 0 },
    total_success:   { type: Number, default: 0 },
    total_skipped:   { type: Number, default: 0 },
    total_partial:   { type: Number, default: 0 },
    total_failed:    { type: Number, default: 0 },

    is_cancelled:  { type: Boolean, default: false },
    started_at:    { type: Date,    default: null },
    finished_at:   { type: Date,    default: null },
    error_message: { type: String,  default: null },
  },
  {
    timestamps: true,
    collection: 'image_upload_jobs',
    suppressReservedKeysWarning: true
  }
);

module.exports = mongoose.model('ImageUploadJob', imageUploadJobSchema);