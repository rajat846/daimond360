'use strict';

const mongoose = require('mongoose');

const stripMetaSchema = new mongoose.Schema(
  {
    total_frames:     { type: Number, required: true },
    used_frames:      { type: Number, required: true },
    ignored_frames:   { type: Number, required: true },
    frames_per_strip: { type: Number, required: true },
    total_strips:     { type: Number, required: true, default: 10 },
  },
  { _id: false }
);

const diamondSchema = new mongoose.Schema(
  {
    // Identity
    // stock_num is NOT required — some CSV rows may omit it.
    // certificate_num is the true unique key.
    stock_num:       { type: String, default: null, index: true },
    certificate_num: { type: String, required: true, unique: true },

    // Diamond attributes
    shape:        { type: String, default: null },
    carat:        { type: Number, default: null },
    clarity:      { type: String, default: null },
    color:        { type: String, default: null },
    cut:          { type: String, default: null },
    polish:       { type: String, default: null },
    symmetry:     { type: String, default: null },
    fluorescence: { type: String, default: null },

    // Measurements
    depth_percent: { type: Number, default: null },
    table_percent: { type: Number, default: null },
    length:        { type: Number, default: null },
    width:         { type: Number, default: null },
    depth:         { type: Number, default: null },
    l_w_ratio:     { type: Number, default: null },

    // Grading
    girdle:     { type: String, default: null },
    culet_size: { type: String, default: null },
    lab:        { type: String, default: null },

    // Origin / pricing
    location:  { type: String, default: null },
    our_price: { type: Number, default: null },

    // Media — from CSV
    image_url: { type: String, default: null },  // original CSV image URL

    // Media — uploaded to S3
    main_image_s3: { type: String, default: null }, // 360-video/<cert>/main.webp (or .jpg)

    // 360 video
    has_video: { type: Boolean, default: false },   // false when API returns no v360 data
    has_img:   { type: Boolean, default: false },   // false when CSV image_url is missing or upload failed
    strips:    { type: [String], default: [] },      // up to 10 S3 WebP strip URLs

    // Strip metadata
    strip_meta: { type: stripMetaSchema, default: null },

    // Processing state
    processing_status: {
      type: String,
      enum: ['pending', 'processing', 'completed', 'failed'],
      default: 'pending',
      index: true,
    },
    error_message: { type: String, default: null },
    processed_at:  { type: Date, default: null },
  },
  {
    timestamps: true,
    collection: 'diamonds',
  }
);

module.exports = mongoose.model('Diamond', diamondSchema);