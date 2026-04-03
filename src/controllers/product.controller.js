'use strict';

const Diamond = require('../models/diamond_v1.model');
const logger = require('../utils/logger');

/**
 * GET /api/products
 *
 * Returns a paginated, sorted list of diamonds (products) from MongoDB.
 *
 * Query params:
 *   page    {number}  — 1-based page number          (default: 1)
 *   limit   {number}  — records per page, max 100    (default: 10)
 *   status  {string}  — filter by processing_status  (optional)
 *   shape   {string}  — filter by shape              (optional)
 *   lab     {string}  — filter by grading lab        (optional)
 *   sort    {string}  — field to sort by             (default: createdAt)
 *   order   {asc|desc}                               (default: desc)
 */
async function listProducts(req, res) {
  try {
    // ── Parse & validate query params ─────────────────────────────
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit, 10) || 10));
    const skip = (page - 1) * limit;

    // Allowed sort fields — whitelist to prevent injection
    const SORTABLE = new Set([
      'createdAt', 'processed_at', 'carat', 'our_price',
      'shape', 'color', 'clarity', 'certificate_num', 'stock_num',
    ]);
    const sortField = SORTABLE.has(req.query.sort) ? req.query.sort : 'createdAt';
    const sortOrder = req.query.order === 'asc' ? 1 : -1;

    // ── Build filter ───────────────────────────────────────────────
    const filter = {};

    if (req.query.status) {
      const VALID_STATUSES = ['pending', 'processing', 'completed', 'failed'];
      if (!VALID_STATUSES.includes(req.query.status)) {
        return res.status(400).json({
          success: false,
          error: `Invalid status. Must be one of: ${VALID_STATUSES.join(', ')}`,
        });
      }
      filter.processing_status = req.query.status;
    }

    if (req.query.shape) {
      filter.shape = { $regex: new RegExp(`^${req.query.shape}$`, 'i') };
    }

    if (req.query.lab) {
      filter.lab = { $regex: new RegExp(`^${req.query.lab}$`, 'i') };
    }

    const finalFilter = {
      ...filter,
      processing_status: "completed", // ✅ fixed spelling
    };

    // ── Query DB ───────────────────────────────────────────────────
    const [diamonds, total] = await Promise.all([
      Diamond.find(finalFilter)
        .select(
          'stock_num certificate_num shape carat color clarity cut ' +
          'polish symmetry fluorescence lab location our_price ' +
          'strip_meta createdAt has_video has_img length depth l_w_ratio'
        )
        .sort({ [sortField]: sortOrder })
        .skip(skip)
        .limit(limit)
        .lean(),
      Diamond.countDocuments(finalFilter),
    ]);

    // ── Shape response ─────────────────────────────────────────────
    const data = diamonds.map((d) => ({
      id: d._id,
      stock_num: d.stock_num,
      certificate_num: d.certificate_num,
      name: `${d.shape || 'Diamond'} ${d.carat ? d.carat + 'ct' : ''} ${d.color || ''} ${d.clarity || ''}`.trim(),
      shape: d.shape,
      carat: d.carat,
      color: d.color,
      clarity: d.clarity,
      cut: d.cut,
      polish: d.polish,
      symmetry: d.symmetry,
      fluorescence: d.fluorescence,
      lab: d.lab,
      location: d.location,
      l_w_ratio: d.l_w_ratio == null ? (d.length && d.depth ? Number((d.length / d.depth).toFixed(2)) : null) : d.l_w_ratio, // fallback for older records
      our_price: d.our_price,
      strip_meta: { ...d.strip_meta, has_video: d.has_video, has_img: d.has_img },
      created_at: d.createdAt,
    }));

    return res.json({
      success: true,
      pagination: {
        page,
        limit,
        total,
        total_pages: Math.ceil(total / limit),
        has_next: page < Math.ceil(total / limit),
        has_prev: page > 1,
      },
      data,
    });
  } catch (err) {
    logger.error(`listProducts controller error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * GET /api/products/:certificateNum
 *
 * Returns a single diamond by certificate number.
 */
async function getProduct(req, res) {
  try {
    const diamond = await Diamond.findOne({
      certificate_num: req.params.certificateNum,
    }).lean();

    if (!diamond) {
      return res.status(404).json({
        success: false,
        error: `Product not found: ${req.params.certificateNum}`,
      });
    }

    return res.json({ success: true, data: diamond });
  } catch (err) {
    logger.error(`getProduct controller error: ${err.message}`);
    return res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = { listProducts, getProduct };