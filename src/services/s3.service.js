'use strict';

const fs     = require('fs');
const https  = require('https');
const http   = require('http');
const sharp  = require('sharp');
const { S3Client, HeadBucketCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const logger = require('../utils/logger');
const { withRetry } = require('../utils/retry');

// WebP hard limit is 16383×16383 px.
// We cap at a comfortable safe size so sharp never hits the format limit.
const WEBP_SAFE_PX   = 3000;
const SHARP_NO_LIMIT = { limitInputPixels: false };

let _s3Client = null;

function getS3Client() {
  if (!_s3Client) {
    _s3Client = new S3Client({
      region: process.env.AWS_REGION || 'us-east-1',
      credentials: {
        accessKeyId:     process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }
  return _s3Client;
}

/* ─────────────────────────────────────────────
   Upload helpers
───────────────────────────────────────────── */

async function uploadFileToS3(localPath, s3Key, contentType = 'image/webp') {
  const bucket = process.env.AWS_S3_BUCKET;

  try {
    return await withRetry(async () => {
      const fileStream = fs.createReadStream(localPath);
      const uploader = new Upload({
        client: getS3Client(),
        params: { Bucket: bucket, Key: s3Key, Body: fileStream, ContentType: contentType },
      });
      const result = await uploader.done();
      return result.Location || '';
    }, 3, 2000);
  } catch {
    return '';
  }
}

async function uploadBufferToS3(buffer, s3Key, contentType = 'image/webp') {
  const bucket = process.env.AWS_S3_BUCKET;

  try {
    return await withRetry(async () => {
      const uploader = new Upload({
        client: getS3Client(),
        params: { Bucket: bucket, Key: s3Key, Body: buffer, ContentType: contentType },
      });
      const result = await uploader.done();
      return result.Location || '';
    }, 3, 2000);
  } catch {
    return '';
  }
}

/* ─────────────────────────────────────────────
   Upload strips
───────────────────────────────────────────── */

async function uploadStripsToS3(certNumber, stripPaths) {
  const uploadedUrls = [];

  for (let i = 0; i < stripPaths.length; i++) {
    try {
      const s3Key = `360-video/${certNumber}/strip_${i}.webp`;
      const url   = await uploadFileToS3(stripPaths[i], s3Key, 'image/webp');
      uploadedUrls.push(url || '');
    } catch (err) {
      logger.error(`[${certNumber}] Strip ${i} upload failed: ${err.message}`);
      uploadedUrls.push('');
    }
  }

  return uploadedUrls;
}

/* ─────────────────────────────────────────────
   Upload main image

   4-stage fallback strategy — this function NEVER throws:

   Stage 0  — No URL in CSV            → return '' immediately (not an error)
   Stage 1  — Download URL             → if download fails, return '' with warning
   Stage 2  — Convert to WebP (≤3000px) → if success, upload as main.webp + return
   Stage 3  — Convert to JPEG (≤3000px) → if success, upload as main.jpg + return
   Stage 4  — Upload raw bytes as-is   → use extension from Content-Type / URL
   If all stages fail                  → return '' (non-fatal)
───────────────────────────────────────────── */

async function uploadMainImageToS3(certNumber, imageUrl) {
  // ── Stage 0: No URL ───────────────────────────────────────────
  if (!imageUrl || typeof imageUrl !== 'string' || imageUrl.trim() === '') {
    logger.info(`[${certNumber}] No image URL in CSV — skipping image upload`);
    return '';
  }

  // ── Stage 1: Download ─────────────────────────────────────────
  let imageBuffer;
  let originalContentType = 'image/jpeg';

  try {
    const result = await downloadToBuffer(imageUrl);
    imageBuffer          = result.buffer;
    originalContentType  = result.contentType || 'image/jpeg';
    logger.info(`[${certNumber}] Image downloaded (${imageBuffer.length} bytes, type: ${originalContentType})`);
  } catch (err) {
    logger.warn(`[${certNumber}] Image download failed: ${err.message} — skipping image upload`);
    return '';
  }

  // ── Stage 2: WebP conversion ──────────────────────────────────
  try {
    const webpBuffer = await sharp(imageBuffer, SHARP_NO_LIMIT)
      .resize(WEBP_SAFE_PX, WEBP_SAFE_PX, { fit: 'inside', withoutEnlargement: true })
      .webp({ quality: 85 })
      .toBuffer();

    const url = await uploadBufferToS3(webpBuffer, `360-video/${certNumber}/main.webp`, 'image/webp');
    if (url) {
      logger.info(`[${certNumber}] Image uploaded as WebP: ${url}`);
      return url;
    }
    logger.warn(`[${certNumber}] WebP S3 upload returned empty — trying JPEG`);
  } catch (webpErr) {
    logger.warn(`[${certNumber}] WebP conversion failed (${webpErr.message}) — trying JPEG`);
  }

  // ── Stage 3: JPEG conversion ──────────────────────────────────
  try {
    const jpegBuffer = await sharp(imageBuffer, SHARP_NO_LIMIT)
      .resize(WEBP_SAFE_PX, WEBP_SAFE_PX, { fit: 'inside', withoutEnlargement: true })
      .jpeg({ quality: 85 })
      .toBuffer();

    const url = await uploadBufferToS3(jpegBuffer, `360-video/${certNumber}/main.jpg`, 'image/jpeg');
    if (url) {
      logger.info(`[${certNumber}] Image uploaded as JPEG: ${url}`);
      return url;
    }
    logger.warn(`[${certNumber}] JPEG S3 upload returned empty — trying raw upload`);
  } catch (jpegErr) {
    logger.warn(`[${certNumber}] JPEG conversion failed (${jpegErr.message}) — trying raw upload`);
  }

  // ── Stage 4: Raw upload with original extension ───────────────
  try {
    const ext    = extensionFromContentType(originalContentType, imageUrl);
    const s3Key  = `360-video/${certNumber}/main.${ext}`;
    const url    = await uploadBufferToS3(imageBuffer, s3Key, originalContentType);
    if (url) {
      logger.info(`[${certNumber}] Image uploaded as raw .${ext}: ${url}`);
      return url;
    }
    logger.warn(`[${certNumber}] Raw image S3 upload also returned empty — no image stored`);
  } catch (rawErr) {
    logger.warn(`[${certNumber}] Raw image upload failed (${rawErr.message}) — no image stored`);
  }

  return '';
}

/* ─────────────────────────────────────────────
   Extension from Content-Type / URL
───────────────────────────────────────────── */

function extensionFromContentType(contentType, url) {
  const CT_MAP = {
    'image/jpeg': 'jpg',
    'image/jpg':  'jpg',
    'image/png':  'png',
    'image/gif':  'gif',
    'image/webp': 'webp',
    'image/bmp':  'bmp',
    'image/tiff': 'tiff',
  };
  const ct = (contentType || '').toLowerCase().split(';')[0].trim();
  if (CT_MAP[ct]) return CT_MAP[ct];

  // Fallback: extract from URL path
  const match = url && url.match(/\.([a-zA-Z]{3,4})(?:\?|$)/);
  if (match) return match[1].toLowerCase();

  return 'jpg'; // safe default
}

/* ─────────────────────────────────────────────
   Download helper — returns { buffer, contentType }
   Follows redirects, 30s timeout
───────────────────────────────────────────── */

function downloadToBuffer(url) {
  return new Promise((resolve, reject) => {
    const transport = url.startsWith('https') ? https : http;
    const chunks    = [];

    const req = transport.get(url, (res) => {
      // Follow redirects
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        return downloadToBuffer(res.headers.location).then(resolve).catch(reject);
      }

      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} from ${url}`));
      }

      const contentType = res.headers['content-type'] || '';
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({ buffer: Buffer.concat(chunks), contentType }));
      res.on('error', reject);
    });

    req.on('error', reject);
    req.setTimeout(30000, () => {
      req.destroy();
      reject(new Error('Download timeout after 30s'));
    });
  });
}

/* ─────────────────────────────────────────────
   Verify S3 connection on startup
───────────────────────────────────────────── */

async function verifyS3Connection() {
  try {
    const bucket = process.env.AWS_S3_BUCKET;
    await getS3Client().send(new HeadBucketCommand({ Bucket: bucket }));
    logger.info(`S3 connected — bucket: ${bucket}`);
  } catch (err) {
    logger.warn(`S3 connection check failed: ${err.message}`);
  }
}

module.exports = {
  uploadStripsToS3,
  uploadMainImageToS3,
  uploadFileToS3,
  uploadBufferToS3,
  verifyS3Connection,
};