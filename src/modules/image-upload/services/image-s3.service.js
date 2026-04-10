'use strict';

const https  = require('https');
const http   = require('http');
const sharp  = require('sharp');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload }   = require('@aws-sdk/lib-storage');
const logger       = require('../../../utils/logger');
const { withRetry } = require('../../../utils/retry');

/**
 * MIGRATE mode:  1200px + quality 60  → ~30% of original size
 * UPLOAD mode:   3000px + quality 85  → high quality, no aggressive compression
 */
const UPLOAD_MAX_PX  = 3000;
const MIGRATE_MAX_PX = 1200;
const UPLOAD_QUALITY  = 85;
const MIGRATE_QUALITY = 60;
const SHARP_OPTIONS   = { limitInputPixels: false };

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
   Download URL → Buffer
───────────────────────────────────────────── */
function downloadToBuffer(url) {
  return new Promise((resolve, reject) => {
    const transport = url.startsWith('https') ? https : http;
    const chunks    = [];

    const req = transport.get(url, (res) => {
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        return downloadToBuffer(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} from ${url}`));
      }
      res.on('data',  (chunk) => chunks.push(chunk));
      res.on('end',   () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });

    req.on('error', reject);
    req.setTimeout(30_000, () => {
      req.destroy();
      reject(new Error('Download timeout after 30s'));
    });
  });
}

/* ─────────────────────────────────────────────
   Convert buffer → WebP
   mode: 'upload' (high quality) | 'migrate' (~30% size)
───────────────────────────────────────────── */
async function toWebpBuffer(rawBuffer, mode = 'upload') {
  const maxPx   = mode === 'migrate' ? MIGRATE_MAX_PX : UPLOAD_MAX_PX;
  const quality = mode === 'migrate' ? MIGRATE_QUALITY : UPLOAD_QUALITY;

  return sharp(rawBuffer, SHARP_OPTIONS)
    .resize(maxPx, maxPx, { fit: 'inside', withoutEnlargement: true })
    .webp({ quality })
    .toBuffer();
}

/* ─────────────────────────────────────────────
   Upload buffer → S3
───────────────────────────────────────────── */
async function uploadBufferToS3(buffer, s3Key) {
  const bucket = process.env.AWS_S3_BUCKET;

  return withRetry(async () => {
    const uploader = new Upload({
      client: getS3Client(),
      params: {
        Bucket:      bucket,
        Key:         s3Key,
        Body:        buffer,
        ContentType: 'image/webp',
      },
    });
    const result = await uploader.done();
    return result.Location || `https://${bucket}.s3.amazonaws.com/${s3Key}`;
  }, 3, 2000, s3Key);
}

/* ─────────────────────────────────────────────
   Upload mode: download → WebP (high quality) → S3
   Used by the existing upload worker.
───────────────────────────────────────────── */
async function downloadConvertUpload(imageUrl, s3Key, certNum) {
  logger.info(`[img-upload][${certNum}] Downloading: ${imageUrl}`);
  const rawBuffer  = await downloadToBuffer(imageUrl);

  logger.info(`[img-upload][${certNum}] Converting to WebP (${rawBuffer.length} bytes)`);
  const webpBuffer = await toWebpBuffer(rawBuffer, 'upload');

  logger.info(`[img-upload][${certNum}] Uploading to S3: ${s3Key}`);
  const s3Url = await uploadBufferToS3(webpBuffer, s3Key);

  logger.info(`[img-upload][${certNum}] Done → ${s3Url}`);
  return { s3Url };
}

/* ─────────────────────────────────────────────
   Migrate mode: download → WebP (~30% size) → S3
   Used by the migration worker.
───────────────────────────────────────────── */
async function downloadCompressUpload(imageUrl, s3Key, certNum) {
  logger.info(`[img-migrate][${certNum}] Downloading: ${imageUrl}`);
  const rawBuffer  = await downloadToBuffer(imageUrl);

  logger.info(`[img-migrate][${certNum}] Compressing to ~30% (${rawBuffer.length} bytes)`);
  const webpBuffer = await toWebpBuffer(rawBuffer, 'migrate');

  const ratio = ((webpBuffer.length / rawBuffer.length) * 100).toFixed(1);
  logger.info(`[img-migrate][${certNum}] Compressed: ${webpBuffer.length} bytes (${ratio}% of original)`);

  logger.info(`[img-migrate][${certNum}] Uploading to S3: ${s3Key}`);
  const s3Url = await uploadBufferToS3(webpBuffer, s3Key);

  logger.info(`[img-migrate][${certNum}] Done → ${s3Url}`);
  return { s3Url };
}

module.exports = { downloadConvertUpload, downloadCompressUpload };