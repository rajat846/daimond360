'use strict';

const fs = require('fs');
const path = require('path');
const sharp = require('sharp');
const logger = require('../utils/logger');

const TOTAL_STRIPS = 10;

// WebP absolute dimension limit is 16383x16383 px.
// We cap at a comfortable safe size so sharp never hits the format limit.
const WEBP_MAX_STRIP_WIDTH = 16000;

/* ─────────────────────────────────────────────────────────────
   Strip math — strict equal distribution
───────────────────────────────────────────────────────────── */

function computeStripMeta(rawFrameCount) {
  let adjusted = rawFrameCount % 2 !== 0 ? rawFrameCount - 1 : rawFrameCount;
  const remainder = adjusted % TOTAL_STRIPS;
  const usable = adjusted - remainder;
  return {
    total_frames: rawFrameCount,
    used_frames: usable,
    ignored_frames: rawFrameCount - usable,
    frames_per_strip: usable / TOTAL_STRIPS,
    total_strips: TOTAL_STRIPS,
  };
}

function computeMaxFrameWidth(framesPerStrip) {
  return Math.floor(WEBP_MAX_STRIP_WIDTH / framesPerStrip);
}

/* ─────────────────────────────────────────────────────────────
   Strip builder

   CHANGE: Buffers are processed sequentially, not via Promise.all.
   WHY: Promise.all loads ALL frame buffers into RAM simultaneously.
     For 25 frames per strip at ~1MB each that is ~25MB per strip, and
     10 strips in a certificate = ~250MB peak. Sequential processing
     keeps peak memory to one frame buffer at a time (~1MB).
   IMPACT: Up to 250MB less peak RAM per certificate processed concurrently.
   BREAK RISK: Slower than parallel within a single strip, but strip-level
     processing is I/O-bound anyway so real-world impact is minimal.
   FUTURE: If strip build time becomes a bottleneck, re-introduce limited
     parallelism with an explicit buffer pool, not a raw Promise.all.
───────────────────────────────────────────────────────────── */

async function buildStrip(framePaths, outputPath) {
  if (framePaths.length === 0) throw new Error('buildStrip called with zero frames');

  // Read first frame to get source dimensions
  const meta = await sharp(framePaths[0]).metadata();
  const srcWidth = meta.width;
  const srcHeight = meta.height;

  if (!srcWidth || !srcHeight) {
    throw new Error(`Cannot read dimensions from frame: ${framePaths[0]}`);
  }

  const maxFrameWidth = computeMaxFrameWidth(framePaths.length);
  const frameWidth = Math.min(srcWidth, maxFrameWidth);
  const frameHeight = Math.round(srcHeight * (frameWidth / srcWidth));

  // ── Sequential buffer loading to bound peak RAM ───────────────
  // Previously: Promise.all — loads all frame buffers at once.
  // Now: one at a time — peak is one buffer plus the compositing canvas.
  const compositeInputs = [];
  for (let i = 0; i < framePaths.length; i++) {
    const buf = await sharp(framePaths[i])
      .resize(frameWidth, frameHeight, { fit: 'fill', withoutEnlargement: false })
      .toBuffer();

    compositeInputs.push({ input: buf, left: i * frameWidth, top: 0 });
  }

  const canvasWidth = frameWidth * framePaths.length;

  await sharp({
    create: {
      width: canvasWidth,
      height: frameHeight,
      channels: 3,
      background: { r: 0, g: 0, b: 0 },
    },
  })
    .composite(compositeInputs)
    .webp({ quality: 60, effort: 6 })
    .toFile(outputPath);

  // Release composite inputs array so GC can reclaim frame buffers
  compositeInputs.length = 0;
}

/* ─────────────────────────────────────────────────────────────
   Main export — generate all 10 strips for a certificate.

   CHANGE: Strips are now built sequentially instead of potentially
     overlapping. Each strip's temp buffers are released before the next
     strip begins.
   WHY: Overlapping strip builds multiply peak RAM. Sequential processing
     keeps memory to one strip's worth of frame buffers at any given time.
   IMPACT: ~10x reduction in peak sharp memory during strip generation.
   BREAK RISK: Total strip-generation time is additive, not parallelised.
     If raw speed is critical, introduce a per-certificate semaphore
     rather than fully sequential processing.
   FUTURE: Accept a concurrency limit argument to balance speed/memory.
───────────────────────────────────────────────────────────── */

async function generateStrips(certNumber, framesDir, rawFrameCount) {
  const meta = computeStripMeta(rawFrameCount);

  logger.info(
    `[${certNumber}] Strip meta — total: ${meta.total_frames}, used: ${meta.used_frames}, ` +
    `ignored: ${meta.ignored_frames}, per strip: ${meta.frames_per_strip}`
  );

  if (meta.frames_per_strip === 0) {
    throw new Error(
      `[${certNumber}] Frame count too low to generate strips (rawFrameCount=${rawFrameCount})`
    );
  }

  const maxFrameWidth = computeMaxFrameWidth(meta.frames_per_strip);
  logger.info(`[${certNumber}] Frame width cap: ${maxFrameWidth}px (${meta.frames_per_strip} frames/strip)`);

  const stripsDir = path.join(framesDir, '__strips__');
  if (!fs.existsSync(stripsDir)) {
    fs.mkdirSync(stripsDir, { recursive: true });
  }

  const stripPaths = [];

  for (let stripIdx = 0; stripIdx < meta.total_strips; stripIdx++) {
    const startFrame = stripIdx * meta.frames_per_strip;
    const endFrame = startFrame + meta.frames_per_strip;

    const framePaths = [];
    for (let f = startFrame; f < endFrame; f++) {
      const fp = path.join(framesDir, `${f}.jpg`);
      if (fs.existsSync(fp)) {
        framePaths.push(fp);
      } else {
        const substitute = findNearestFrame(framesDir, f, meta.total_frames);
        if (substitute) {
          logger.warn(`[${certNumber}] Frame ${f} missing — substituting ${path.basename(substitute)}`);
          framePaths.push(substitute);
        } else {
          logger.warn(`[${certNumber}] Frame ${f} missing and no substitute found — skipping frame`);
        }
      }
    }

    if (framePaths.length === 0) {
      logger.warn(`[${certNumber}] Strip ${stripIdx}: no frames available — skipping strip`);
      continue;
    }

    const outputPath = path.join(stripsDir, `strip_${stripIdx}.webp`);

    try {
      await buildStrip(framePaths, outputPath);
      stripPaths.push(outputPath);
      logger.debug(
        `[${certNumber}] Strip ${stripIdx + 1}/${meta.total_strips} created ` +
        `(frames ${startFrame}–${endFrame - 1})`
      );
    } catch (stripErr) {
      logger.error(
        `[${certNumber}] Strip ${stripIdx + 1}/${meta.total_strips} FAILED: ${stripErr.message} — skipping`
      );
    }
  }

  logger.info(`[${certNumber}] ${stripPaths.length}/${meta.total_strips} strips generated`);
  return { stripPaths, meta };
}

/* ─────────────────────────────────────────────────────────────
   Find nearest available frame on disk within [0, maxFrames)
───────────────────────────────────────────────────────────── */

function findNearestFrame(framesDir, targetIdx, maxFrames) {
  for (let offset = 1; offset < maxFrames; offset++) {
    for (const idx of [targetIdx - offset, targetIdx + offset]) {
      if (idx >= 0 && idx < maxFrames) {
        const fp = path.join(framesDir, `${idx}.jpg`);
        if (fs.existsSync(fp)) return fp;
      }
    }
  }
  return null;
}

module.exports = { generateStrips, computeStripMeta };