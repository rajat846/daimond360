'use strict';

const fs     = require('fs');
const path   = require('path');
const sharp  = require('sharp');
const logger = require('../utils/logger');

const TOTAL_STRIPS = 10;

// WebP absolute dimension limit is 16383×16383.
// A strip is: frameWidth × framesPerStrip wide.
// We cap frame width so the strip canvas always fits within this limit.
// e.g. 25 frames/strip → max frame width = floor(16383 / 25) = 655px
// We use a safe buffer (16000) instead of the hard limit.
const WEBP_MAX_STRIP_WIDTH = 16000;

/* ─────────────────────────────────────────────────────────────
   Strip math — strict equal distribution
───────────────────────────────────────────────────────────── */

function computeStripMeta(rawFrameCount) {
  let adjusted    = rawFrameCount % 2 !== 0 ? rawFrameCount - 1 : rawFrameCount;
  const remainder = adjusted % TOTAL_STRIPS;
  const usable    = adjusted - remainder;
  return {
    total_frames:     rawFrameCount,
    used_frames:      usable,
    ignored_frames:   rawFrameCount - usable,
    frames_per_strip: usable / TOTAL_STRIPS,
    total_strips:     TOTAL_STRIPS,
  };
}

/* ─────────────────────────────────────────────────────────────
   Compute the max safe frame width for a given strip size.

   The strip canvas = frameWidth × framesPerStrip.
   To stay under WEBP_MAX_STRIP_WIDTH we cap frameWidth at:
     floor(WEBP_MAX_STRIP_WIDTH / framesPerStrip)

   We also never upscale — if the original frame is already smaller
   than the cap, we use the original width.
───────────────────────────────────────────────────────────── */

function computeMaxFrameWidth(framesPerStrip) {
  return Math.floor(WEBP_MAX_STRIP_WIDTH / framesPerStrip);
}

/* ─────────────────────────────────────────────────────────────
   Strip builder
   - Reads the first frame to get canonical dimensions
   - Caps frame width so strip canvas never exceeds WebP limit
   - Forces ALL frames to exactly the capped dimensions (fixes
     "Image to composite must have same dimensions or smaller")
   - Composites frames horizontally left → right
   - Outputs as WebP (quality 85)
───────────────────────────────────────────────────────────── */

async function buildStrip(framePaths, outputPath) {
  if (framePaths.length === 0) throw new Error('buildStrip called with zero frames');

  // Read first frame to get source dimensions
  const meta = await sharp(framePaths[0]).metadata();
  const srcWidth  = meta.width;
  const srcHeight = meta.height;

  if (!srcWidth || !srcHeight) {
    throw new Error(`Cannot read dimensions from frame: ${framePaths[0]}`);
  }

  // Compute the capped frame size — never exceed WebP strip width limit
  const maxFrameWidth = computeMaxFrameWidth(framePaths.length);
  const frameWidth    = Math.min(srcWidth, maxFrameWidth);
  // Scale height proportionally to maintain aspect ratio
  const frameHeight   = Math.round(srcHeight * (frameWidth / srcWidth));

  // Read + resize all frame buffers in parallel.
  // fit:'fill' forces EXACT dimensions so composite never rejects them.
  const buffers = await Promise.all(
    framePaths.map((p) =>
      sharp(p)
        .resize(frameWidth, frameHeight, {
          fit: 'fill',
          withoutEnlargement: false,
        })
        .toBuffer()
    )
  );

  const canvasWidth = frameWidth * framePaths.length;

  // Composite all frames side-by-side
  await sharp({
    create: {
      width:    canvasWidth,
      height:   frameHeight,
      channels: 3,
      background: { r: 0, g: 0, b: 0 },
    },
  })
    .composite(
      buffers.map((buf, i) => ({
        input: buf,
        left:  i * frameWidth,
        top:   0,
      }))
    )
    .webp({ quality: 85 })
    .toFile(outputPath);
}

/* ─────────────────────────────────────────────────────────────
   Main export — generate all 10 strips for a certificate.

   Individual strip failures are caught and logged — a single
   bad strip produces an empty string in the array rather than
   aborting the entire certificate.
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

  for (let stripIdx = 0; stripIdx < TOTAL_STRIPS; stripIdx++) {
    const startFrame = stripIdx * meta.frames_per_strip;
    const endFrame   = startFrame + meta.frames_per_strip;

    // Collect frames — substitute missing ones with the nearest available
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
        `[${certNumber}] Strip ${stripIdx + 1}/${TOTAL_STRIPS} created ` +
        `(frames ${startFrame}–${endFrame - 1}, canvas=${Math.min(framePaths[0] ? 0 : 0, maxFrameWidth) * framePaths.length}px)`
      );
    } catch (stripErr) {
      // One bad strip must not abort the whole certificate
      logger.error(
        `[${certNumber}] Strip ${stripIdx + 1}/${TOTAL_STRIPS} FAILED: ${stripErr.message} — skipping`
      );
    }
  }

  logger.info(`[${certNumber}] ${stripPaths.length}/${TOTAL_STRIPS} strips generated`);
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