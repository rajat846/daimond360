'use strict';

/**
 * Maps each MongoDB collection name to its S3 path prefix.
 *
 * DB collection  →  S3 folder under /stones/<folder>/<certNum>/main.webp
 */
const COLLECTION_MAP = [
  { collection: 'diamondns',  s3Folder: 'natural-white'   },
  { collection: 'diamondls',  s3Folder: 'lab-white'       },
  { collection: 'diamondfs',  s3Folder: 'natural-colored' },
  { collection: 'diamondfls', s3Folder: 'lab-colored'     },
  { collection: 'gemstones',  s3Folder: 'gemstones'       },
];

/**
 * Build the S3 key for a given folder + certNum.
 * e.g. stones/natural-white/1513853325/main.webp
 */
function buildS3Key(s3Folder, certNum) {
  return `stones/${s3Folder}/${certNum}/main.webp`;
}

module.exports = { COLLECTION_MAP, buildS3Key };