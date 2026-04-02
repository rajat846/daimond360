'use strict';

const axios  = require('axios');
const logger = require('../utils/logger');
const { withRetry } = require('../utils/retry');

const GRAPHQL_URL    = process.env.GRAPHQL_URL        || 'https://integrations.nivoda.net/graphql-loupe360';
const RETRY_ATTEMPTS = parseInt(process.env.API_RETRY_ATTEMPTS, 10) || 3;
const RETRY_DELAY_MS = parseInt(process.env.API_RETRY_DELAY_MS, 10) || 2000;

const QUERY = `
  query ($cert_number: String!) {
    certificate: certificate_by_cert_number(cert_number: $cert_number) {
      v360 {
        frame_count
        url
      }
    }
  }
`;

/**
 * Fetches 360 video metadata for a given certificate number.
 *
 * Returns null (does NOT throw) when:
 *   - The API returns no v360 data for the cert  → has_video: false
 *   - v360.url or v360.frame_count is missing    → has_video: false
 *
 * Throws only on genuine network / server errors (retried up to RETRY_ATTEMPTS times).
 * A "no v360 data" response from the API is NOT an error — it is a valid state.
 *
 * @param {string} certNumber
 * @returns {Promise<{ frame_count: number, url: string } | null>}
 */
async function fetchVideoData(certNumber) {
  // We wrap only the network call in withRetry.
  // If the network call succeeds but returns no v360, we return null immediately
  // without counting it as a retry-able error.
  return withRetry(
    async () => {
      logger.debug(`Fetching v360 data for certificate: ${certNumber}`);

      let response;
      try {
        response = await axios.post(
          GRAPHQL_URL,
          {
            query:     QUERY,
            variables: { cert_number: String(certNumber) },
          },
          {
            headers: { 'Content-Type': 'application/json' },
            timeout: 30_000,
          }
        );
      } catch (networkErr) {
        // Network error — worth retrying
        throw new Error(`Network error fetching v360 for ${certNumber}: ${networkErr.message}`);
      }

      // Hard GraphQL-level errors — retry (could be transient)
      if (response.data.errors && response.data.errors.length > 0) {
        const msg = response.data.errors.map((e) => e.message).join('; ');
        throw new Error(`GraphQL error for ${certNumber}: ${msg}`);
      }

      const v360 = response.data?.data?.certificate?.v360;

      // No v360 data at all — valid "no video" state, return null immediately
      // (do NOT throw — this must not be retried)
      if (!v360) {
        logger.info(`[${certNumber}] No v360 data — has_video: false`);
        return null;
      }

      // Missing url or frame_count — treat as no video (do NOT throw)
      if (!v360.url || !v360.frame_count) {
        logger.info(
          `[${certNumber}] Incomplete v360 data (url=${v360.url}, frames=${v360.frame_count}) — has_video: false`
        );
        return null;
      }

      logger.debug(`[${certNumber}] v360 OK — frames: ${v360.frame_count}, url: ${v360.url}`);
      return v360;
    },
    RETRY_ATTEMPTS,
    RETRY_DELAY_MS,
    `fetchVideoData(${certNumber})`
  );
}

module.exports = { fetchVideoData };