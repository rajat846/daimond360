'use strict';

/**
 * Maps raw CSV column names to our normalised field names.
 *
 * The CSV may come from different sources with varying column headers.
 * This module resolves each field by checking a priority list of aliases
 * against the actual headers present in the file.
 */

/** Priority-ordered aliases for each canonical field */
const FIELD_ALIASES = {
  certificate_num: ['certificate_num', 'certificatenum', 'reportno', 'report_no', 'certificateno', 'certificate_no', 'certno', 'cert_no', 'reportnumber', 'report_number'],
  stock_num:       ['stockid', 'stock_id', 'stocknum', 'stock_num', 'stock', 'id'],
  video_url:       ['video', 'video_url', 'videourl', 'v360url', 'v360_url'],
  image_url:       ['image', 'image_url', 'imageurl', 'img', 'img_url'],
  shape:           ['shape'],
  carat:           ['carats', 'carat', 'weight'],
  color:           ['col', 'color', 'colour'],
  clarity:         ['clar', 'clarity'],
  cut:             ['cut'],
  polish:          ['pol', 'polish'],
  symmetry:        ['symm', 'symmetry', 'sym'],
  fluorescence:    ['flo', 'fluorescence', 'fluor', 'flour'],
  depth_percent:   ['depth', 'depth_percent', 'depth%'],
  table_percent:   ['table', 'table_percent', 'table%'],
  length:          ['length', 'len'],
  width:           ['width', 'wid'],
  height:          ['height', 'ht'],
  girdle:          ['girdle'],
  culet_size:      ['culet', 'culet_size'],
  lab:             ['lab'],
  location:        ['country', 'location', 'loc'],
  our_price:       ['price', 'our_price', 'ourprice', 'deliveredprice'],
  l_w_ratio:       ['l_w_ratio', 'lw_ratio', 'lwratio'],
};

/**
 * Given an array of raw CSV header strings, build a mapping of
 * canonical field → actual CSV column name.
 *
 * @param {string[]} rawHeaders
 * @returns {{ [canonicalField: string]: string }}
 */
function buildColumnMap(rawHeaders) {
  // Normalise headers to lowercase-no-space for matching
  const normMap = {};
  for (const h of rawHeaders) {
    normMap[h.toLowerCase().replace(/[\s_-]+/g, '')] = h;
  }

  const columnMap = {};
  for (const [field, aliases] of Object.entries(FIELD_ALIASES)) {
    for (const alias of aliases) {
      const norm = alias.toLowerCase().replace(/[\s_-]+/g, '');
      if (normMap[norm]) {
        columnMap[field] = normMap[norm];
        break;
      }
    }
  }

  return columnMap;
}

/**
 * Transforms a raw CSV row object into our canonical diamond document shape.
 *
 * @param {object}  rawRow   - Object keyed by original CSV headers
 * @param {object}  colMap   - Output of buildColumnMap()
 * @returns {object}
 */
function mapRow(rawRow, colMap) {
  const get = (field) => {
    const col = colMap[field];
    return col ? rawRow[col] : undefined;
  };

  /**
   * Parse a number from a CSV cell.
   * Handles: "1,234.56" (comma thousands), "1234.56", "1.234,56" (EU format).
   * Returns null (not undefined) if the value is absent or non-numeric.
   */
  const toNum = (v) => {
    if (v == null || v === '' || v === 'NaN') return null;
    // Remove currency symbols, whitespace
    let s = String(v).trim().replace(/[$ €£¥]/g, '');
    // Detect European decimal format: e.g. "1.234,56" — comma is decimal
    if (/^\d{1,3}(\.\d{3})+(,\d+)?$/.test(s)) {
      s = s.replace(/\./g, '').replace(',', '.');
    } else {
      // Standard or comma-thousands: "1,234.56" or "1234.56"
      s = s.replace(/,/g, '');
    }
    const n = parseFloat(s);
    return isNaN(n) ? null : n;
  };

  /**
   * Coerce a CSV cell to a trimmed string.
   * Returns null for empty/null/undefined/'NaN' values.
   * Explicitly does NOT return undefined — every field gets a consistent type.
   */
  const str = (v) => {
    if (v == null || v === '' || v === 'NaN') return null;
    const s = String(v).trim();
    return s === '' ? null : s;
  };

  return {
    certificate_num: str(get('certificate_num')),
    stock_num:       str(get('stock_num')),
    video_url:       str(get('video_url')),
    image_url:       str(get('image_url')),
    shape:           str(get('shape')),
    carat:           toNum(get('carat')),
    color:           str(get('color')),
    clarity:         str(get('clarity')),
    cut:             str(get('cut')),
    polish:          str(get('polish')),
    symmetry:        str(get('symmetry')),
    fluorescence:    str(get('fluorescence')),
    depth_percent:   toNum(get('depth_percent')),
    table_percent:   toNum(get('table_percent')),
    length:          toNum(get('length')),
    width:           toNum(get('width')),
    depth:           toNum(get('height')), // CSV "height" = physical depth of the stone
    girdle:          str(get('girdle')),
    culet_size:      str(get('culet_size')),
    lab:             str(get('lab')),
    location:        str(get('location')),
    our_price:       toNum(get('our_price')),
    l_w_ratio:       toNum(get('l_w_ratio')),
  };
}

module.exports = { buildColumnMap, mapRow };