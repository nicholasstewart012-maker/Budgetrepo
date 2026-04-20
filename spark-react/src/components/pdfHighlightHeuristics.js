/**
 * pdfHighlightHeuristics.js
 *
 * Shared heuristics for classifying PDF text fragments and scoring highlight
 * candidates. Used by both the main PdfSourceViewer.jsx and the new helper
 * modules (pdfLayoutModel, pdfMatchScoring, pdfTextBlockResolver).
 */

const PDF_MEANINGFUL_MIN_LENGTH = 30

// --------------------------------------------------------------------------
// Vocabulary sets
// --------------------------------------------------------------------------

/** Section / heading words common in policy PDFs. */
const HEADING_TERMS = new Set([
  'purpose',
  'scope',
  'policy',
  'overview',
  'definitions',
  'introduction',
  'revision',
  'history',
  'procedure',
  'procedures',
  'roles',
  'contents',
  'applicability',
  'responsibilities',
  'approval',
  'committee',
  'sponsor',
  'owner',
  'effective',
  'background',
  'references',
  'appendix',
  'objectives',
  'summary',
])

/** Administrative metadata labels found in policy / procedure document headers. */
const METADATA_LABEL_TERMS = [
  'sponsoring department',
  'executive sponsor',
  'board committee',
  'committee approval date',
  'policy statement',
  'change management policy',
  'effective date',
  'approved by',
  'document owner',
  'policy owner',
  'version number',
  'revision date',
  'review date',
  'next review',
  'last reviewed',
  'last modified',
  'classification',
  'document number',
  'applicable to',
]

const STOPWORDS = new Set([
  'the', 'and', 'for', 'are', 'that', 'with', 'this', 'from', 'have', 'has', 'you', 'your', 'was', 'were',
  'will', 'shall', 'must', 'may', 'can', 'not', 'all', 'any', 'but', 'than', 'then', 'into', 'onto',
])

// --------------------------------------------------------------------------
// Core tokeniser
// --------------------------------------------------------------------------

/**
 * Tokenises text into lowercase alphanumeric tokens.
 * @param {string} text
 * @returns {string[]}
 */
export function tokenizeNormalized(text = '') {
  return String(text || '').toLowerCase().match(/[a-z0-9]+/g) || []
}

// --------------------------------------------------------------------------
// Block classification
// --------------------------------------------------------------------------

/**
 * Returns true if the text looks like a section heading rather than body text.
 *
 * Signals:
 *  - Numbered section prefix (e.g. "3 PURPOSE", "1.2 Scope")
 *  - Short text (≤26 chars or ≤3 tokens)
 *  - Mostly uppercase letters (≥72%)
 *  - Heading-term only (≤4 tokens, one is a heading term, no punctuation)
 *
 * @param {string} text
 * @returns {boolean}
 */
export function isHeadingLikeText(text = '') {
  const raw = String(text || '').trim()
  if (!raw) return true

  const normalized = raw.replace(/\s+/g, ' ')
  const lower = normalized.toLowerCase()
  const tokens = tokenizeNormalized(normalized)
  const tokenCount = tokens.length

  // Numbered section prefix: "3 PURPOSE" or "1.2 Scope Overview"
  const numberPrefix =
    /^\s*\d+(?:\.\d+)*\s*[A-Z][A-Z\s-]*$/.test(normalized) ||
    /^\s*\d+(?:\.\d+)*\s+[A-Za-z]/.test(normalized)

  const shortText = normalized.length <= 26 || tokenCount <= 3

  const alphaChars = normalized.replace(/[^A-Za-z]/g, '')
  const uppercaseRatio = alphaChars.length > 0
    ? normalized.replace(/[^A-Z]/g, '').length / alphaChars.length
    : 0
  const mostlyUpper = uppercaseRatio >= 0.72

  const headingTermOnly = tokenCount <= 5 && tokens.some(t => HEADING_TERMS.has(t))
  const punctLight = !/[,:;.!?]/.test(normalized)

  // Extra: title-case short phrase with no punctuation
  const words = normalized.split(/\s+/).filter(Boolean)
  const titleCaseCount = words.filter(w => /^[A-Z][a-z]/.test(w)).length
  const titleLike = words.length <= 8 && words.length >= 2 &&
    titleCaseCount / words.length >= 0.65 && punctLight

  return Boolean(
    (numberPrefix && shortText) ||
    (mostlyUpper && shortText) ||
    (headingTermOnly && punctLight) ||
    titleLike
  )
}

/**
 * Returns true if the text looks like an administrative metadata block
 * (sponsor, committee, effective-date blocks in policy document headers).
 *
 * @param {string} text
 * @returns {boolean}
 */
export function isMetadataLikeText(text = '') {
  const cleaned = String(text || '').replace(/\s+/g, ' ').trim()
  if (!cleaned || cleaned.length < 35) return false
  const lower = cleaned.toLowerCase()
  const labelHits = METADATA_LABEL_TERMS.reduce(
    (n, term) => n + (lower.includes(term) ? 1 : 0), 0
  )
  const colonCount = (cleaned.match(/:/g) || []).length
  // Strong signal: multiple known labels OR many colons with no sentence punctuation
  if (labelHits >= 2) return true
  if (colonCount >= 3 && !/[.!?]/.test(cleaned)) return true
  return false
}

/**
 * Returns true if the text looks like a table region or revision-history block.
 *
 * Signals:
 *  - Pipe characters (table separators)
 *  - High density of date-like patterns and version numbers
 *  - Short fields with many colons relative to word count
 *
 * @param {string} text
 * @returns {boolean}
 */
export function isTableLikeText(text = '') {
  const cleaned = String(text || '').replace(/\s+/g, ' ').trim()
  if (!cleaned) return false

  const hasPipes = (cleaned.match(/\|/g) || []).length >= 2
  if (hasPipes) return true

  // Revision history heuristic: lots of dates + version numbers + short fields
  const dateCount = (cleaned.match(/\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{4}\b/gi) || []).length
  const versionCount = (cleaned.match(/\bv?\d+\.\d+/gi) || []).length
  const tokens = tokenizeNormalized(cleaned)
  const colonCount = (cleaned.match(/:/g) || []).length

  if ((dateCount >= 2 || versionCount >= 2) && colonCount >= 2) return true
  if (dateCount >= 3) return true

  return false
}

/**
 * Returns true if the text looks like a real body sentence.
 *
 * @param {string} text
 * @returns {boolean}
 */
export function isSentenceLikeText(text = '') {
  const raw = String(text || '').trim()
  if (!raw) return false
  if (isHeadingLikeText(raw)) return false

  const tokens = tokenizeNormalized(raw)
  if (tokens.length < 6) return false

  const meaningfulTokens = tokens.filter(t => !STOPWORDS.has(t))
  const hasMixedCase = /[a-z]/.test(raw) && /[A-Z]/.test(raw)
  const hasSentencePunctuation = /[,:;.!?]/.test(raw)
  return meaningfulTokens.length >= 4 && (hasMixedCase || hasSentencePunctuation || raw.length >= 52)
}

// --------------------------------------------------------------------------
// Sentence chunk extraction
// --------------------------------------------------------------------------

/**
 * Extracts the most meaningful sentence-like fragments from a list of source
 * texts. Used to build phrase candidate lists for fallback matching.
 *
 * @param {Array<string | null | undefined>} texts
 * @param {number} [maxCount]
 * @returns {string[]}
 */
export function extractMeaningfulSentenceChunks(texts = [], maxCount = 6) {
  const candidates = []
  for (const value of texts) {
    const source = String(value || '').trim()
    if (!source) continue
    const sentences = source
      .replace(/\s+/g, ' ')
      .split(/(?<=[.!?])\s+/)
      .map(s => s.trim())
      .filter(Boolean)
    for (const sentence of sentences) {
      if (sentence.length < PDF_MEANINGFUL_MIN_LENGTH) continue
      if (!isSentenceLikeText(sentence)) continue
      candidates.push(sentence)
    }
  }

  const deduped = []
  const seen = new Set()
  for (const sentence of candidates.sort((a, b) => b.length - a.length)) {
    const key = sentence.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    deduped.push(sentence)
    if (deduped.length >= maxCount) break
  }
  return deduped
}

// --------------------------------------------------------------------------
// Candidate scoring (used by validate script and legacy frontend path)
// --------------------------------------------------------------------------

/**
 * Scores a single phrase candidate against a page's text.
 * Returns a breakdown object compatible with selectBestPdfMatch().
 *
 * @param {{
 *   candidate: string,
 *   target: string,
 *   normalizedPageText: string,
 *   occurrenceCount?: number,
 *   isLoose?: boolean,
 *   candidateOrder?: number,
 *   totalCandidates?: number,
 * }} opts
 * @returns {{ score: number, isHeadingLike: boolean, isSentenceLike: boolean, meaningfulLen: number, tokenCoverage: number }}
 */
export function scorePdfMatchCandidate({
  candidate = '',
  target = '',
  normalizedPageText = '',
  occurrenceCount = 1,
  isLoose = false,
  candidateOrder = 0,
  totalCandidates = 1,
}) {
  const rawCandidate = String(candidate || '')
  const rawTarget = String(target || '')
  const candidateTokens = tokenizeNormalized(rawCandidate)
  const targetTokens = tokenizeNormalized(rawTarget)
  const pageTokens = tokenizeNormalized(normalizedPageText)

  const tokenCoverage = targetTokens.length > 0
    ? targetTokens.filter(t => pageTokens.includes(t)).length / targetTokens.length
    : 0

  const meaningfulLen = rawTarget.length
  const isHeadingLike = isHeadingLikeText(rawTarget)
  const isSentenceLike = isSentenceLikeText(rawTarget)
  const numericChars = rawTarget.replace(/[^0-9]/g, '').length
  const alphaChars = rawTarget.replace(/[^A-Za-z]/g, '').length
  const numericHeavy = numericChars > 0 && alphaChars > 0 && (numericChars / (numericChars + alphaChars)) > 0.45

  const orderBonus = totalCandidates > 1 ? (1 - (candidateOrder / (totalCandidates - 1))) * 0.12 : 0.12
  const lengthScore = Math.min(1.4, meaningfulLen / 90)
  const coverageScore = tokenCoverage * 1.2
  const occurrenceScore = Math.min(0.22, Math.max(0, occurrenceCount - 1) * 0.04)
  const sentenceBonus = isSentenceLike ? 0.42 : 0
  const meaningfulBonus = meaningfulLen >= PDF_MEANINGFUL_MIN_LENGTH ? 0.2 : 0

  let penalty = 0
  // Stronger heading penalty — was 0.75, now 1.10
  if (isHeadingLike) penalty += 1.10
  if (meaningfulLen < PDF_MEANINGFUL_MIN_LENGTH) penalty += (PDF_MEANINGFUL_MIN_LENGTH - meaningfulLen) / 100
  if (numericHeavy) penalty += 0.2
  if (isLoose) penalty += 0.1

  const score = lengthScore + coverageScore + occurrenceScore + sentenceBonus + meaningfulBonus + orderBonus - penalty

  return {
    score,
    isHeadingLike,
    isSentenceLike,
    meaningfulLen,
    tokenCoverage,
  }
}

// --------------------------------------------------------------------------
// Best match selection (used by validate script and legacy frontend path)
// --------------------------------------------------------------------------

/**
 * Picks the best scoring option from a list, preferring body-text candidates
 * over heading-like ones.
 *
 * @param {Array<{ variant: string, rank: ReturnType<typeof scorePdfMatchCandidate> }>} matchOptions
 * @returns {typeof matchOptions[number] | null}
 */
export function selectBestPdfMatch(matchOptions = []) {
  if (!Array.isArray(matchOptions) || matchOptions.length === 0) return null

  const sorted = matchOptions
    .slice()
    .sort((a, b) => (b.rank.score - a.rank.score) || (b.rank.meaningfulLen - a.rank.meaningfulLen))

  const meaningfulBodyCandidates = sorted.filter(option =>
    option.rank.meaningfulLen >= PDF_MEANINGFUL_MIN_LENGTH && !option.rank.isHeadingLike
  )

  if (meaningfulBodyCandidates.length > 0) {
    const winner = meaningfulBodyCandidates[0]
    const headerWinner = sorted[0]
    // Tighter margin: was 0.15, now 0.08 so body text wins more easily
    const materiallyBetterBody = winner.rank.score >= (headerWinner.rank.score - 0.08)
    if (materiallyBetterBody) {
      return winner
    }
  }

  return sorted[0]
}

export const PDF_HIGHLIGHT_MEANINGFUL_LENGTH = PDF_MEANINGFUL_MIN_LENGTH
