import {
  extractMeaningfulSentenceChunks,
  isHeadingLikeText,
  isMetadataLikeText,
  isTableLikeText,
  scorePdfMatchCandidate,
  selectBestPdfMatch,
} from '../src/components/pdfHighlightHeuristics.js'

function assert(condition, message) {
  if (!condition) {
    throw new Error(message)
  }
}

// ── Existing tests (must still pass) ─────────────────────────────────────

function caseHeadingVsSentence() {
  const pageText = '3 PURPOSE The primary goal of the Renasant Technology change management organization is to accomplish Technology changes in the most efficient manner while minimizing the business impact, costs, and risks.'
  const options = [
    {
      variant: '3 PURPOSE',
      rank: scorePdfMatchCandidate({
        candidate: '3 PURPOSE',
        target: '3 PURPOSE',
        normalizedPageText: pageText,
        occurrenceCount: 1,
        candidateOrder: 0,
        totalCandidates: 2,
      }),
    },
    {
      variant: 'The primary goal of the Renasant Technology change management organization is to accomplish Technology changes in the most efficient manner while minimizing the business impact, costs, and risks.',
      rank: scorePdfMatchCandidate({
        candidate: 'The primary goal of the Renasant Technology change management organization is to accomplish Technology changes in the most efficient manner while minimizing the business impact, costs, and risks.',
        target: 'The primary goal of the Renasant Technology change management organization is to accomplish Technology changes in the most efficient manner while minimizing the business impact, costs, and risks.',
        normalizedPageText: pageText,
        occurrenceCount: 1,
        candidateOrder: 1,
        totalCandidates: 2,
      }),
    },
  ]
  const winner = selectBestPdfMatch(options)
  assert(winner?.variant?.includes('primary goal of the Renasant Technology'), 'Expected body sentence to beat heading fragment')
}

function caseShortFragmentVsLongBody() {
  const pageText = 'The change management process includes formally requesting the change, analyzing and justifying the change, prioritizing, approving, implementing, and post-implementation review.'
  const shortOption = {
    variant: 'change management',
    rank: scorePdfMatchCandidate({
      candidate: 'change management',
      target: 'change management',
      normalizedPageText: pageText,
      occurrenceCount: 1,
      candidateOrder: 0,
      totalCandidates: 2,
    }),
  }
  const longOption = {
    variant: 'The change management process includes formally requesting the change, analyzing and justifying the change, prioritizing, approving, implementing, and post-implementation review.',
    rank: scorePdfMatchCandidate({
      candidate: 'The change management process includes formally requesting the change, analyzing and justifying the change, prioritizing, approving, implementing, and post-implementation review.',
      target: 'The change management process includes formally requesting the change, analyzing and justifying the change, prioritizing, approving, implementing, and post-implementation review.',
      normalizedPageText: pageText,
      occurrenceCount: 1,
      candidateOrder: 1,
      totalCandidates: 2,
    }),
  }
  const winner = selectBestPdfMatch([shortOption, longOption])
  assert(winner?.variant?.startsWith('The change management process includes formally requesting'), 'Expected long body text to beat short fragment')
}

function caseFallbackSentenceChunks() {
  const snippets = [
    '3 PURPOSE The primary goal is to accomplish technology changes while minimizing business impact.',
    'Short heading',
    'Analyze and Justify Change',
    'The process includes formally requesting the change, prioritizing the change, approving the change, and post-implementation review.',
  ]
  const chunks = extractMeaningfulSentenceChunks(snippets, 4)
  assert(chunks.length >= 2, 'Expected at least two meaningful sentence chunks')
  assert(chunks[0].length >= chunks[chunks.length - 1].length, 'Expected chunks ordered by length')
  assert(chunks.some(chunk => chunk.includes('process includes formally requesting')), 'Expected meaningful fallback body sentence to be present')
}

// ── New tests ─────────────────────────────────────────────────────────────

/**
 * Test: isHeadingLikeText correctly identifies common policy PDF section headers
 * including numbered sections and new vocabulary terms.
 */
function caseHeadingDetection() {
  const headings = [
    '3 PURPOSE',
    '1 INTRODUCTION',
    '4 SCOPE',
    'Applicability',
    'Roles and Responsibilities',
    'Executive Sponsor',
    '2.1 Background',
    'REVISION HISTORY',
    'OVERVIEW',
  ]
  for (const h of headings) {
    assert(isHeadingLikeText(h), `Expected "${h}" to be detected as heading-like`)
  }

  const bodyTexts = [
    'The primary goal of this policy is to ensure that all technology changes are managed effectively and with minimal business disruption.',
    'Employees are required to submit a formal change request before implementing any modifications to production systems.',
    'This procedure applies to all departments within the organization, including IT, Finance, and Human Resources.',
  ]
  for (const b of bodyTexts) {
    assert(!isHeadingLikeText(b), `Expected body text not to be detected as heading: "${b.slice(0, 60)}..."`)
  }
}

/**
 * Test: isMetadataLikeText correctly identifies administrative metadata blocks.
 */
function caseMetadataDetection() {
  const metadataBlocks = [
    'Sponsoring Department: Information Technology  Executive Sponsor: Jane Smith',
    'Policy Owner: CIO  Effective Date: 01/15/2024  Review Date: 01/15/2025  Classification: Internal',
    'Document Number: IT-POL-042  Version Number: 2.3  Approved By: Board Committee',
  ]
  for (const m of metadataBlocks) {
    assert(isMetadataLikeText(m), `Expected metadata block detected: "${m.slice(0, 60)}..."`)
  }

  const nonMetadata = [
    'The change management process includes formally requesting the change, analyzing and justifying the change.',
    'All employees must complete the required training within 30 days of hire.',
  ]
  for (const n of nonMetadata) {
    assert(!isMetadataLikeText(n), `Expected non-metadata not flagged: "${n.slice(0, 60)}..."`)
  }
}

/**
 * Test: isTableLikeText correctly identifies revision-history and table regions.
 */
function caseTableDetection() {
  const tableBlocks = [
    'Version | Date | Author | Changes  1.0 | 01/15/2023 | J. Smith | Initial release  2.0 | 06/20/2023 | A. Jones | Updated scope',
    'v1.0 01/15/2023 Initial version  v1.1 06/20/2023 Minor updates: revised Section 3  v2.0 January 2024 Major overhaul',
    'Date: 03/12/2024 | Reviewer: IT Committee | Status: Approved | Reference: IT-POL-042',
  ]
  for (const t of tableBlocks) {
    assert(isTableLikeText(t), `Expected table-like block detected: "${t.slice(0, 60)}..."`)
  }

  const nonTable = [
    'The change management process includes formally requesting the change.',
    'All employees must comply with this policy.',
  ]
  for (const n of nonTable) {
    assert(!isTableLikeText(n), `Expected non-table not flagged: "${n.slice(0, 60)}..."`)
  }
}

/**
 * Test: heading still loses to body text under the new stricter margin (0.08 vs 0.15).
 * This validates the tighter selectBestPdfMatch threshold.
 */
function caseStrictHeadingMargin() {
  // A scenario where heading has a slightly higher raw score but body should still win
  // under the tighter 0.08 margin rule.
  const pageText = 'Scope This policy applies to all employees. Employees are required to follow the change management process when requesting modifications to production systems including planning, approval, and post-implementation review.'
  const headingOption = {
    variant: 'Scope',
    rank: scorePdfMatchCandidate({
      candidate: 'Scope',
      target: 'Scope',
      normalizedPageText: pageText,
      occurrenceCount: 1,
      candidateOrder: 0,
      totalCandidates: 2,
    }),
  }
  const bodyOption = {
    variant: 'Employees are required to follow the change management process when requesting modifications to production systems including planning, approval, and post-implementation review.',
    rank: scorePdfMatchCandidate({
      candidate: 'Employees are required to follow the change management process when requesting modifications to production systems including planning, approval, and post-implementation review.',
      target: 'Employees are required to follow the change management process when requesting modifications to production systems including planning, approval, and post-implementation review.',
      normalizedPageText: pageText,
      occurrenceCount: 1,
      candidateOrder: 1,
      totalCandidates: 2,
    }),
  }
  const winner = selectBestPdfMatch([headingOption, bodyOption])
  assert(
    winner?.variant?.includes('Employees are required'),
    'Expected body text to win even under tighter margin threshold'
  )
}

// ── Runner ────────────────────────────────────────────────────────────────

try {
  caseHeadingVsSentence()
  caseShortFragmentVsLongBody()
  caseFallbackSentenceChunks()
  caseHeadingDetection()
  caseMetadataDetection()
  caseTableDetection()
  caseStrictHeadingMargin()
  console.log('PDF highlight heuristic validation passed (7 cases)')
} catch (error) {
  console.error('PDF highlight heuristic validation failed:', error.message)
  process.exitCode = 1
}
