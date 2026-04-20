import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import styles from './ViewerApp.module.css'
import {
  extractMeaningfulSentenceChunks,
} from './pdfHighlightHeuristics.js'
import { getApiBase } from '../api.js'

const API = getApiBase()
const DEV = import.meta.env.DEV
const NORM_CACHE_LIMIT = 4000
const PHRASE_WINDOW_CACHE_LIMIT = 800
const normCache = new Map()
const phraseWindowCache = new Map()

function norm(value) {
  const input = String(value ?? '')
  const cached = normCache.get(input)
  if (cached !== undefined) return cached

  const normalized = input
    .replace(/\r\n|\r|\n/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase()

  normCache.set(input, normalized)
  if (normCache.size > NORM_CACHE_LIMIT) {
    const first = normCache.keys().next()
    if (!first.done) normCache.delete(first.value)
  }
  return normalized
}

function splitSentences(text) {
  if (!text) return []
  const clean = String(text).replace(/\s+/g, ' ').trim()
  return clean.split(/(?<=[.!?])\s+/).map(s => s.trim()).filter(Boolean)
}

function phraseWindows(text, windows = [12, 10, 8, 6, 5, 4, 3]) {
  const source = String(text || '')
  const cacheKey = `${source}::${windows.join(',')}`
  const cached = phraseWindowCache.get(cacheKey)
  if (cached) {
    return cached
  }

  const words = norm(source).split(' ').filter(Boolean)
  const seen = new Set()
  const phrases = []
  for (const size of windows) {
    if (words.length < size) continue
    for (let i = 0; i <= words.length - size; i++) {
      const phrase = words.slice(i, i + size).join(' ')
      if (!phrase || seen.has(phrase)) continue
      seen.add(phrase)
      phrases.push(phrase)
    }
  }
  phraseWindowCache.set(cacheKey, phrases)
  if (phraseWindowCache.size > PHRASE_WINDOW_CACHE_LIMIT) {
    const first = phraseWindowCache.keys().next()
    if (!first.done) phraseWindowCache.delete(first.value)
  }
  return phrases
}

function uniqueCandidates(maybeList) {
  const seen = new Set()
  const out = []
  for (const item of maybeList) {
    const clean = String(item || '').trim()
    const key = norm(clean)
    if (!clean || !key || seen.has(key)) continue
    seen.add(key)
    out.push(clean)
  }
  return out
}

function logDev(...args) {
  if (DEV) {
    console.debug('[Spark Viewer]', ...args)
  }
}

const EXTRACTION_DISPLAY = {
  text_layer: {
    label: 'Document text',
    note: '',
  },
  ocr: {
    label: 'OCR text',
    note: 'This page was read using OCR, so in-document highlighting may be limited.',
  },
  ocr_failed: {
    label: 'OCR unavailable',
    note: 'This scanned page could not be read automatically.',
  },
  unknown: {
    label: 'Document source',
    note: '',
  },
}

function normalizeExtractionMethod(value) {
  const method = String(value || '').trim().toLowerCase()
  if (method === 'text_layer' || method === 'ocr' || method === 'ocr_failed' || method === 'vision_fallback') {
    return method
  }
  return 'unknown'
}

function uniqueOrdered(values) {
  return uniqueCandidates(values)
}

function addWindowsForText(values, text) {
  const source = String(text || '').trim()
  if (!source) return
  values.push(source)
  for (const sentence of splitSentences(source)) {
    values.push(sentence)
    values.push(...phraseWindows(sentence))
  }
  values.push(...phraseWindows(source))
}

function normalizePdfSearchText(value) {
  return String(value || '')
    .replace(/[–—−]/g, '-')
    .replace(/\s+/g, ' ')
    .replace(/^["'“”]+|["'“”]+$/g, '')
    .trim()
}

function stripPolicyHeading(value) {
  return normalizePdfSearchText(value)
    .replace(/^\d+(?:\.\d+)*\s+[A-Z][A-Z\s-]{2,}\s*[-:]\s*/i, '')
    .replace(/^\d+(?:\.\d+)*\s*/i, '')
    .replace(/^(scope|purpose|policy|procedure|procedures|introduction|definitions)\s*[-:]\s*/i, '')
    .trim()
}

const GENERIC_PDF_SEARCH_TERMS = new Set([
  'change',
  'changes',
  'changed',
  'changing',
  'implementation',
  'implemented',
  'implement',
  'process',
  'procedure',
  'procedures',
  'policy',
  'section',
  'scope',
  'purpose',
  'document',
  'review',
  'completion',
  'completed',
  'approval',
  'approved',
  'request',
  'system',
  'systems',
  'technology',
])

const PDF_CANDIDATE_TYPES = {
  exactEvidence: 'exact_evidence',
  bodySentence: 'body_sentence',
  snippetSentence: 'snippet_sentence',
  chunkSentence: 'chunk_sentence',
  headingLabel: 'heading_label',
  acronym: 'acronym',
  fallback: 'fallback',
}

function isUpperAcronym(value) {
  const clean = normalizePdfSearchText(value)
  return /^[A-Z0-9]{2,8}$/.test(clean)
}

function isGenericEvidencePhrase(value) {
  const clean = normalizePdfSearchText(value).toLowerCase()
  if (!clean) return true
  if (GENERIC_PDF_SEARCH_TERMS.has(clean)) return true
  if (clean.length < 8 && !/^[A-Z0-9]{2,8}$/.test(clean)) return true
  return false
}

function splitPdfSentences(text) {
  return normalizePdfSearchText(text)
    .split(/(?<=[.!?])\s+/)
    .map(part => normalizePdfSearchText(part))
    .filter(Boolean)
}

function isHeadingOnlyCandidate(value) {
  const clean = normalizePdfSearchText(value)
  if (!clean) return true
  if (/^\d+(?:\.\d+)*\s+/.test(clean) && clean.split(/\s+/).length <= 6) return true
  if (/^[A-Z0-9][A-Z0-9\s-]{2,}$/.test(clean) && clean.split(/\s+/).length <= 5) return true
  if (/^(implement|approve|prioritize|analyze|review|set)\b/i.test(clean) && clean.split(/\s+/).length <= 4) return true
  return false
}

const PDF_BODY_STARTERS = new Set([
  'the',
  'this',
  'all',
  'any',
  'information',
  'users',
  'owners',
  'company',
  'renasant',
  'payment',
  'data',
  'configuration',
  'management',
  'procedures',
  'requirements',
  'controls',
  'access',
  'systems',
  'employees',
  'vendors',
  'contractors',
])

function cleanPdfToken(value) {
  return String(value || '').replace(/^[^\w\d]+|[^\w\d]+$/g, '')
}

function looksLikeEmbeddedPdfHeadingPrefix(value) {
  const clean = normalizePdfSearchText(value)
  if (!clean || /[.!?;]/.test(clean)) return false

  const words = clean.split(/\s+/).filter(Boolean)
  if (words.length < 2 || words.length > 12) return false

  const alphaWords = words
    .map(word => cleanPdfToken(word))
    .filter(word => /[A-Za-z]/.test(word))

  if (alphaWords.length === 0) return false

  const titleishCount = alphaWords.filter(word => (
    /^[A-Z][A-Za-z'’()/&-]*$/.test(word)
    || /^[A-Z0-9]{2,}$/.test(word)
    || /^\([A-Z0-9]{2,}\)$/.test(word)
  )).length

  const startsNumbered = /^\d+(?:\.\d+)*\b/.test(clean)
  const hasKnownHeadingTerm = /(policy|statement|data|security|environment|defined|industry|classification|responsibilities|requirements|scope|purpose|availability|applicability|program|standard|standards|procedure|procedures)/i.test(clean)
  const titleishRatio = titleishCount / Math.max(alphaWords.length, 1)

  return startsNumbered || hasKnownHeadingTerm || titleishRatio >= 0.6
}

function splitLeadingEmbeddedPdfHeading(value) {
  const clean = normalizePdfSearchText(value)
  const words = clean.split(/\s+/).filter(Boolean)
  if (words.length < 8) return null

  let best = null
  const maxPrefixWords = Math.min(12, words.length - 5)
  for (let index = 2; index <= maxPrefixWords; index += 1) {
    const heading = words.slice(0, index).join(' ')
    const body = words.slice(index).join(' ')
    const firstBodyWord = cleanPdfToken(words[index] || '').toLowerCase()

    if (!PDF_BODY_STARTERS.has(firstBodyWord)) continue
    if (!looksLikeEmbeddedPdfHeadingPrefix(heading)) continue

    const bodyWords = body.split(/\s+/).filter(Boolean)
    if (body.length < 40 || bodyWords.length < 6 || isHeadingOnlyCandidate(body)) continue

    best = { heading, body }
  }

  return best
}

function stripEmbeddedPdfHeading(value) {
  const clean = normalizePdfSearchText(value)
  const split = splitLeadingEmbeddedPdfHeading(clean)
  return split?.body || clean
}


function scorePdfCandidate(candidate) {
  let score = candidate.type === PDF_CANDIDATE_TYPES.exactEvidence ? 100 : 0
  if (candidate.type === PDF_CANDIDATE_TYPES.bodySentence) score = 90
  if (candidate.type === PDF_CANDIDATE_TYPES.snippetSentence) score = 80
  if (candidate.type === PDF_CANDIDATE_TYPES.chunkSentence) score = 70
  if (candidate.type === PDF_CANDIDATE_TYPES.headingLabel) score = 20
  if (candidate.type === PDF_CANDIDATE_TYPES.acronym) score = 15
  if (candidate.type === PDF_CANDIDATE_TYPES.fallback) score = 5

  const wordCount = candidate.text.split(/\s+/).filter(Boolean).length
  const length = candidate.text.length
  if (wordCount >= 5) score += 15
  if (length >= 30) score += 10
  if (length >= 80) score += 5
  if (candidate.type === PDF_CANDIDATE_TYPES.headingLabel) score -= 10
  if (candidate.type === PDF_CANDIDATE_TYPES.acronym) score += 5
  return score
}

function buildPdfSearchCandidatesV2(evidenceObject, evidenceAnchor, evidenceContext, evidenceText, snippet, chunkText) {
  const rawValues = [
    evidenceObject?.displayText,
    evidenceAnchor,
    evidenceContext,
    evidenceText,
    snippet,
    evidenceObject?.phrase,
    chunkText,
  ]

  const combinedText = normalizePdfSearchText(rawValues.join(' '))
  const acronymAnchors = ['SDLC', 'PCI', 'COTS', 'MFA', 'VPN', 'BCP', 'DR', 'IR']
  const candidates = []
  const seen = new Set()

  const add = (text, type, source, reason) => {
    const clean = normalizePdfSearchText(text)
    if (!clean) return

    const lower = clean.toLowerCase()
    if (seen.has(lower)) return

    const wordCount = clean.split(/\s+/).filter(Boolean).length
    if (wordCount === 1) {
      if (!isUpperAcronym(clean) && GENERIC_PDF_SEARCH_TERMS.has(lower)) return
      if (!isUpperAcronym(clean) && clean.length < 4) return
    } else if (clean.length < 8) {
      return
    }

    const candidate = {
      text: clean,
      type,
      source,
      score: scorePdfCandidate({ text: clean, type }),
      reason,
    }

    seen.add(lower)
    candidates.push(candidate)
  }

  const addBodyCandidate = (value, source, reason) => {
    const clean = normalizePdfSearchText(value)
    if (!clean) return
    add(clean, PDF_CANDIDATE_TYPES.bodySentence, source, reason)
  }

  const addHeadingCandidate = (value, source, reason) => {
    const clean = normalizePdfSearchText(value)
    if (!clean) return
    if (isGenericEvidencePhrase(clean) && !isUpperAcronym(clean)) return
    add(clean, PDF_CANDIDATE_TYPES.headingLabel, source, reason)
  }

  const addAcronymCandidate = (value, source, reason) => {
    const clean = normalizePdfSearchText(value)
    if (!clean || !isUpperAcronym(clean)) return
    add(clean, PDF_CANDIDATE_TYPES.acronym, source, reason)
  }

  const expandText = (value) => {
    const clean = stripEmbeddedPdfHeading(normalizePdfSearchText(value))
    if (!clean) return []
    return splitPdfSentences(clean)
  }

  const explicitSentenceCandidates = []
  const pushExplicitSentence = (value, source, type, reason) => {
    for (const sentence of expandText(value)) {
      explicitSentenceCandidates.push({ text: sentence, source, type, reason })
    }
  }

  pushExplicitSentence(evidenceAnchor, 'evidenceAnchor', PDF_CANDIDATE_TYPES.bodySentence, 'evidence anchor sentence')
  pushExplicitSentence(evidenceContext, 'evidenceContext', PDF_CANDIDATE_TYPES.bodySentence, 'evidence context sentence')
  pushExplicitSentence(evidenceObject?.displayText, 'evidenceObject.displayText', PDF_CANDIDATE_TYPES.exactEvidence, 'display text sentence')
  pushExplicitSentence(evidenceText, 'evidenceText', PDF_CANDIDATE_TYPES.bodySentence, 'evidence text sentence')
  pushExplicitSentence(snippet, 'snippet', PDF_CANDIDATE_TYPES.snippetSentence, 'snippet sentence')
  pushExplicitSentence(evidenceObject?.phrase, 'firstMatch.phrase', PDF_CANDIDATE_TYPES.fallback, 'match phrase sentence')

  const paragraphChunks = []
  const chunkSource = normalizePdfSearchText(chunkText)
  if (chunkSource) {
    const paragraphParts = chunkSource.split(/\n{2,}/).map(part => normalizePdfSearchText(part)).filter(Boolean)
    for (const part of paragraphParts.length > 0 ? paragraphParts : [chunkSource]) {
      const sentences = splitPdfSentences(part)
      if (sentences.length === 0) {
        paragraphChunks.push({ text: part, source: chunkText, type: PDF_CANDIDATE_TYPES.chunkSentence })
      } else {
        for (const sentence of sentences) {
          paragraphChunks.push({ text: sentence, source: chunkText, type: PDF_CANDIDATE_TYPES.chunkSentence })
        }
      }
    }
  }

  const overlapScore = (candidateText) => {
    const needle = normalizePdfSearchText(candidateText).toLowerCase()
    const haystacks = [
      normalizePdfSearchText(evidenceObject?.displayText).toLowerCase(),
      normalizePdfSearchText(evidenceText).toLowerCase(),
      normalizePdfSearchText(snippet).toLowerCase(),
    ].filter(Boolean)
    let best = 0
    for (const haystack of haystacks) {
      let score = 0
      if (needle && haystack.includes(needle)) score += 3
      const needleWords = needle.split(/\s+/).filter(Boolean)
      for (const word of needleWords) {
        if (word.length >= 4 && haystack.includes(word)) score += 1
      }
      best = Math.max(best, score)
    }
    return best
  }

  const stripHeadingOnly = (value) => {
    const stripped = stripPolicyHeading(value)
    if (!stripped || stripped === normalizePdfSearchText(value)) return stripped
    return stripped
  }

  const addOrderedSentence = (text, type, source, reason, allowHeading = false) => {
    const clean = normalizePdfSearchText(text)
    if (!clean) return

    const wordCount = clean.split(/\s+/).filter(Boolean).length
    const isBody = wordCount >= 5 && clean.length >= 30 && !isHeadingOnlyCandidate(clean)
    const isHeading = isHeadingOnlyCandidate(clean)

    if ((type === PDF_CANDIDATE_TYPES.exactEvidence
      || type === PDF_CANDIDATE_TYPES.bodySentence
      || type === PDF_CANDIDATE_TYPES.snippetSentence
      || type === PDF_CANDIDATE_TYPES.chunkSentence) && !isBody) {
      if (!(allowHeading && type === PDF_CANDIDATE_TYPES.exactEvidence && !isHeading)) return
    }

    if (isHeading && !allowHeading) return

    add(clean, type, source, `${reason}; overlap=${overlapScore(clean)}`)
  }

  const addGenericListCandidate = (value, source, allowHeading = false) => {
    const clean = normalizePdfSearchText(value)
    if (!clean) return

    const stripped = stripEmbeddedPdfHeading(stripHeadingOnly(clean))
    const leadingMatch = stripped.match(/^(.{3,40}?)\s*[-:]\s*(.+)$/)
    if (leadingMatch) {
      const label = normalizePdfSearchText(leadingMatch[1])
      const body = normalizePdfSearchText(leadingMatch[2])
      const bodyWords = body.split(/\s+/).filter(Boolean)
      const labelIsGood = label && label.length >= 3 && label.length <= 40 && /[A-Za-z]/.test(label) && !GENERIC_PDF_SEARCH_TERMS.has(label.toLowerCase())

      if (labelIsGood) {
        addHeadingCandidate(label, source, 'label anchor')
      }
      if (bodyWords.length >= 5) {
        addBodyCandidate(bodyWords.slice(0, 12).join(' '), source, 'dash/colon body')
        addBodyCandidate(bodyWords.slice(0, 8).join(' '), source, 'dash/colon body short')
      } else if (bodyWords.length > 0) {
        addBodyCandidate(body, source, 'dash/colon body')
      }
      if (labelIsGood && bodyWords.length > 0) {
        addBodyCandidate(`${label} ${bodyWords.slice(0, 8).join(' ')}`, source, 'label + body')
      }
      addBodyCandidate(stripped, source, 'stripped phrase')
      return
    }

    const words = stripped.split(/\s+/).filter(Boolean)
    const prefixWords = []
    for (let i = 0; i < Math.min(words.length, 3); i++) {
      if (!/^[A-Z0-9][A-Za-z0-9/&()'.-]*$/.test(words[i])) break
      prefixWords.push(words[i])
    }

    if (prefixWords.length > 0) {
      const label = prefixWords.join(' ')
      const remainder = words.slice(prefixWords.length)
      if (prefixWords.length === 1 && remainder.length > 0 && !GENERIC_PDF_SEARCH_TERMS.has(label.toLowerCase())) {
        addHeadingCandidate(label, source, 'prefix label')
      }
      if (remainder.length >= 5) {
        addBodyCandidate(remainder.slice(0, 12).join(' '), source, 'prefix remainder')
        addBodyCandidate(remainder.slice(0, 8).join(' '), source, 'prefix remainder short')
      }
      if (remainder.length > 0 && !isHeadingOnlyCandidate(stripped)) {
        addBodyCandidate(`${label} ${remainder.slice(0, 8).join(' ')}`, source, 'prefix + body')
      }
      addBodyCandidate(stripped, source, 'stripped fallback')
      return
    }

    addBodyCandidate(stripped, source, 'generic stripped')
    if (allowHeading) {
      addHeadingCandidate(clean, source, 'fallback heading')
    }
  }

  for (const sentence of explicitSentenceCandidates) {
    addOrderedSentence(sentence.text, sentence.type, sentence.source, sentence.reason || 'evidence text', false)
  }

  for (const chunk of paragraphChunks) {
    addOrderedSentence(chunk.text, chunk.type, chunk.source, 'chunk sentence')
  }

  for (const value of [evidenceAnchor, evidenceContext, evidenceObject?.displayText, evidenceText, snippet, evidenceObject?.phrase]) {
    addGenericListCandidate(value, value, true)
  }

  for (const anchor of acronymAnchors) {
    if (combinedText.toLowerCase().includes(anchor.toLowerCase())) {
      addAcronymCandidate(anchor, 'combined text', 'acronym anchor')
    }
  }

  const finalCandidates = candidates
    .filter(candidate => candidate.text)
    .map(candidate => ({
      ...candidate,
      orderScore: candidate.score + (candidate.type === PDF_CANDIDATE_TYPES.headingLabel ? -20 : 0),
    }))
    .sort((a, b) => b.orderScore - a.orderScore || b.text.length - a.text.length)

  if (DEV) {
    console.debug(
      '[Spark Viewer] PDF search candidates',
      finalCandidates.map(candidate => ({
        text: candidate.text,
        type: candidate.type,
        source: candidate.source,
        score: candidate.score,
        reason: candidate.reason,
      }))
    )
  }

  return finalCandidates
}

function selectBestPdfSearchCandidate(candidates) {
  const list = Array.isArray(candidates) ? candidates : []

  const contextualBody = list.find(candidate => {
    const text = normalizePdfSearchText(candidate?.text || candidate)
    const wordCount = text.split(/\s+/).filter(Boolean).length
    return (
      candidate?.type === 'body_sentence' &&
      wordCount >= 8 &&
      text.length >= 60 &&
      text.length <= 320 &&
      !isHeadingOnlyCandidate(text)
    )
  })

  if (contextualBody) {
    return stripEmbeddedPdfHeading(contextualBody.text)
  }

  const fullEvidence = list.find(c =>
    c?.type === 'exact_evidence' &&
    c.text &&
    c.text.length > 80
  )

  if (fullEvidence) {
    return stripEmbeddedPdfHeading(fullEvidence.text)
  }

  const bodyCandidates = list.filter(candidate => {
    const text = normalizePdfSearchText(candidate?.text || candidate)
    const wordCount = text.split(/\s+/).filter(Boolean).length
    return (
      candidate?.type === 'body_sentence' ||
      candidate?.type === 'snippet_sentence' ||
      candidate?.type === 'chunk_sentence' ||
      (wordCount >= 5 && text.length >= 30 && !isHeadingOnlyCandidate(text))
    )
  })

  const preferred = bodyCandidates[0] || list[0]
  return preferred ? stripEmbeddedPdfHeading(preferred.text || preferred) : ''
}

function buildHighlightCandidates({
  backendPhrase = '',
  evidenceAnchor = '',
  contextText = '',
  evidenceText = '',
  snippet = '',
  chunkText = '',
  answer = '',
  question = '',
  manualMode = false,
  manualSearch = '',
  prioritizeBackend = false,
  includeWindows = true,
}) {
  const ordered = []

  if (manualMode && manualSearch.trim()) {
    ordered.push(manualSearch.trim())
  }

  ordered.push(evidenceAnchor)
  ordered.push(contextText)

  if (prioritizeBackend) {
    ordered.push(backendPhrase)
    ordered.push(evidenceText)
  } else {
    ordered.push(evidenceText)
    ordered.push(backendPhrase)
  }
  ordered.push(snippet)
  ordered.push(chunkText)
  if (includeWindows) {
    addWindowsForText(ordered, chunkText)
    addWindowsForText(ordered, snippet)
    addWindowsForText(ordered, answer)
  }

  if (question && manualMode && !manualSearch.trim()) {
    ordered.push(question)
  }

  if (!manualMode && !includeWindows) {
    const meaningfulSentenceFallbacks = extractMeaningfulSentenceChunks(
      [evidenceAnchor, contextText, backendPhrase, snippet, answer, evidenceText],
      8
    )
    ordered.push(...meaningfulSentenceFallbacks)
  }

  return uniqueOrdered(ordered)
}

function escapeRegExp(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function highlightTokens(value) {
  return norm(value).split(/\s+/).filter(Boolean)
}

function isUnsafeTinyHighlightPhrase(value) {
  const clean = String(value || '').trim()
  const normalized = norm(clean)
  if (!normalized || normalized.length <= 1) return true

  const tokens = highlightTokens(clean)
  if (tokens.length === 0) return true

  // A 1-2 character phrase is the exact bug class: it can mark letters
  // inside unrelated words. Keep 3+ character acronyms, but reject tiny
  // fragments for inline document highlights.
  if (tokens.length === 1 && tokens[0].length <= 2) return true

  if (GENERIC_PDF_SEARCH_TERMS.has(normalized) && tokens.length <= 2) return true

  return false
}

function shouldUseBoundaryHighlight(value) {
  const normalized = norm(value)
  const tokens = highlightTokens(value)
  return normalized.length < 20 || tokens.length <= 2
}

function buildHighlightRegex(phrase) {
  const clean = String(phrase || '').trim()
  if (isUnsafeTinyHighlightPhrase(clean)) return null

  const escaped = escapeRegExp(clean).replace(/\s+/g, '\\s+')
  if (shouldUseBoundaryHighlight(clean)) {
    return {
      regex: new RegExp(`(^|[^A-Za-z0-9_])(${escaped})(?=$|[^A-Za-z0-9_])`, 'ig'),
      groupIndex: 2,
    }
  }

  return {
    regex: new RegExp(escaped, 'ig'),
    groupIndex: 0,
  }
}

function buildHighlightRanges(text, phrases) {
  const source = String(text || '')
  const ordered = uniqueOrdered(
    (Array.isArray(phrases) ? phrases : [])
      .map(value => String(value || '').trim())
      .filter(Boolean)
  ).sort((a, b) => b.length - a.length)

  if (!source || ordered.length === 0) {
    return []
  }

  const occupied = []
  const ranges = []

  for (const phrase of ordered) {
    const search = buildHighlightRegex(phrase)
    if (!search) continue

    const { regex: pattern, groupIndex } = search
    let match
    while ((match = pattern.exec(source)) !== null) {
      const matchedText = groupIndex === 0 ? match[0] : match[groupIndex]
      if (!matchedText) {
        if (pattern.lastIndex <= match.index) pattern.lastIndex = match.index + 1
        continue
      }

      const leadingLength = groupIndex === 0 ? 0 : (match[1] || '').length
      const start = match.index + leadingLength
      const end = start + matchedText.length
      const overlaps = occupied.some(range => start < range.end && end > range.start)
      if (overlaps) {
        if (pattern.lastIndex <= start) {
          pattern.lastIndex = start + 1
        }
        continue
      }
      occupied.push({ start, end })
      ranges.push({ start, end, phrase })
      if (pattern.lastIndex <= start) {
        pattern.lastIndex = end
      }
    }
  }

  return ranges.sort((a, b) => a.start - b.start || b.end - a.end)
}

function renderHighlightedText(text, phrases, className = '') {
  const source = String(text || '')
  if (!source) {
    return null
  }

  const ranges = buildHighlightRanges(source, phrases)
  if (ranges.length === 0) {
    return source
  }

  const nodes = []
  let cursor = 0

  ranges.forEach((range, index) => {
    if (range.start > cursor) {
      nodes.push(source.slice(cursor, range.start))
    }
    nodes.push(
      <mark key={`${range.start}-${range.end}-${index}`} className={className}>
        {source.slice(range.start, range.end)}
      </mark>
    )
    cursor = range.end
  })

  if (cursor < source.length) {
    nodes.push(source.slice(cursor))
  }

  return nodes
}

function _normalizeBlockText(block) {
  const type = String(block?.type || 'paragraph')
  const text = String(block?.text || '')
  if (type === 'list') {
    const items = Array.isArray(block?.items) && block.items.length > 0 ? block.items : text.split('\n')
    return items.map(it => `• ${String(it).trim()}`).join('\n')
  }
  if (type === 'table') {
    const rows = Array.isArray(block?.rows) ? block.rows : []
    return rows.map(row => row.join(' | ')).join('\n')
  }
  return text
}

function renderBlockText(block, phrases, styles) {
  const type = String(block?.type || 'paragraph')
  const text = String(block?.text || '')

  if (type === 'heading') {
    const level = Math.min(Math.max(Number(block?.level || 2), 1), 3)
    const HeadingTag = `h${level}`
    return <HeadingTag className={styles.docHeading}>{renderHighlightedText(text, phrases, styles.blockHighlight)}</HeadingTag>
  }

  if (type === 'list') {
    const items = Array.isArray(block?.items) && block.items.length > 0 ? block.items : text.split('\n')
    return (
      <ul className={styles.docList}>
        {items.map((item, index) => (
          <li key={index} className={styles.docListItem}>
            {renderHighlightedText(String(item), phrases, styles.blockHighlight)}
          </li>
        ))}
      </ul>
    )
  }

  if (type === 'pre') {
    return <pre className={styles.docPre}>{renderHighlightedText(text, phrases, styles.blockHighlight)}</pre>
  }

  if (type === 'table') {
    const rows = Array.isArray(block?.rows) ? block.rows : []
    return (
      <div className={styles.docTableWrap}>
        <table className={styles.docTable}>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex} className={styles.docTableCell}>
                    {renderHighlightedText(String(cell), phrases, styles.blockHighlight)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }

  return <p className={styles.docParagraph}>{renderHighlightedText(text, phrases, styles.blockHighlight)}</p>
}

function normalizeSpreadsheetSheet(sheet) {
  const headers = Array.isArray(sheet?.headers) ? sheet.headers.map(value => String(value || '').trim()).filter(Boolean) : []
  const rows = Array.isArray(sheet?.preview_rows) ? sheet.preview_rows : []
  return {
    sheetName: String(sheet?.sheet_name || 'Sheet 1'),
    usedRange: String(sheet?.used_range || ''),
    maxRow: Number(sheet?.max_row || 0),
    maxColumn: Number(sheet?.max_column || 0),
    headers,
    rows,
  }
}

function renderSpreadsheetPreview(sheet, highlightMatches, styles) {
  if (!sheet) return null

  const activeSheetName = String(highlightMatches?.[0]?.sheetName || highlightMatches?.[0]?.sheet_name || '').trim().toLowerCase()
  const highlightedRow = Number(highlightMatches?.[0]?.rowNumber || highlightMatches?.[0]?.row_number || 0)
  const highlightedRange = String(highlightMatches?.[0]?.rangeRef || highlightMatches?.[0]?.range_ref || '').trim().toLowerCase()
  const matchedSheet = activeSheetName && activeSheetName === sheet.sheetName.toLowerCase()
  const titlePhrases = matchedSheet ? [sheet.sheetName, sheet.usedRange].filter(Boolean) : [sheet.sheetName].filter(Boolean)

  return (
    <section className={styles.xlsxSheet}>
      <div className={styles.xlsxSheetHeader}>
        <div>
          <div className={styles.xlsxSheetTitle}>{renderHighlightedText(sheet.sheetName, titlePhrases, styles.blockHighlight)}</div>
          <div className={styles.xlsxSheetMeta}>
            {sheet.usedRange || 'Used range unavailable'}
            {sheet.maxRow ? ` · ${sheet.maxRow} rows` : ''}
            {sheet.maxColumn ? ` · ${sheet.maxColumn} columns` : ''}
          </div>
        </div>
      </div>
      <div className={styles.docTableWrap}>
        <table className={styles.docTable}>
          <thead>
            <tr>
              <th className={styles.docTableHeadCell}>Row</th>
              {sheet.headers.map((header, index) => (
                <th key={`${header}-${index}`} className={styles.docTableHeadCell}>
                  {header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sheet.rows.length > 0 ? sheet.rows.map(row => {
              const isActiveRow = highlightedRow && Number(row.row_number) === highlightedRow
              const cells = Array.isArray(row.cells) ? row.cells : []
              return (
                <tr key={row.row_number} className={isActiveRow ? styles.docTableRowHighlight : ''}>
                  <td className={styles.docTableCell}>{row.row_number}</td>
                  {cells.map((cell, cellIndex) => {
                    const cellValue = String(cell?.value || '')
                    const isRangeMatch = highlightedRange && String(cell?.header || '').trim().toLowerCase().includes(highlightedRange)
                    return (
                      <td key={`${row.row_number}-${cellIndex}`} className={`${styles.docTableCell} ${isActiveRow || isRangeMatch ? styles.docTableCellHighlight : ''}`}>
                        {cellValue}
                      </td>
                    )
                  })}
                </tr>
              )
            }) : (
              <tr>
                <td className={styles.docTableCell} colSpan={Math.max(sheet.headers.length + 1, 2)}>
                  No preview rows available.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default function ViewerApp({
  path,
  sourcePath,
  sourceName,
  evidenceAnchor,
  evidenceContext,
  evidenceText,
  snippet,
  chunkIndex,
  answer,
  question,
  chunkText,
  chunkId,
  pageNumber,
  extractionMethod,
  hasTextLayer,
  ocrConfidence,
}) {
  const [meta, setMeta] = useState(null)
  const [status, setStatus] = useState('loading')
  const [error, setError] = useState(null)
  const [manualSearch, setManualSearch] = useState('')
  const [manualMode, setManualMode] = useState(false)
  const [highlightMatches, setHighlightMatches] = useState([])
  const [highlightInfo, setHighlightInfo] = useState(null)
  const [primaryBlockIndexes, setPrimaryBlockIndexes] = useState([])
  const [secondaryBlockIndexes, setSecondaryBlockIndexes] = useState([])
  const [copyStatus, setCopyStatus] = useState('')
  const [userTargetPage, setUserTargetPage] = useState(null)
  const [evidenceExpanded, setEvidenceExpanded] = useState(false)
  const blockRefs = useRef(new Map())
  const viewerBodyRef = useRef(null)
  const scrolledRef = useRef(false)
  const copyTimerRef = useRef(null)
  const pdfFrameRef = useRef(null)
  const [pdfFrameReady, setPdfFrameReady] = useState(false)
  const autoSearchSignatureRef = useRef('')
  const pdfFindSucceededRef = useRef(false)
  const pendingPdfRequestIdRef = useRef(null)

  const resolvedPath = sourcePath || path || ''

  const fileUrl = useMemo(() => {
    if (!resolvedPath) return ''
    return `${API}/document/file?path=${encodeURIComponent(resolvedPath)}`
  }, [resolvedPath])

  const documentFile = useMemo(() => {
    if (!fileUrl) return null
    return { url: fileUrl }
  }, [fileUrl])

  const resolvedTitle = useMemo(() => {
    return meta?.file_name || sourceName || resolvedPath.split('\\').pop().split('/').pop() || 'Document'
  }, [meta?.file_name, sourceName, resolvedPath])

  const previewType = String(meta?.viewerType || meta?.extension || '').toLowerCase()
  const isPdf = previewType === 'pdf'
  const isSpreadsheet = previewType === 'xlsx' || previewType === 'xlsm' || previewType === 'csv'
  const canPreview = meta?.canPreview !== false
  const isStructuredPreview = canPreview && !isPdf && (previewType === 'docx' || previewType === 'text' || isSpreadsheet)
  const documentBlocks = Array.isArray(meta?.documentTextLayer) ? meta.documentTextLayer : []
  const documentPlainText = String(meta?.plainText || '')
  const spreadsheetSheets = Array.isArray(meta?.spreadsheetSheets) ? meta.spreadsheetSheets.map(normalizeSpreadsheetSheet) : []

  useEffect(() => {
    setUserTargetPage(null)
    setEvidenceExpanded(false)
  }, [resolvedPath])

  const evidenceObject = useMemo(() => {
    const firstMatch = highlightMatches?.[0] || null
    const resolvedExtractionMethod = normalizeExtractionMethod(
      firstMatch?.extractionMethod || extractionMethod || (firstMatch?.hasTextLayer === false || hasTextLayer === false ? 'ocr' : '')
    )
    const resolvedHasTextLayer = firstMatch?.hasTextLayer ?? hasTextLayer ?? (resolvedExtractionMethod === 'text_layer' ? true : resolvedExtractionMethod === 'ocr' ? false : null)
    const extractionDisplay = isPdf
      ? (EXTRACTION_DISPLAY[resolvedExtractionMethod] || EXTRACTION_DISPLAY.unknown)
      : { label: 'Document text', note: 'Structured text extracted from the source document.' }
    
    // pageNumber fallback
    const resolvedPageNum = Number(firstMatch?.pageNumber || highlightInfo?.pageNumber || pageNumber || 1)
    const safePageNum = Number.isFinite(resolvedPageNum) && resolvedPageNum > 0 ? resolvedPageNum : 1

    // phrase/displayText fallback
    const firstPhrase = normalizePdfSearchText(firstMatch?.phrase || '')
    let display = "No evidence text available"
    if (evidenceContext && evidenceContext.length >= 40 && !isGenericEvidencePhrase(evidenceContext)) {
      display = evidenceContext
    } else if (evidenceAnchor && evidenceAnchor.length >= 25 && !isGenericEvidencePhrase(evidenceAnchor)) {
      display = evidenceAnchor
    } else if (firstPhrase && firstPhrase.length >= 25 && !isGenericEvidencePhrase(firstPhrase)) {
      display = firstPhrase
    } else if (evidenceText && !isGenericEvidencePhrase(evidenceText)) {
      display = evidenceText
    } else if (snippet && !isGenericEvidencePhrase(snippet)) {
      display = snippet
    } else if (chunkText && !isGenericEvidencePhrase(chunkText)) {
      display = chunkText
    } else if (firstPhrase && !isGenericEvidencePhrase(firstPhrase)) {
      display = firstPhrase
    } else if (snippet) {
      display = snippet
    } else if (chunkText) {
      display = chunkText
    }

    // qualityLabel mapping
    let label = isPdf ? "Page match" : "Block match"
    const scoreVal = Number(firstMatch?.score || 0)
    const mType = String(firstMatch?.matchType || '').toLowerCase()
    
    if (mType.includes('metadata')) {
      label = "Metadata match"
    } else if (scoreVal >= 0.75) {
      label = "Strong match"
    } else if (scoreVal >= 0.45) {
      label = "Moderate match"
    } else if (scoreVal > 0) {
      label = "Weak match"
    }

    return {
      exists: !!firstMatch || (!!isPdf && Number.isFinite(safePageNum) && safePageNum > 0),
      pageNumber: safePageNum,
      phrase: firstMatch?.phrase || '',
      matchType: firstMatch?.matchType || '',
      extractionMethod: resolvedExtractionMethod,
      hasTextLayer: resolvedHasTextLayer,
      ocrConfidence: firstMatch?.ocrConfidence ?? ocrConfidence ?? null,
      sourceLabel: extractionDisplay.label,
      sourceNote: extractionDisplay.note,
      score: scoreVal,
      displayText: display,
      qualityLabel: label,
      warning: highlightInfo?.warning || '',
      found: highlightInfo?.found ?? !!firstMatch,
    }
  }, [highlightMatches, highlightInfo, evidenceText, snippet, chunkText, pageNumber, extractionMethod, hasTextLayer, ocrConfidence, isPdf])

  const evidenceSearchNote = 'This was OCR-read text. If the PDF does not highlight it, use the evidence text shown here.'
  const evidenceMatchStrength = evidenceObject.qualityLabel === 'Page match'
    ? 'Match'
    : evidenceObject.qualityLabel.split(' ')[0]

  const spreadsheetSheet = useMemo(() => {
    if (!isSpreadsheet) return null
    const matchSheetName = String(highlightMatches?.[0]?.sheetName || highlightMatches?.[0]?.sheet_name || '').trim().toLowerCase()
    if (matchSheetName) {
      const matched = spreadsheetSheets.find(sheet => sheet.sheetName.toLowerCase() === matchSheetName)
      if (matched) return matched
    }
    return spreadsheetSheets[0] || null
  }, [highlightMatches, isSpreadsheet, spreadsheetSheets])


  const pdfSearchCandidates = useMemo(() => {
    if (!isPdf || !evidenceObject?.exists) {
      return []
    }

    return buildPdfSearchCandidatesV2(evidenceObject, evidenceAnchor, evidenceContext, evidenceText, snippet, chunkText)
  }, [chunkText, evidenceAnchor, evidenceContext, evidenceObject, evidenceText, isPdf, snippet])

  const jumpToEvidencePage = useCallback(() => {
    if (evidenceObject.pageNumber) {
      setUserTargetPage(evidenceObject.pageNumber)
    }
  }, [evidenceObject.pageNumber])

  const jumpToEvidenceBlock = useCallback(() => {
    const firstPrimary = (Array.isArray(primaryBlockIndexes) ? primaryBlockIndexes : [])
      .map(value => Number(value))
      .find(value => Number.isInteger(value))
    const firstMatch = highlightMatches.find(match => match.blockIndex !== null && match.blockIndex !== undefined)
    const targetIndex = Number.isInteger(firstPrimary) ? firstPrimary : Number(firstMatch?.blockIndex)
    if (!Number.isInteger(targetIndex)) return
    const el = blockRefs.current.get(targetIndex)
    if (!el) return
    el.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }, [highlightMatches, primaryBlockIndexes])

  const sendPdfJsFindCommandWithCandidates = useCallback((query, candidates = [], options = {}) => {
    const frame = pdfFrameRef.current
    const searchCandidates = Array.isArray(candidates)
      ? candidates
          .map(candidate => stripEmbeddedPdfHeading(normalizePdfSearchText(candidate?.text || candidate)))
          .filter(Boolean)
      : []
    const cleanQuery = stripEmbeddedPdfHeading(String(query || '').trim())
    const targetPage = Number(options?.targetPage || 0)

    if (!frame?.contentWindow || (!cleanQuery && searchCandidates.length === 0)) {
      return false
    }

    // Generate a unique requestId so we can reject stale responses from earlier
    // citation clicks. The bridge echoes this back on spark-pdf-find-result.
    const requestId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
    pendingPdfRequestIdRef.current = requestId

    try {
      if (DEV) {
        logDev('pdf.js find invoked', {
          query: cleanQuery,
          candidateCount: searchCandidates.length,
          candidatesPreview: searchCandidates.slice(0, 4),
          targetPage,
          requestId,
        })
      }
      frame.contentWindow.postMessage(
        {
          type: 'spark-pdf-find',
          query: cleanQuery,
          candidates: searchCandidates,
          targetPage: Number.isFinite(targetPage) && targetPage > 0 ? targetPage : undefined,
          requestId,
        },
        window.location.origin
      )
      return true
    } catch (err) {
      console.error('[Spark Viewer] Failed to send PDF search command', err)
      return false
    }
  }, [])
  const inlinePdfUrl = useMemo(() => {
    if (!isPdf || !resolvedPath) return ''
    return `${API}/document/inline?path=${encodeURIComponent(resolvedPath)}`
  }, [isPdf, resolvedPath])

  const pdfViewerUrl = useMemo(() => {
    if (!isPdf || !inlinePdfUrl) return ''

    // Keep the iframe URL stable for the current PDF.
    // Do not derive this from highlightMatches/userTargetPage; changing the iframe
    // src after PDF.js has found text reloads the viewer and clears the highlight.
    const propPage = Number(pageNumber || 0)
    const safePage = Math.max(1, Math.floor(Number.isFinite(propPage) && propPage > 0 ? propPage : 1))
    const appBase = import.meta.env.BASE_URL || '/'
    const viewerPath = `${appBase.replace(/\/$/, '')}/pdfjs/web/viewer.html`
    const fileParam = encodeURIComponent(inlinePdfUrl)

    return `${viewerPath}?file=${fileParam}&sparkBridgeVersion=spark-pdfjs-bridge-20260419-v5#page=${safePage}&zoom=page-actual`
  }, [isPdf, inlinePdfUrl, pageNumber])

  useEffect(() => {
    setPdfFrameReady(false)
  }, [pdfViewerUrl])

  useEffect(() => {
    if (!isPdf || !pdfFrameReady || !pdfViewerUrl || !evidenceObject?.pageNumber) return
    if (highlightInfo?.found === false) return

    const query = selectBestPdfSearchCandidate(pdfSearchCandidates)

    if (!query) return

    const signature = `${resolvedPath}|${evidenceObject.pageNumber}|${pdfSearchCandidates.map(candidate => candidate?.text || candidate).join('||')}`
    if (autoSearchSignatureRef.current === signature) return
    autoSearchSignatureRef.current = signature

    const timers = []
    pdfFindSucceededRef.current = false

    const sendSearch = () => {
      // A later duplicate PDF.js find can clear/repaint highlights while the user
      // scrolls. Once the bridge confirms a match, stop retrying.
      if (pdfFindSucceededRef.current) return
      sendPdfJsFindCommandWithCandidates(query, pdfSearchCandidates, {
        targetPage: evidenceObject.pageNumber,
      })
    }

    timers.push(window.setTimeout(() => {
      timers.push(window.setTimeout(sendSearch, 350))
      timers.push(window.setTimeout(sendSearch, 1600))

      setCopyStatus('Evidence search sent to PDF.')
      if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current)
      copyTimerRef.current = window.setTimeout(() => setCopyStatus(''), 4000)
    }, 500))

    return () => timers.forEach(timer => window.clearTimeout(timer))
  }, [
    isPdf,
    pdfFrameReady,
    pdfViewerUrl,
    evidenceObject,
    highlightInfo?.found,
    pdfSearchCandidates,
    resolvedPath,
    sendPdfJsFindCommandWithCandidates,
  ])


  useEffect(() => {
    if (!DEV || !isPdf || !pdfViewerUrl) return
    console.debug('[Spark Viewer] PDF.js iframe URL', pdfViewerUrl)
  }, [isPdf, pdfViewerUrl])


  const blockMatchPhrasesMap = useMemo(() => {
    const map = new Map()
    for (const match of highlightMatches) {
      if (match.blockIndex === null || match.blockIndex === undefined) continue
      const list = map.get(match.blockIndex) || []
      list.push(match)
      map.set(match.blockIndex, list)
    }
    for (const [key, list] of map.entries()) {
      const ordered = list
        .slice()
        .sort((a, b) => {
          const byScore = Number(b?.score || 0) - Number(a?.score || 0)
          if (byScore !== 0) return byScore
          return String(b?.phrase || '').length - String(a?.phrase || '').length
        })
      map.set(key, ordered)
    }
    return map
  }, [highlightMatches])

  const primaryBlockSet = useMemo(() => new Set(
    (Array.isArray(primaryBlockIndexes) ? primaryBlockIndexes : [])
      .map(value => Number(value))
      .filter(value => Number.isInteger(value))
  ), [primaryBlockIndexes])

  const secondaryBlockSet = useMemo(() => new Set(
    (Array.isArray(secondaryBlockIndexes) ? secondaryBlockIndexes : [])
      .map(value => Number(value))
      .filter(value => Number.isInteger(value))
  ), [secondaryBlockIndexes])


  const resetPdfHighlightState = useCallback(() => {
    scrolledRef.current = false
    pdfFindSucceededRef.current = false
    autoSearchSignatureRef.current = ''
    pendingPdfRequestIdRef.current = null
  }, [])

  useEffect(() => {
    if (!isPdf) return
    const onMessage = (event) => {
      if (event.origin !== window.location.origin) return
      const payload = event.data || {}
      if (payload.type !== 'spark-pdf-find-result') return

      // Ignore stale responses from earlier citation clicks. The bridge echoes
      // the requestId we sent on spark-pdf-find; any result whose requestId
      // doesn't match the most recent one we dispatched is stale and must not
      // overwrite UI state for the current citation.
      if (payload.requestId && payload.requestId !== pendingPdfRequestIdRef.current) {
        if (DEV) {
          logDev('pdf.js find result ignored (stale)', {
            receivedRequestId: payload.requestId,
            pendingRequestId: pendingPdfRequestIdRef.current,
          })
        }
        return
      }

      if (DEV) {
        logDev('pdf.js find result', payload)
      }
      if (payload.found) {
        pdfFindSucceededRef.current = true
      }
      if (!payload.found && evidenceObject?.pageNumber) {
        setCopyStatus('Relevant page found, but exact PDF text highlight was not available.')
        if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current)
        copyTimerRef.current = window.setTimeout(() => setCopyStatus(''), 4500)
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [isPdf, evidenceObject?.pageNumber])

  const blockCandidates = useCallback((blockIndex) => {
    if (manualMode && manualSearch.trim()) {
      return uniqueOrdered([manualSearch.trim()])
    }

    if (isPdf) {
      const match = (blockMatchPhrasesMap.get(blockIndex) || [])[0]
      return buildHighlightCandidates({
        backendPhrase: match?.phrase || '',
        evidenceAnchor: evidenceAnchor || '',
        contextText: evidenceContext || '',
        evidenceText: evidenceText || '',
        snippet: snippet || '',
        chunkText: chunkText || '',
        answer: answer || '',
        question: question || '',
        manualMode,
        manualSearch,
      })
    }

    if (!primaryBlockSet.has(blockIndex)) {
      return []
    }

    const blockMatches = blockMatchPhrasesMap.get(blockIndex) || []
    const strongest = blockMatches
      .filter(match => match?.isPrimary !== false)
      .slice(0, 1)
      .map(match => String(match?.phrase || '').trim())
      .filter(Boolean)

    return uniqueOrdered(strongest)
  }, [answer, blockMatchPhrasesMap, chunkText, evidenceAnchor, evidenceContext, evidenceText, isPdf, manualMode, manualSearch, primaryBlockSet, question, snippet])



  useEffect(() => {
    let mounted = true
    // AbortController kills in-flight fetches on cleanup. This matters for:
    // 1. React 18 StrictMode in dev, which double-mounts effects — without
    //    abort, both mounts fire a full /document/meta round-trip. Cutting
    //    the duplicate saves ~960ms per citation click on PDFs.
    // 2. Rapid citation-click scenarios where the user clicks B before A
    //    has resolved — the A response is discarded cleanly.
    const controller = new AbortController()
    async function loadMeta() {
      if (!resolvedPath) {
        setError('No file path provided.')
        setStatus('error')
        return
      }

      setStatus('loading')
      setError(null)

      try {
        const res = await fetch(
          `${API}/document/meta?path=${encodeURIComponent(resolvedPath)}`,
          { signal: controller.signal }
        )
        if (!res.ok) {
          const payload = await res.json().catch(() => ({}))
          throw new Error(payload.detail || `HTTP ${res.status}`)
        }
        const payload = await res.json()
        if (!mounted) return
        setMeta(payload)
        if (payload.canPreview === false) {
          setStatus('unsupported')
        } else if ((payload.viewerType || payload.extension || '').toLowerCase() === 'pdf') {
          setStatus('ready')
        } else {
          setStatus('ready')
        }
      } catch (err) {
        if (!mounted) return
        // Aborted fetches are expected during cleanup; don't surface them
        // as errors to the user.
        if (err?.name === 'AbortError') return
        setError(err.message || 'Could not load document metadata.')
        setStatus('error')
      }
    }

    loadMeta()
    return () => {
      mounted = false
      controller.abort()
    }
  }, [resolvedPath])

  useEffect(() => {
    return () => {
      if (copyTimerRef.current) {
        window.clearTimeout(copyTimerRef.current)
      }
    }
  }, [])

  useEffect(() => {
    let mounted = true
    // AbortController cancels in-flight POST /document/highlight calls when
    // the effect re-runs or unmounts. This effect has many dependencies
    // (evidenceText, snippet, chunkText, etc.) so it can re-fire rapidly
    // in certain flows — without abort, a stale response can arrive after
    // a newer one and clobber the UI. StrictMode double-mount is the other
    // common trigger.
    const controller = new AbortController()
    async function loadHighlight() {
      if (!resolvedPath || !canPreview) {
        setHighlightMatches([])
        setHighlightInfo(null)
        setPrimaryBlockIndexes([])
        setSecondaryBlockIndexes([])
        return
      }

      try {
        const payloadBody = {
          path: resolvedPath,
          evidenceAnchor: evidenceAnchor || '',
          evidenceContext: evidenceContext || '',
          evidenceText: evidenceText || '',
          snippet: snippet || '',
          chunkText: chunkText || '',
          answer: answer || '',
          question: question || '',
          chunkIndex: Number(chunkIndex || 0),
          chunkId: chunkId || '',
          extractionMethod: evidenceObject.extractionMethod || extractionMethod || '',
          hasTextLayer: evidenceObject.hasTextLayer ?? hasTextLayer,
          ocrConfidence: evidenceObject.ocrConfidence ?? ocrConfidence ?? null,
        }
        if (pageNumber !== null && pageNumber !== undefined) {
          payloadBody.pageNumber = pageNumber
        }
        if (DEV) {
          logDev('document highlight request', payloadBody)
        }

        const res = await fetch(`${API}/document/highlight`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payloadBody),
          signal: controller.signal,
        })
        if (!res.ok) {
          const payload = await res.json().catch(() => ({}))
          throw new Error(payload.detail || `HTTP ${res.status}`)
        }
        const payload = await res.json()
        if (!mounted) return
        setHighlightMatches(Array.isArray(payload.matches) ? payload.matches : [])
        setHighlightInfo({
          found: payload.found !== false,
          matchType: payload.matchType || payload.matches?.[0]?.matchType || '',
          pageNumber: Number(payload.pageNumber || payload.matches?.[0]?.pageNumber || 0) || null,
          matchedText: payload.matchedText || payload.primaryMatchText || payload.matches?.[0]?.phrase || '',
          candidateCount: Number(payload.candidateCount || 0) || 0,
          warning: payload.warning || '',
        })
        setPrimaryBlockIndexes(Array.isArray(payload.primaryBlockIndexes) ? payload.primaryBlockIndexes : [])
        setSecondaryBlockIndexes(Array.isArray(payload.secondaryBlockIndexes) ? payload.secondaryBlockIndexes : [])
        setManualMode(false)
        setManualSearch('')
        resetPdfHighlightState()
        if (DEV) {
          logDev('document highlight response', {
            found: payload.found !== false,
            matchesLength: Array.isArray(payload.matches) ? payload.matches.length : 0,
            firstMatchPhrase: payload.matches?.[0]?.phrase || '',
            pageNumber: payload.matches?.[0]?.pageNumber || null,
            blockIndex: payload.matches?.[0]?.blockIndex || null,
            primaryBlockIndexes: payload.primaryBlockIndexes || [],
            secondaryBlockIndexes: payload.secondaryBlockIndexes || [],
            matchType: payload.matches?.[0]?.matchType || '',
            normalizedLength: payload.normalizedLength || 0,
            phrasePreview: payload.phrasePreview || '',
            candidateCount: payload.candidateCount || 0,
            warning: payload.warning || '',
          })
        }
      } catch (err) {
        if (!mounted) return
        // Silently drop aborted requests — they are expected during cleanup
        // and should not trigger the "clear highlight state" reset below.
        if (err?.name === 'AbortError') return
        setHighlightMatches([])
        setHighlightInfo(null)
        setPrimaryBlockIndexes([])
        setSecondaryBlockIndexes([])
        if (DEV) {
          logDev('highlight request failed', err)
        }
      }
    }

    loadHighlight()
    return () => {
      mounted = false
      controller.abort()
    }
  }, [resolvedPath, canPreview, evidenceText, snippet, chunkText, answer, question, chunkIndex, chunkId, pageNumber, resetPdfHighlightState])


  useEffect(() => {
    if (isPdf || status !== 'ready' || !isStructuredPreview) return
    if (scrolledRef.current) return

    const firstPrimary = (Array.isArray(primaryBlockIndexes) ? primaryBlockIndexes : [])
      .map(value => Number(value))
      .find(value => Number.isInteger(value))
    const firstMatch = highlightMatches.find(match => match.blockIndex !== null && match.blockIndex !== undefined)
    const targetIndex = Number.isInteger(firstPrimary) ? firstPrimary : firstMatch?.blockIndex
    if (targetIndex === null || targetIndex === undefined) {
      return
    }

    const el = blockRefs.current.get(targetIndex)
    if (!el) {
      return
    }

    scrolledRef.current = true
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' })
      })
    })
  }, [highlightMatches, isPdf, isStructuredPreview, status, manualMode, manualSearch, documentBlocks.length, primaryBlockIndexes])


  const registerBlockRef = useCallback((blockIndex, el) => {
    if (!el) {
      blockRefs.current.delete(blockIndex)
      return
    }
    blockRefs.current.set(blockIndex, el)
  }, [])



  const manualSearchChange = useCallback((value) => {
    setManualMode(true)
    setManualSearch(value)
    resetPdfHighlightState()
  }, [resetPdfHighlightState])

  const resetSearch = useCallback(() => {
    setManualMode(false)
    setManualSearch('')
    resetPdfHighlightState()
  }, [resetPdfHighlightState])

  const openOriginal = useCallback(() => {
    if (!fileUrl) return
    window.open(fileUrl, '_blank', 'noopener,noreferrer')
  }, [fileUrl])

  const downloadOriginal = useCallback(() => {
    if (!fileUrl) return
    const link = document.createElement('a')
    link.href = fileUrl
    link.download = meta?.file_name || resolvedTitle
    link.rel = 'noopener noreferrer'
    document.body.appendChild(link)
    link.click()
    link.remove()
  }, [fileUrl, meta?.file_name, resolvedTitle])

  const copyText = useCallback(async () => {
    const text = documentPlainText || (documentBlocks || []).map(block => _normalizeBlockText(block)).join('\n\n')
    if (!text.trim()) return
    try {
      await navigator.clipboard.writeText(text)
      setCopyStatus('Copied')
    } catch (err) {
      const fallback = document.createElement('textarea')
      fallback.value = text
      fallback.setAttribute('readonly', 'true')
      fallback.style.position = 'fixed'
      fallback.style.opacity = '0'
      document.body.appendChild(fallback)
      fallback.select()
      document.execCommand('copy')
      fallback.remove()
      setCopyStatus('Copied')
    }
    if (copyTimerRef.current) {
      window.clearTimeout(copyTimerRef.current)
    }
    copyTimerRef.current = window.setTimeout(() => setCopyStatus(''), 1500)
  }, [documentBlocks, documentPlainText])

  const pdfTextLayer = meta?.pdfTextLayer || []


  return (
    <div className={styles.root}>
      <header className={styles.toolbar}>
        <div className={styles.toolbarLeft}>
          <button className={styles.backBtn} onClick={() => window.close()} aria-label="Close viewer">
            Close
          </button>
          <div className={styles.toolbarDivider} />
          <div className={styles.docTitleBlock}>
            <div className={styles.docTitle}>{resolvedTitle}</div>
            <div className={styles.docMeta}>
              {meta?.extension?.toUpperCase() || 'FILE'}
              {typeof meta?.page_count === 'number' ? ` · ${meta?.page_count} pages` : ''}
            </div>
          </div>
        </div>

        <div className={styles.toolbarRight}>
          <button type="button" className={styles.toolbarBtn} onClick={openOriginal}>Open original</button>
          <button type="button" className={styles.toolbarBtn} onClick={downloadOriginal}>Download</button>
        </div>
      </header>

      {isPdf && (
        <div className={styles.searchBar}>
          <span className={styles.searchHint}>
            Use the PDF toolbar search inside the document preview.
          </span>
        </div>
      )}

      {isStructuredPreview && (
        <div className={styles.previewBar}>
          <div className={styles.previewBarLeft}>
            <span className={styles.searchLabel}>Search within document</span>
            <input
              type="text"
              className={styles.searchInput}
              value={manualSearch}
              onChange={e => manualSearchChange(e.target.value)}
              placeholder="Type to search and highlight..."
            />
            <button type="button" className={styles.resetBtn} onClick={resetSearch}>Reset</button>
            <span className={styles.searchHint}>
              {manualMode && manualSearch.trim()
                ? 'Manual search'
                : highlightMatches.length > 0
                  ? 'Highlighted from source metadata'
                  : 'Waiting for source metadata'}
            </span>
          </div>
          <div className={styles.previewBarRight}>
            <button type="button" className={styles.toolbarBtn} onClick={copyText}>Copy text</button>
            <button type="button" className={styles.toolbarBtn} onClick={openOriginal}>Open original</button>
            {copyStatus ? <span className={styles.copyStatus}>{copyStatus}</span> : null}
          </div>
        </div>
      )}

      <div className={styles.viewerBody} ref={viewerBodyRef}>
        {status === 'loading' && (
          <div className={styles.state}>
            <div className={styles.spinner} />
            <div>Loading document metadata...</div>
          </div>
        )}

        {status === 'error' && (
          <div className={styles.state}>
            <div className={styles.errorTitle}>Could not load document</div>
            <div className={styles.errorBody}>{error}</div>
          </div>
        )}

        {status === 'unsupported' && (
          <div className={styles.docxFallback}>
            <div className={styles.docxTitle}>Preview not available</div>
            <div className={styles.docxBody}>
              This file type is available for download, but the in-app preview is not supported yet.
            </div>
            <div className={styles.metaGrid}>
              <div><strong>File:</strong> {resolvedTitle}</div>
              <div><strong>Type:</strong> {meta?.extension?.toUpperCase() || 'FILE'}</div>
              <div><strong>Size:</strong> {meta?.file_size ? `${Math.round(meta.file_size / 1024)} KB` : 'Unknown'}</div>
            </div>
            <button type="button" className={styles.primaryBtn} onClick={openOriginal}>Open original</button>
          </div>
        )}

        {isPdf && status === 'ready' && documentFile && pdfViewerUrl && (
          <div className={styles.pdfShell}>
            <div className={styles.pdfHeader}>
              <div className={styles.pdfHeaderTitle}>Document canvas</div>
              <div className={styles.pdfHeaderMeta}>PDF.js preview</div>
            </div>

            <div className={styles.evidencePanel}>
              {!evidenceObject.exists ? (
                <div className={styles.evidenceEmpty}>No source match available yet.</div>
              ) : (
                <>
                  <div className={styles.evidenceHeader}>
                    <div className={styles.evidenceLabel}>
                      Relevant section found on page {evidenceObject.pageNumber} · {evidenceObject.sourceLabel}
                      {evidenceObject.qualityLabel && evidenceObject.qualityLabel !== 'Page match' ? ` · ${evidenceObject.qualityLabel}` : ''}
                    </div>
                  </div>

                  {(
                    <div className={styles.evidenceContent}>
                      <div className={styles.evidenceLeft}>
                        <div className={styles.evidenceText}>
                          "{evidenceObject.displayText}"
                        </div>
                        <div className={styles.evidenceMeta}>
                          <div>Source: {evidenceObject.sourceLabel}</div>
                          <div>Match strength: {evidenceMatchStrength}</div>
                          {evidenceObject.found === false && evidenceObject.warning ? (
                            <div className={styles.evidenceNote}>{evidenceObject.warning}</div>
                          ) : null}
                          {evidenceObject.extractionMethod === 'ocr' ? (
                            <div className={styles.evidenceNote}>{evidenceObject.sourceNote}</div>
                          ) : null}
                          {evidenceObject.extractionMethod === 'ocr_failed' ? (
                            <div className={styles.evidenceNote}>{evidenceObject.sourceNote}</div>
                          ) : null}
                          {DEV && evidenceObject.score > 0 ? (
                            <div>Score: {evidenceObject.score.toFixed(2)}</div>
                          ) : null}
                        </div>
                      </div>
                      <div className={styles.evidenceRight}>
                        <button
                          type="button"
                          className={styles.evidenceBtn}
                          onClick={jumpToEvidencePage}
                        >
                          Go to block
                        </button>
                        <button
                          type="button"
                          className={styles.evidenceBtn}
                          onClick={() => {
                            const query = selectBestPdfSearchCandidate(pdfSearchCandidates)
                            const sent = sendPdfJsFindCommandWithCandidates(query, pdfSearchCandidates, {
                              targetPage: evidenceObject.pageNumber,
                            })
                            const isOcrDerived = evidenceObject.extractionMethod === 'ocr' || evidenceObject.hasTextLayer === false
                            if (sent) {
                              setCopyStatus(isOcrDerived ? evidenceSearchNote : 'Search sent to PDF viewer.')
                            } else {
                              navigator.clipboard.writeText(query)
                              setCopyStatus(isOcrDerived ? evidenceSearchNote : 'Copied. Use PDF toolbar search.')
                            }
                            if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current)
                            copyTimerRef.current = window.setTimeout(() => setCopyStatus(''), 3000)
                          }}
                        >
                          Find in document
                        </button>
                        <button
                          type="button"
                          className={styles.evidenceBtn}
                          onClick={() => {
                            navigator.clipboard.writeText(evidenceObject.displayText || evidenceObject.phrase)
                            setCopyStatus('Copied text')
                            if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current)
                            copyTimerRef.current = window.setTimeout(() => setCopyStatus(''), 1500)
                          }}
                        >
                          Copy text
                        </button>
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>

            <div className={styles.pdfFrameWrap}>
              <iframe
                ref={pdfFrameRef}
                src={pdfViewerUrl}
                title={resolvedTitle}
                className={styles.pdfFrame}
                onLoad={() => setPdfFrameReady(true)}
              />
            </div>
          </div>
        )}

        {isStructuredPreview && status === 'ready' && (
          <div className={styles.docShell}>
            <div className={styles.pdfHeader}>
              <div className={styles.pdfHeaderTitle}>Document canvas</div>
              <div className={styles.pdfHeaderMeta}>
                {isSpreadsheet
                  ? 'Workbook preview and row-level evidence.'
                  : 'Readable structured preview and highlightable source text.'}
              </div>
            </div>
            {isSpreadsheet ? (
              <>
                <div className={styles.evidencePanel}>
                  {!evidenceObject.exists ? (
                    <div className={styles.evidenceEmpty}>No source match available yet.</div>
                  ) : (
                    <div className={styles.evidenceContent}>
                      <div className={styles.evidenceLeft}>
                        <div className={styles.evidenceText}>
                          "{evidenceObject.displayText}"
                        </div>
                        <div className={styles.evidenceMeta}>
                          <div>Workbook: {resolvedTitle}</div>
                          <div>Sheet: {spreadsheetSheet?.sheetName || 'Unknown sheet'}</div>
                          <div>Range: {highlightMatches?.[0]?.rangeRef || highlightMatches?.[0]?.range_ref || spreadsheetSheet?.usedRange || 'Unknown range'}</div>
                          <div>Match type: {highlightMatches?.[0]?.matchType || 'chunk_proximity'}</div>
                        </div>
                      </div>
                      <div className={styles.evidenceRight}>
                        <button type="button" className={styles.evidenceBtn} onClick={jumpToEvidenceBlock}>
                          Go to row
                        </button>
                        <button
                          type="button"
                          className={styles.evidenceBtn}
                          onClick={() => {
                            navigator.clipboard.writeText(evidenceObject.displayText || evidenceObject.phrase)
                            setCopyStatus('Copied text')
                            if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current)
                            copyTimerRef.current = window.setTimeout(() => setCopyStatus(''), 1500)
                          }}
                        >
                          Copy text
                        </button>
                      </div>
                    </div>
                  )}
                </div>
                <div className={styles.docCanvas}>
                  {spreadsheetSheet ? renderSpreadsheetPreview(spreadsheetSheet, highlightMatches, styles) : (
                    <div className={styles.state}>
                      <div className={styles.errorTitle}>No spreadsheet preview available</div>
                      <div className={styles.errorBody}>The workbook opened correctly, but no readable sheet preview was generated.</div>
                      <button type="button" className={styles.primaryBtn} onClick={openOriginal}>Open original</button>
                    </div>
                  )}
                </div>
              </>
            ) : (
              <>
                <div className={styles.evidencePanel}>
                  {!evidenceObject.exists ? (
                    <div className={styles.evidenceEmpty}>No source match available yet.</div>
                  ) : (
                    <>
                      <div className={styles.evidenceHeader}>
                        <div className={styles.evidenceLabel}>
                          Relevant block found:
                          {primaryBlockIndexes?.[0] !== undefined ? ` Block ${primaryBlockIndexes[0]}` : ''}
                          {' · Document text'}
                          {evidenceObject.qualityLabel ? ` · ${evidenceObject.qualityLabel}` : ''}
                        </div>
                      </div>

                      <div className={styles.evidenceContent}>
                        <div className={styles.evidenceLeft}>
                          <div className={styles.evidenceText}>
                            "{evidenceObject.displayText}"
                          </div>
                          <div className={styles.evidenceMeta}>
                            <div>Source: Document text</div>
                            <div>Match strength: {evidenceMatchStrength}</div>
                            {DEV && evidenceObject.score > 0 ? (
                              <div>Score: {evidenceObject.score.toFixed(2)}</div>
                            ) : null}
                          </div>
                        </div>
                        <div className={styles.evidenceRight}>
                          <button type="button" className={styles.evidenceBtn} onClick={jumpToEvidenceBlock}>
                            Go to block
                          </button>
                          <button
                            type="button"
                            className={styles.evidenceBtn}
                            onClick={() => {
                              navigator.clipboard.writeText(evidenceObject.displayText || evidenceObject.phrase)
                              setCopyStatus('Copied text')
                              if (copyTimerRef.current) window.clearTimeout(copyTimerRef.current)
                              copyTimerRef.current = window.setTimeout(() => setCopyStatus(''), 1500)
                            }}
                          >
                            Copy text
                          </button>
                        </div>
                      </div>
                    </>
                  )}
                </div>
                <div className={styles.docCanvas}>
                  {documentBlocks.length > 0 ? (
                    documentBlocks.map(block => {
                      const blockIndex = Number(block.blockIndex)
                      const candidates = blockCandidates(block.blockIndex)
                      const isPrimary = !manualMode && primaryBlockSet.has(blockIndex)
                      const isSecondary = !manualMode && !isPrimary && secondaryBlockSet.has(blockIndex)
                      const wrapperClass = [
                        styles.docBlock,
                        styles[`docBlock_${String(block.type || 'paragraph')}`] || '',
                        isPrimary ? styles.docBlockPrimary : '',
                        isSecondary ? styles.docBlockSecondary : '',
                        manualMode && candidates.length > 0 ? styles.docBlockActive : '',
                      ].filter(Boolean).join(' ')
                      return (
                        <section
                          key={block.blockIndex}
                          ref={el => registerBlockRef(block.blockIndex, el)}
                          className={wrapperClass}
                          data-block-index={block.blockIndex}
                        >
                          {renderBlockText(block, candidates, styles)}
                        </section>
                      )
                    })
                  ) : (
                    <div className={styles.state}>
                      <div className={styles.errorTitle}>No extractable text found</div>
                      <div className={styles.errorBody}>The document opened correctly, but there was no readable text to preview inline.</div>
                      <button type="button" className={styles.primaryBtn} onClick={openOriginal}>Open original</button>
                    </div>
                  )}
                </div>
              </>
            )}
          </div>
        )}
      </div>

      <div className={styles.footer}>
        <div className={styles.footerMeta}>
          {isPdf && pdfTextLayer.length > 0
            ? `Text layer available for ${pdfTextLayer.length} pages`
            : isSpreadsheet && spreadsheetSheet
              ? `Workbook preview available for ${spreadsheetSheet.sheetName}`
            : isStructuredPreview
              ? `Structured preview available for ${documentBlocks.length} blocks`
              : 'Open original to download the source file'}
        </div>
        <div className={styles.footerMeta}>
          {highlightMatches.length > 0
            ? isPdf
              ? `First match on page ${highlightMatches[0].pageNumber}`
              : isSpreadsheet
                ? `First match on sheet ${highlightMatches[0].sheetName || highlightMatches[0].sheet_name || spreadsheetSheet?.sheetName || 'Unknown'}`
              : `First evidence block ${primaryBlockIndexes[0] ?? highlightMatches[0].blockIndex}`
            : 'No highlight match found yet'}
        </div>
      </div>
    </div>
  )
}
