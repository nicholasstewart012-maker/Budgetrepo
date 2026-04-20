/**
 * Spark PDF.js Viewer Bridge
 *
 * Runs inside public/pdfjs/web/viewer.html. Receives search requests from the
 * React host and dispatches PDF.js built-in find operations.
 *
 * v5 changes (2026-04-19):
 * - requestId support: ViewerApp sends a requestId on every find request; the
 *   bridge echoes it back on the result. This lets the host ignore stale
 *   responses when citations are clicked in rapid succession.
 * - Trimmed heuristics: the ingest pipeline no longer concatenates heading
 *   text into chunk bodies, so the embedded-heading-stripping machinery from
 *   v4 has been removed. Query generation is now simpler and faster.
 * - Lower evaluation ceilings: MAX_QUERY_ATTEMPTS 120->24, evaluation limit
 *   36->10. Worst-case highlight latency drops from ~7s to ~1.5s.
 * - Kept: best-match selection (PDF.js only highlights the last dispatched
 *   query, so we still evaluate multiple candidates and re-dispatch the
 *   winner), apostrophe/dash normalization, acronym allowlist.
 */
(function sparkPdfBridge() {
  const VERSION = 'spark-bridge-20260419-v5'
  const MAX_QUERY_ATTEMPTS = 24
  const FIND_SETTLE_MS = 60
  const FIND_POLL_COUNT = 10
  const FIND_POLL_INTERVAL_MS = 70
  const APP_WAIT_TIMEOUT_MS = 12000
  const APP_WAIT_INTERVAL_MS = 100
  const SUCCESS_EVALUATION_LIMIT = 10
  const SUCCESS_EARLY_ACCEPT_SCORE = 140

  window.__sparkPdfBridgeVersion = VERSION
  console.debug('[Spark PDF Bridge] loaded', { version: VERSION })

  function sleep(ms) {
    return new Promise(resolve => window.setTimeout(resolve, ms))
  }

  function normalizeSpaces(value) {
    return String(value || '')
      .replace(/\r\n|\r|\n/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
  }

  function normalizeQueryText(value) {
    return normalizeSpaces(value)
      .replace(/[•·●▪◦]/g, ' ')
      .replace(/â¢/g, ' ')
      .replace(/[“”]/g, '"')
      .replace(/[’]/g, "'")
      .replace(/[–—−]/g, '-')
      .replace(/^['"“”]+|['"“”]+$/g, '')
      .trim()
  }

  function stripTrailingPunctuation(value) {
    return normalizeSpaces(value).replace(/[.;,:]+$/g, '').trim()
  }

  function queryTokens(value) {
    return normalizeQueryText(value).split(/\s+/).filter(Boolean)
  }

  const COMMON_WORDS = new Set([
    'the', 'and', 'for', 'are', 'that', 'with', 'this', 'from', 'have', 'has', 'was', 'were',
    'will', 'shall', 'must', 'may', 'can', 'not', 'all', 'any', 'section',
    'page', 'source', 'text', 'data', 'system', 'systems', 'process', 'procedure', 'procedures',
  ])

  function meaningfulTokenCount(value) {
    return queryTokens(value.toLowerCase())
      .filter(token => token.length >= 4 && !COMMON_WORDS.has(token))
      .length
  }

  function isHeadingOnly(value) {
    const clean = normalizeQueryText(value)
    if (!clean) return true
    const tokens = queryTokens(clean)
    if (/^\d+(?:\.\d+)*\s+/.test(clean) && tokens.length <= 7) return true
    if (/^[A-Z0-9][A-Z0-9\s\-/&]{2,}$/.test(clean) && tokens.length <= 7) return true
    if (/^(purpose|scope|policy|policy statement|procedure|procedures|definitions|applicability|availability)$/i.test(clean)) return true
    return false
  }

  function isUnsafeTinyQuery(value) {
    const clean = normalizeQueryText(value)
    if (!clean) return true

    const tokens = queryTokens(clean)
    if (tokens.length === 0) return true
    if (tokens.length === 1 && tokens[0].length <= 1) return true

    // Let real acronyms like PCI/MFA through. Block most 1-2 char terms because
    // PDF.js can highlight them as substrings inside normal words.
    if (tokens.length === 1 && tokens[0].length <= 2 && !/^[A-Z0-9]{2}$/.test(tokens[0])) {
      return true
    }

    if (clean.length < 24 && tokens.length < 4 && meaningfulTokenCount(clean) < 2 && !/^[A-Z0-9]{2,8}$/.test(clean)) {
      return true
    }

    return false
  }

  function shouldUseEntireWord(value) {
    const clean = normalizeQueryText(value)
    const tokens = queryTokens(clean)
    if (tokens.length === 1) return true
    return clean.length < 22 && tokens.length <= 2
  }

  function queryQualityScore(query, reason = '') {
    const clean = normalizeQueryText(query)
    const tokens = queryTokens(clean)
    const length = clean.length
    const meaningful = meaningfulTokenCount(clean)
    const headingPenalty = isHeadingOnly(clean) ? 80 : 0

    let reasonScore = 0
    if (reason === 'full') reasonScore = 80
    else if (reason === 'sentence') reasonScore = 72
    else if (reason === 'leading-window') reasonScore = 58
    else if (reason === 'sentence-leading-window') reasonScore = 54
    else if (reason === 'window') reasonScore = 42
    else if (reason === 'sentence-window') reasonScore = 38

    const lengthScore = Math.min(45, Math.floor(length / 3))
    const tokenScore = Math.min(35, tokens.length * 3)
    const meaningfulScore = Math.min(30, meaningful * 5)

    return reasonScore + lengthScore + tokenScore + meaningfulScore - headingPenalty
  }

  function addVariant(out, seen, value, reason = '') {
    const clean = stripTrailingPunctuation(normalizeQueryText(value))
    if (!clean || isUnsafeTinyQuery(clean)) return

    const tokenCount = queryTokens(clean).length
    const bodyLike = clean.length >= 24 && tokenCount >= 4 && meaningfulTokenCount(clean) >= 2 && !isHeadingOnly(clean)
    const acronym = /^[A-Z0-9]{2,8}$/.test(clean)

    if (!bodyLike && !acronym) return

    const key = clean.toLowerCase()
    if (seen.has(key)) return
    seen.add(key)
    out.push({
      query: clean,
      reason,
      qualityScore: queryQualityScore(clean, reason),
      length: clean.length,
      tokenCount,
    })
  }

  function apostropheVariants(value) {
    const clean = normalizeQueryText(value)
    const variants = new Set([
      clean,
      clean.replace(/'/g, '’'),
      clean.replace(/[’']/g, ''),
      clean.replace(/[’']/g, ' '),
      clean.replace(/\b([A-Za-z]+)[’']s\b/g, '$1'),
    ])
    return [...variants].filter(Boolean)
  }

  function sentenceSegments(value) {
    const clean = normalizeQueryText(value)
    if (!clean) return []

    return clean
      .split(/(?<=[.!?])\s+|[;•]/)
      .map(part => normalizeQueryText(part))
      .filter(Boolean)
  }

  function slidingWindows(value, sizes = [16, 12, 10, 8, 6]) {
    const words = queryTokens(value)
    const windows = []

    for (const size of sizes) {
      if (words.length < size) continue
      for (let index = 0; index <= words.length - size; index += 1) {
        const phrase = words.slice(index, index + size).join(' ')
        if (phrase.length >= 24 && meaningfulTokenCount(phrase) >= 2 && !isHeadingOnly(phrase)) {
          windows.push(phrase)
        }
      }
    }

    return windows
  }

  function leadingWindows(value, sizes = [24, 18, 14, 10, 8]) {
    const words = queryTokens(value)
    const windows = []

    for (const size of sizes) {
      if (words.length < size) continue
      const phrase = words.slice(0, size).join(' ')
      if (phrase.length >= 24 && meaningfulTokenCount(phrase) >= 2 && !isHeadingOnly(phrase)) {
        windows.push(phrase)
      }
    }

    return windows
  }

  function buildQueryAttempts(query, candidates) {
    const attempts = []
    const seen = new Set()
    const rawValues = [query, ...(Array.isArray(candidates) ? candidates : [])]
      .map(value => normalizeQueryText(value))
      .filter(Boolean)

    for (const raw of rawValues) {
      for (const variant of apostropheVariants(raw)) {
        // The citation text from Spark is now heading-free (fixed in ingest.py
        // _chunk_semantic_units), so we can feed it directly to PDF.js find()
        // without heading-stripping gymnastics.
        addVariant(attempts, seen, variant, 'full')

        for (const segment of sentenceSegments(variant)) {
          addVariant(attempts, seen, segment, 'sentence')
          for (const windowText of leadingWindows(segment)) {
            addVariant(attempts, seen, windowText, 'sentence-leading-window')
          }
          for (const windowText of slidingWindows(segment)) {
            addVariant(attempts, seen, windowText, 'sentence-window')
          }
        }

        for (const windowText of leadingWindows(variant)) {
          addVariant(attempts, seen, windowText, 'leading-window')
        }
        for (const windowText of slidingWindows(variant)) {
          addVariant(attempts, seen, windowText, 'window')
        }
      }
    }

    return attempts.slice(0, MAX_QUERY_ATTEMPTS)
  }

  function totalFindMatches(app) {
    const pages = app?.findController?._pageMatches
    if (!Array.isArray(pages)) return 0
    return pages.reduce((sum, pageMatches) => sum + (Array.isArray(pageMatches) ? pageMatches.length : 0), 0)
  }

  async function waitForPdfApp() {
    const started = Date.now()

    while (Date.now() - started < APP_WAIT_TIMEOUT_MS) {
      const app = window.PDFViewerApplication
      if (app?.eventBus && app?.pdfViewer && app?.findController) {
        try {
          if (app.initializedPromise) await app.initializedPromise
          if (app.pdfLoadingTask?.promise) await app.pdfLoadingTask.promise.catch(() => null)
        } catch (err) {
          console.debug('[Spark PDF Bridge] PDF app wait observed init error', err)
        }
        return app
      }

      await sleep(APP_WAIT_INTERVAL_MS)
    }

    return null
  }

  async function ensurePageFitDefault(app) {
    if (!app?.pdfViewer) return false

    try {
      app.pdfViewer.currentScaleValue = 'page-actual'
    } catch (err) {
      console.debug('[Spark PDF Bridge] Unable to set page-actual zoom', err)
    }

    return true
  }

  function setTargetPage(app, targetPage) {
    const page = Number(targetPage || 0)
    if (!Number.isFinite(page) || page <= 0 || !app?.pdfViewer) return

    try {
      app.pdfViewer.currentPageNumber = Math.max(1, Math.floor(page))
    } catch (err) {
      console.debug('[Spark PDF Bridge] Unable to set target page', { targetPage, err })
    }
  }

  async function waitForFindMatches(app) {
    await sleep(FIND_SETTLE_MS)

    for (let index = 0; index < FIND_POLL_COUNT; index += 1) {
      const total = totalFindMatches(app)
      if (total > 0) return total
      await sleep(FIND_POLL_INTERVAL_MS)
    }

    return totalFindMatches(app)
  }

  async function dispatchFind(app, searchQuery) {
    const entireWord = shouldUseEntireWord(searchQuery)

    app.eventBus.dispatch('find', {
      source: window,
      type: '',
      query: searchQuery,
      phraseSearch: true,
      caseSensitive: false,
      entireWord,
      highlightAll: true,
      findPrevious: false,
    })

    const total = await waitForFindMatches(app)
    console.debug('[Spark PDF Bridge] find attempt', { searchQuery, total, entireWord })
    return { total, entireWord }
  }

  function scoreSuccessfulAttempt(attempt, result) {
    if (!attempt || !result || result.total <= 0) return -Infinity

    const query = normalizeQueryText(attempt.query)
    const tokens = queryTokens(query)
    const totalMatches = Number(result.total || 0)
    // Duplicate penalty: capped so short intentional acronym queries (e.g. PCI
    // appearing 20 times in a policy doc) aren't unfairly outranked by longer
    // phrases. Scales with token count so long phrases with many duplicates
    // still get penalized harder than short ones.
    const duplicatePenalty = Math.min(30, Math.max(0, totalMatches - 1) * Math.max(1, tokens.length / 2))

    return (
      Number(attempt.qualityScore || queryQualityScore(query, attempt.reason))
      + Math.min(70, query.length / 2)
      + Math.min(45, tokens.length * 4)
      - duplicatePenalty
    )
  }

  window.addEventListener('message', async event => {
    if (event.origin !== window.location.origin) return

    const data = event.data || {}
    if (data.type !== 'spark-pdf-find') return

    const requestId = data.requestId || null
    const query = normalizeQueryText(data.query || '')
    const candidates = Array.isArray(data.candidates)
      ? data.candidates.map(value => normalizeQueryText(value)).filter(Boolean)
      : []
    const orderedQueries = buildQueryAttempts(query, candidates)

    function postResult(payload) {
      window.parent.postMessage(
        { ...payload, requestId, version: VERSION, type: 'spark-pdf-find-result' },
        window.location.origin,
      )
    }

    if (orderedQueries.length === 0) {
      postResult({
        found: false,
        query: '',
        totalMatches: 0,
        triedCount: 0,
        reason: 'no_safe_queries',
      })
      return
    }

    const app = await waitForPdfApp()
    if (!app?.eventBus) {
      postResult({
        found: false,
        query: orderedQueries[0]?.query || '',
        totalMatches: 0,
        triedCount: orderedQueries.length,
        reason: 'pdf_app_not_ready',
      })
      return
    }

    await ensurePageFitDefault(app)
    setTargetPage(app, data.targetPage)

    let bestMatch = null
    let attemptedCount = 0
    const orderedForEvaluation = [...orderedQueries]
      .sort((left, right) => Number(right.qualityScore || 0) - Number(left.qualityScore || 0))

    for (const attempt of orderedForEvaluation.slice(0, SUCCESS_EVALUATION_LIMIT)) {
      attemptedCount += 1
      const result = await dispatchFind(app, attempt.query)
      if (result.total <= 0) continue

      const successScore = scoreSuccessfulAttempt(attempt, result)
      const candidate = { attempt, result, successScore }
      if (!bestMatch || candidate.successScore > bestMatch.successScore) {
        bestMatch = candidate
      }

      // Early termination: if we found a high-quality long-span match, stop
      // looking. Also prune attempts whose max-possible score is already below
      // what we have — no point evaluating them.
      if (successScore >= SUCCESS_EARLY_ACCEPT_SCORE && attempt.query.length >= 72) {
        break
      }
    }

    const selectedQuery = bestMatch?.attempt?.query || orderedQueries[0]?.query || ''
    let foundMatches = bestMatch?.result?.total || 0
    const selectedReason = bestMatch?.attempt?.reason || orderedQueries[0]?.reason || ''

    // Re-dispatch the winning query last so the visible PDF.js highlight is
    // the best span, not whichever partial attempt ran last during evaluation.
    // Skip re-dispatch when the winner was the most recent attempt we ran
    // (saves one find round trip in the common case).
    if (bestMatch?.attempt?.query) {
      const lastEvaluated = orderedForEvaluation[attemptedCount - 1]
      const wasLast = lastEvaluated && lastEvaluated.query === bestMatch.attempt.query
      if (!wasLast) {
        setTargetPage(app, data.targetPage)
        const finalResult = await dispatchFind(app, bestMatch.attempt.query)
        foundMatches = finalResult.total || foundMatches
      }
    }

    postResult({
      found: foundMatches > 0,
      query: selectedQuery,
      totalMatches: foundMatches,
      triedCount: attemptedCount,
      availableCount: orderedQueries.length,
      reason: selectedReason,
      score: bestMatch?.successScore || 0,
    })
  })
})()