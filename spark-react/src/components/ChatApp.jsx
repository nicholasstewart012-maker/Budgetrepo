import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import { faArrowRightLong, faGear } from '@fortawesome/free-solid-svg-icons'
import styles from './ChatApp.module.css'
import { getApiBase } from '../api.js'
import { buildThemeVars, getIconOption, loadPersistedSettings } from '../settings.js'

const API = getApiBase()
const SPARK_USER = 'local_user'

function esc(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
}

function isFallbackAnswer(text) {
  return String(text || '').toLowerCase().includes("i don't have enough information")
}

function formatAnswer(text) {
  const escaped = esc(text)
  return escaped
    .replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>')
    .replace(/(?<!href=")(https?:\/\/[^\s<]+[^<.,:;"')\]\s])/g, '<a href="$1" target="_blank" rel="noopener noreferrer">$1</a>')
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/\n{2,}/g, '</p><p>')
    .replace(/\n/g, '<br/>')
}

function openViewer(sourceDetail, answerText, questionText) {
  const sourcePath = sourceDetail?.source_path || sourceDetail?.path || ''
  const sourceName = sourceDetail?.source_title || sourceDetail?.name || ''
  const sourcePage = sourceDetail?.page_number ?? sourceDetail?.pageNumber ?? null
  const pathValue = sourcePath || sourceName
  if (!pathValue) return
  const params = new URLSearchParams({
    path: sourcePath || pathValue,
    sourcePath,
    sourceName,
    snippet: sourceDetail.snippet || '',
    evidenceText: sourceDetail.evidence_text || sourceDetail.evidenceText || sourceDetail.evidence_anchor || sourceDetail.snippet || '',
    chunk: String(sourceDetail.chunk_index ?? 0),
    answer: answerText || '',
    question: questionText || '',
    chunkText: sourceDetail.chunk_text || sourceDetail.chunkText || sourceDetail.snippet || '',
    source: sourceName,
    chunkId: sourceDetail.chunk_id || '',
    extractionMethod: sourceDetail.extraction_method || sourceDetail.extractionMethod || '',
    hasTextLayer: sourceDetail.has_text_layer ?? sourceDetail.hasTextLayer ?? '',
    ocrConfidence: sourceDetail.ocr_confidence ?? sourceDetail.ocrConfidence ?? '',
  })
  if (sourcePage !== null && sourcePage !== undefined && sourcePage !== '') {
    params.set('pageNumber', String(sourcePage))
  }
  window.open(`/ui/?${params.toString()}`, '_blank')
}

function getWelcomeCards(splashCards, suggestions) {
  if (Array.isArray(splashCards) && splashCards.length > 0) {
    return splashCards
  }

  return suggestions.map((question, index) => ({
    id: `${index}-${question}`,
    category: index % 3 === 0 ? 'Support' : index % 3 === 1 ? 'Policy' : 'Search',
    question,
    icon: index % 2 === 0 ? 'sparkles' : 'search',
  }))
}

function getConversationTitle(question) {
  return String(question || '').trim().substring(0, 52) || 'New conversation'
}

function formatConversationDate(messages) {
  const timestamps = messages
    .map(msg => msg.timestamp)
    .filter(Boolean)
    .map(value => new Date(value))
    .filter(date => !Number.isNaN(date.getTime()))
  if (timestamps.length === 0) return ''
  const latest = timestamps[timestamps.length - 1]
  return latest.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

function LogoImage({ alt, className, fallbackClassName }) {
  const [failed, setFailed] = useState(false)

  if (failed) {
    return <span className={fallbackClassName}>S</span>
  }

  return (
    <img
      src="/ui/logo.png"
      alt={alt}
      className={className}
      onError={() => setFailed(true)}
    />
  )
}

function MessageActions({ msg, question, onFeedback }) {
  if (!msg.timestamp) return null
  return (
    <div className={styles.messageActions}>
      <button
        type="button"
        className={`${styles.actionBtn} ${msg.feedback === 'up' ? styles.actionBtnActive : ''}`}
        onClick={() => onFeedback(msg.timestamp, 'up')}
        disabled={!!msg.feedback}
        aria-label="Mark helpful"
      >
        Up
      </button>
      <button
        type="button"
        className={`${styles.actionBtn} ${msg.feedback === 'down' ? styles.actionBtnActive : ''}`}
        onClick={() => onFeedback(msg.timestamp, 'down')}
        disabled={!!msg.feedback}
        aria-label="Mark not helpful"
      >
        Down
      </button>
      <button
        type="button"
        className={styles.actionBtn}
        onClick={() => navigator.clipboard.writeText(msg.text || '')}
        aria-label="Copy answer"
      >
        Copy
      </button>
      {question && (
        <button
          type="button"
          className={styles.actionBtn}
          onClick={() => openViewer(msg.source_detail?.[0] || null, msg.text, question)}
          disabled={!msg.source_detail?.length}
        >
          Open source
        </button>
      )}
    </div>
  )
}

function AssistantMessage({ msg, question, onFeedback, assistantName }) {
  const sourceList = msg.sources || []
  const sourceDetail = msg.source_detail || []
  const fallback = isFallbackAnswer(msg.text)

  return (
    <div className={styles.assistantWrap}>
      <div className={styles.assistantBubble}>
        <div className={styles.assistantHeader}>
          <div className={styles.assistantBadge}>{assistantName || 'Spark'}</div>
          {fallback && <div className={styles.fallbackBadge}>Answer not found</div>}
        </div>
        <div className={styles.answerBody}>
          <div className={styles.answerText} dangerouslySetInnerHTML={{ __html: `<p>${formatAnswer(msg.text)}</p>` }} />
        </div>
        {sourceList.length > 0 && (
          <div className={styles.sourcesBlock}>
            <div className={styles.sourcesTitle}>Sources ({sourceList.length})</div>
            <div className={styles.sourcesRail}>
              {sourceList.map((srcName, index) => {
                const detail = sourceDetail.find(d => d.name === srcName) || { name: srcName }
                return (
                  <button
                    key={`${srcName}-${index}`}
                    type="button"
                    className={styles.sourceCard}
                    onClick={() => openViewer(detail, msg.text, question)}
                    title="Open source document"
                  >
                    <div className={styles.sourceCardTitle}>{detail.source_title || detail.name}</div>
                    <div className={styles.sourceCardSnippet}>
                      {detail.evidence_context || detail.snippet || 'Open document'}
                    </div>
                  </button>
                )
              })}
            </div>
          </div>
        )}
        <MessageActions msg={msg} question={question} onFeedback={onFeedback} />
      </div>
    </div>
  )
}

function UserMessage({ msg }) {
  return (
    <div className={styles.userWrap}>
      <div className={styles.userBubble}>{msg.text}</div>
    </div>
  )
}

function LandingComposer({ input, onChange, onSend, loading }) {
  const handleKey = e => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      onSend()
    }
  }

  return (
    <div className={styles.landingComposer}>
      <textarea
        className={styles.landingInput}
        placeholder="Ask an open-ended question..."
        value={input}
        onChange={e => onChange(e.target.value)}
        onKeyDown={handleKey}
      />
      <div className={styles.landingComposerFooter}>
        <div className={styles.landingHint}>Ask directly instead of choosing a preset.</div>
        <button type="button" className={styles.sendBtn} onClick={onSend} disabled={loading || !String(input || '').trim()}>
          Ask
        </button>
      </div>
    </div>
  )
}

function HistoryItem({ conv, active, onClick }) {
  const dateLabel = formatConversationDate(conv.messages)
  const countLabel = `${conv.messages.length} msg${conv.messages.length === 1 ? '' : 's'}`
  return (
    <button type="button" className={`${styles.historyItem} ${active ? styles.historyItemActive : ''}`} onClick={onClick}>
      <div className={styles.historyTitle}>{conv.title}</div>
      <div className={styles.historyMeta}>
        <span>{countLabel}</span>
        {dateLabel && <span>{dateLabel}</span>}
      </div>
    </button>
  )
}

function Welcome({ splashCards, suggestions, onSuggest, settings, input, onInputChange, onSend, loading }) {
  const cards = getWelcomeCards(splashCards, suggestions)

  return (
    <div className={styles.welcome}>
      <div className={styles.welcomeMark}>
        <LogoImage
          alt="Spark"
          className={styles.welcomeMarkImage}
          fallbackClassName={styles.welcomeMarkFallback}
        />
      </div>
      <div className={styles.welcomeTitle}>What can I help you search today?</div>
      <div className={styles.welcomeSubtitle}>
        {settings.disclaimer || 'Ask a question, upload a file, or start from a suggested prompt.'}
      </div>
      <div className={styles.quickGrid}>
        {cards.map((card, i) => {
          const iconOption = getIconOption(card.icon)
          return (
            <button key={card.id || `${card.question}-${i}`} type="button" className={styles.quickCard} onClick={() => onSuggest(card.question)}>
              <span className={styles.quickIcon}>
                <FontAwesomeIcon icon={iconOption.icon || faArrowRightLong} />
              </span>
              <span className={styles.quickCardBody}>
                <span className={styles.quickCardCategory}>{card.category || 'Prompt'}</span>
                <span className={styles.quickCardQuestion}>{card.question}</span>
              </span>
            </button>
          )
        })}
      </div>
      <LandingComposer input={input} onChange={onInputChange} onSend={onSend} loading={loading} />
    </div>
  )
}

function deriveSuggestions(convs) {
  const seen = new Set()
  const items = []
  for (const conv of convs) {
    for (const msg of conv.messages) {
      if (msg.role !== 'user') continue
      const text = String(msg.text || '').trim()
      if (!text || seen.has(text.toLowerCase())) continue
      seen.add(text.toLowerCase())
      items.push(text)
    }
  }
  return items.slice(0, 6)
}

export default function ChatApp() {
  const [settings, setSettings] = useState(() => loadPersistedSettings())
  const [convs, setConvs] = useState([])
  const [current, setCurrent] = useState(null)
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [toast, setToast] = useState(null)
  const messagesRef = useRef(null)
  const textareaRef = useRef(null)

  useEffect(() => {
    const syncSettings = () => setSettings(loadPersistedSettings())
    syncSettings()
    window.addEventListener('storage', syncSettings)
    window.addEventListener('spark-settings-updated', syncSettings)
    return () => {
      window.removeEventListener('storage', syncSettings)
      window.removeEventListener('spark-settings-updated', syncSettings)
    }
  }, [])

  useEffect(() => {
    fetch(`${API}/history?user=${SPARK_USER}&limit=100`)
      .then(async r => {
        const raw = await r.text()
        let payload = {}
        try {
          payload = raw ? JSON.parse(raw) : {}
        } catch (parseErr) {
          console.error('[Spark Chat] Failed to parse /history response', {
            status: r.status,
            body: raw,
            error: parseErr,
          })
          return { history: [] }
        }
        if (!r.ok) {
          console.error('[Spark Chat] /history failed', { status: r.status, body: raw })
          return { history: [] }
        }
        return payload
      })
      .then(d => {
        if (!d.history?.length) return
        const loaded = d.history.map(h => ({
          id: new Date(h.timestamp).getTime(),
          title: getConversationTitle(h.question),
          messages: [
            { role: 'user', text: h.question },
            {
              role: 'spark',
              text: h.answer,
              sources: h.sources || [],
              source_detail: h.source_detail || [],
              timestamp: h.timestamp,
              feedback: h.feedback,
            },
          ],
        }))
        const sorted = loaded.reverse()
        setConvs(sorted)
        setCurrent(null)
      })
      .catch(e => console.error('Could not load history:', e))
  }, [])

  useEffect(() => {
    const resetToLanding = () => {
      const params = new URLSearchParams(window.location.search)
      const viewerPath = params.get('path') || params.get('sourcePath')
      const view = params.get('view')
      if (viewerPath || view === 'viewer') return
      setCurrent(null)
      setInput('')
      setLoading(false)
    }

    const handlePageShow = () => resetToLanding()

    window.addEventListener('popstate', resetToLanding)
    window.addEventListener('pageshow', handlePageShow)
    return () => {
      window.removeEventListener('popstate', resetToLanding)
      window.removeEventListener('pageshow', handlePageShow)
    }
  }, [])

  useEffect(() => {
    if (messagesRef.current) messagesRef.current.scrollTop = messagesRef.current.scrollHeight
  }, [current?.messages, loading])

  useEffect(() => {
    if (!textareaRef.current) return
    textareaRef.current.style.height = 'auto'
    textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 140)}px`
  }, [input])

  const suggestions = useMemo(() => deriveSuggestions(convs), [convs])
  const splashCards = useMemo(() => settings.splashCards || [], [settings])
  const themeVars = useMemo(() => buildThemeVars(settings), [settings])

  const showToast = useCallback(msg => {
    setToast(msg)
    window.clearTimeout(showToast._timer)
    showToast._timer = window.setTimeout(() => setToast(null), 2500)
  }, [])

  const send = useCallback(async (question) => {
    const q = (question || input).trim()
    if (!q || loading) return
    setInput('')

    let conv = current
    if (!conv) {
      conv = { id: Date.now(), title: getConversationTitle(q), messages: [] }
      setConvs(prev => [conv, ...prev])
      setCurrent(conv)
    }

    const userMsg = { role: 'user', text: q }
    const updatedMsgs = [...(conv.messages || []), userMsg]
    const updatedConv = { ...conv, title: getConversationTitle(q), messages: updatedMsgs }
    setCurrent(updatedConv)
    setConvs(prev => prev.map(c => (c.id === updatedConv.id ? updatedConv : c)))
    setLoading(true)

    try {
      const r = await fetch(`${API}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q, user: SPARK_USER }),
      })
      const raw = await r.text()
      let d = {}
      try {
        d = raw ? JSON.parse(raw) : {}
      } catch (parseErr) {
        console.error('[Spark Chat] Failed to parse /query response', {
          status: r.status,
          body: raw,
          error: parseErr,
        })
        throw new Error(raw || `Spark API returned HTTP ${r.status} with an invalid JSON body.`)
      }
      if (!r.ok) {
        console.error('[Spark Chat] /query failed', { status: r.status, body: raw })
        throw new Error(d?.detail || d?.error || raw || `Spark API returned HTTP ${r.status}.`)
      }
      const chunks = Array.isArray(d.source_detail)
        ? d.source_detail
        : Array.isArray(d.chunks)
          ? d.chunks
          : []
      const sourceDetail = chunks.map(chunk => {
        return {
          ...chunk,
          source_title: chunk.source_title || chunk.source_name || chunk.document_id || chunk.name || chunk.source || '',
          name: chunk.name || chunk.source_title || chunk.source_name || chunk.document_id || chunk.source || '',
          source_path: chunk.source_path || chunk.path || chunk.document_id || chunk.source || '',
          snippet: chunk.evidence_context || '',
          evidence_text: chunk.evidence_anchor || chunk.evidence_context || '',
          evidence_anchor: chunk.evidence_anchor || '',
          evidence_context: chunk.evidence_context || '',
          chunk_text: chunk.text || '',
          chunk_id: chunk.chunk_id || '',
        }
      })
      const sparkMsg = {
        role: 'spark',
        text: d.answer || '',
        sources: Array.isArray(d.sources) ? d.sources : chunks.map(chunk => chunk.source_title || chunk.source_name || chunk.document_id || chunk.source).filter(Boolean),
        source_detail: sourceDetail,
        timestamp: d.timestamp,
        feedback: null,
      }
      const finalConv = { ...updatedConv, messages: [...updatedMsgs, sparkMsg] }
      setCurrent(finalConv)
      setConvs(prev => prev.map(c => (c.id === finalConv.id ? finalConv : c)))
    } catch (e) {
      const errMsg = {
        role: 'spark',
        text: e?.message || 'Could not reach the Spark API. Make sure the server is running on port 8000.',
        sources: [],
        source_detail: [],
      }
      const finalConv = { ...updatedConv, messages: [...updatedMsgs, errMsg] }
      setCurrent(finalConv)
      setConvs(prev => prev.map(c => (c.id === finalConv.id ? finalConv : c)))
      console.error(e)
    } finally {
      setLoading(false)
    }
  }, [current, input, loading])

  const handleFeedback = useCallback((timestamp, value) => {
    const updateMsgs = msgs => msgs.map(m => (m.timestamp !== timestamp || m.feedback ? m : { ...m, feedback: value }))

    let wasAlreadyVoted = false
    setConvs(prev => prev.map(c => {
      const existing = c.messages.find(m => m.timestamp === timestamp)
      if (existing?.feedback) wasAlreadyVoted = true
      return { ...c, messages: updateMsgs(c.messages) }
    }))
    setCurrent(prev => (prev ? { ...prev, messages: updateMsgs(prev.messages) } : prev))

    if (wasAlreadyVoted) return

    fetch(`${API}/feedback`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ timestamp, feedback: value, user: SPARK_USER }),
    })
      .then(() => showToast('Feedback saved'))
      .catch(e => console.error('Feedback failed:', e))
  }, [showToast])

  const newConv = () => {
    setCurrent(null)
    setInput('')
  }

  const handleKey = e => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      send()
    }
  }

  const showingWelcome = !current || current.messages.length === 0
  const currentMessages = current?.messages || []
  const hasMessages = currentMessages.length > 0

  return (
    <div className={styles.appShell} style={themeVars}>
      <header className={styles.topBar}>
        <div className={styles.topBrand}>
          <div className={styles.topBrandMark}>
            <LogoImage
              alt="Spark"
              className={styles.topBrandImage}
              fallbackClassName={styles.topBrandFallback}
            />
          </div>
        </div>
        <div className={styles.topActions}>
          <div className={styles.topOrg}>Renasant</div>
          <a href="?view=admin" className={styles.settingsBtn}>
            <FontAwesomeIcon icon={faGear} />
            <span>Settings</span>
          </a>
        </div>
      </header>

      <div className={styles.pageBody}>
        <aside className={styles.sidebar}>
          <button type="button" className={styles.newChatBtn} onClick={newConv}>
            <span className={styles.newChatIcon}>+</span>
            New chat
          </button>

          <div className={styles.sidebarSection}>
            <div className={styles.sidebarLabel}>History</div>
            <div className={styles.historyList}>
              {convs.length === 0 ? (
                <div className={styles.historyEmpty}>No conversations yet.</div>
              ) : (
                convs.map(c => (
                  <HistoryItem key={c.id} conv={c} active={c.id === current?.id} onClick={() => setCurrent(c)} />
                ))
              )}
            </div>
          </div>

          <div className={styles.sidebarFooter}>
            <a href="?view=admin" className={styles.sidebarLink}>Admin</a>
          </div>
        </aside>

        <main className={styles.mainPanel}>
          <div className={styles.messageRegion} ref={messagesRef}>
            {showingWelcome ? (
              <Welcome
                splashCards={splashCards}
                suggestions={suggestions.length > 0 ? suggestions : []}
                onSuggest={s => send(s)}
                settings={settings}
                input={input}
                onInputChange={setInput}
                onSend={() => send()}
                loading={loading}
              />
            ) : (
              <>
                <div className={styles.thread}>
                  {currentMessages.map((m, i) => (
                    <div key={`${m.role}-${m.timestamp || i}-${i}`} className={styles.threadMessage}>
                      {m.role === 'user' ? (
                        <UserMessage msg={m} />
                      ) : (
                        <AssistantMessage
                          msg={m}
                          question={currentMessages.slice(0, i).reverse().find(x => x.role === 'user')?.text || ''}
                          onFeedback={handleFeedback}
                          assistantName={settings.assistantName}
                        />
                      )}
                    </div>
                  ))}
                  {loading && (
                    <div className={styles.typingRow}>
                      <div className={styles.typingBubble}>
                        <span />
                        <span />
                        <span />
                      </div>
                    </div>
                  )}
                </div>
                {hasMessages && (
                  <div className={styles.stickyComposerShell}>
                    <div className={styles.composer}>
                      <textarea
                        ref={textareaRef}
                        className={styles.input}
                        placeholder="Search for files, info, or anything really..."
                        value={input}
                        onChange={e => setInput(e.target.value)}
                        onKeyDown={handleKey}
                      />
                      <div className={styles.composerFooter}>
                        <div className={styles.composerControls}>
                          <button type="button" className={styles.composerBtn} aria-label="Add attachment">+</button>
                          <button type="button" className={styles.composerBtn} aria-label="Attach document">Doc</button>
                          <button type="button" className={styles.composerBtn} aria-label="Filter">Filter</button>
                        </div>
                        <button type="button" className={styles.sendBtn} onClick={() => send()} disabled={loading || !String(input || '').trim()}>
                          Send
                        </button>
                      </div>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </main>
      </div>

      {toast && <div className={styles.toast}>{toast}</div>}
    </div>
  )
}
