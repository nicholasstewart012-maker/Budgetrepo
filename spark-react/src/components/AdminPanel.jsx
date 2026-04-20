import { useCallback, useEffect, useMemo, useState } from 'react'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import styles from './AdminPanel.module.css'
import { getApiBase } from '../api.js'
import {
  ADMIN_SETTINGS_STORAGE_KEY,
  DEFAULT_SETTINGS,
  DEFAULT_SPLASH_CARD_PRESETS,
  SPLASH_ICON_OPTIONS,
  buildThemeVars,
  getIconOption,
  loadPersistedSettings,
} from '../settings.js'

const API = getApiBase()

const TABS = [
  { id: 'search', label: 'Search Logs' },
  { id: 'analytics', label: 'Analytics & Insights' },
  { id: 'knowledge', label: 'Knowledge' },
  { id: 'departments', label: 'Departments' },
  { id: 'settings', label: 'Settings' },
]

const SETTINGS_TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'appearance', label: 'Appearance' },
  { id: 'splash', label: 'Splash Cards' },
  { id: 'licenses', label: 'Licenses' },
]

const FRONTEND_LICENSES = [
  { name: 'React', version: '18.3.1', license: 'MIT' },
  { name: 'React DOM', version: '18.3.1', license: 'MIT' },
  { name: 'Vite', version: '5.4.21', license: 'MIT' },
  { name: '@vitejs/plugin-react', version: '4.7.0', license: 'MIT' },
  { name: 'Agent Browser', version: '0.26.0', license: 'MIT' },
  { name: 'Font Awesome SVG Core', version: '7.2.0', license: 'MIT' },
  { name: 'Font Awesome React', version: '3.3.0', license: 'MIT' },
  { name: 'Font Awesome Solid Icons', version: '7.2.0', license: 'CC-BY-4.0 + MIT' },
  { name: 'pdfjs-dist', version: '4.4.168', license: 'Apache-2.0' },
]

const BACKEND_LICENSES = [
  { name: 'FastAPI', version: '>=0.111.0', license: 'MIT' },
  { name: 'Uvicorn', version: '0.29.0', license: 'BSD-3-Clause' },
  { name: 'python-dotenv', version: '1.0.1', license: 'BSD-3-Clause' },
  { name: 'sentence-transformers', version: '>=3.0.1', license: 'Apache-2.0' },
  { name: 'pypdf', version: '4.2.0', license: 'BSD-3-Clause' },
  { name: 'python-docx', version: '1.1.2', license: 'MIT' },
  { name: 'NumPy', version: '1.26.4', license: 'BSD-3-Clause' },
  { name: 'ChromaDB', version: '>=0.5.0', license: 'Apache-2.0' },
  { name: 'rank-bm25', version: '0.2.2', license: 'Apache-2.0' },
  { name: 'aiohttp', version: '>=3.9.5', license: 'Apache-2.0 + MIT' },
  { name: 'Pydantic', version: '>=2.7.0', license: 'MIT' },
  { name: 'Pillow', version: '>=10.0.0', license: 'HPND' },
  { name: 'PyMuPDF', version: '>=1.24.0', license: 'AGPL-3.0 or commercial' },
  { name: 'pytesseract', version: '>=0.3.10', license: 'Apache-2.0' },
]

function makeId() {
  return `card_${Math.random().toString(36).slice(2, 10)}_${Date.now().toString(36)}`
}

function isFallbackAnswer(answer) {
  return String(answer || '').toLowerCase().includes("i don't have enough information")
}

function formatPct(value) {
  if (value == null || Number.isNaN(Number(value))) return 'N/A'
  return `${Math.round(Number(value) * 100)}%`
}

function formatList(items, emptyText = 'None') {
  if (!items || items.length === 0) return emptyText
  const shown = items.slice(0, 5)
  const remaining = items.length - shown.length
  const joined = shown.join(', ')
  return remaining > 0 ? `${joined}, +${remaining} more` : joined
}

function normalizeKnowledgeValue(value) {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, ' ')
}

function getDocumentDisplayName(doc) {
  return doc?.title || doc?.file_name || doc?.file || 'Untitled'
}

function getDocumentReferenceStrings(doc) {
  const values = [
    doc?.document_id,
    doc?.source_path,
    doc?.title,
    doc?.file_name,
    doc?.file,
    doc?.source,
  ]
  const normalized = new Set()
  for (const value of values) {
    const text = normalizeKnowledgeValue(value)
    if (text) normalized.add(text)
    const fileOnly = normalizeKnowledgeValue(String(value || '').split(/[/\\]/).pop())
    if (fileOnly) normalized.add(fileOnly)
  }
  return normalized
}

function documentMatchesReference(doc, reference) {
  const normalizedReference = normalizeKnowledgeValue(reference)
  if (!normalizedReference) return false
  const docRefs = getDocumentReferenceStrings(doc)
  for (const docRef of docRefs) {
    if (!docRef) continue
    if (normalizedReference === docRef || normalizedReference.includes(docRef) || docRef.includes(normalizedReference)) {
      return true
    }
  }
  return false
}

function getDocumentUsageCount(doc, logs) {
  if (!Array.isArray(logs) || logs.length === 0) return 0
  let count = 0
  for (const log of logs) {
    const sources = Array.isArray(log?.sources) ? log.sources : []
    const references = [
      ...sources,
      log?.source,
      log?.source_path,
      log?.title,
    ]
    const matched = references.some(reference => documentMatchesReference(doc, reference))
    if (matched) count += 1
  }
  return count
}

function getDocumentIssue(doc, indexHealth) {
  if (!indexHealth) {
    return { label: 'Unknown', key: 'unknown', tone: 'neutral' }
  }

  const sourcePath = normalizeKnowledgeValue(doc?.source_path)
  const title = normalizeKnowledgeValue(doc?.title)
  const fileName = normalizeKnowledgeValue(doc?.file_name)
  const failedDocs = indexHealth?.sqlite?.recent_failed_documents || []
  const zeroChunkDocs = indexHealth?.sqlite?.documents_with_zero_chunks || []
  const missingVectors = new Set((indexHealth?.drift?.sqlite_sources_missing_vectors || []).map(normalizeKnowledgeValue))
  const orphanVectors = new Set((indexHealth?.drift?.orphan_chroma_sources || []).map(normalizeKnowledgeValue))
  const isFailed = failedDocs.some(item => documentMatchesReference(doc, item.source_path || item.file_name || item.title || item.document_id))
  const isZeroChunk = zeroChunkDocs.some(item => documentMatchesReference(doc, item.source_path || item.file_name || item.title || item.document_id))
  const isMissingVectors = [sourcePath, title, fileName].some(value => value && missingVectors.has(value))
  const isOrphanVector = [sourcePath, title, fileName].some(value => value && orphanVectors.has(value))
  const isInactive = normalizeKnowledgeValue(doc?.status) !== 'active'

  if (isFailed) return { label: 'Failed ingestion', key: 'failed', tone: 'danger' }
  if (isZeroChunk) return { label: 'Zero chunks', key: 'zero_chunks', tone: 'danger' }
  if (isMissingVectors) return { label: 'Missing vectors', key: 'missing_vectors', tone: 'warning' }
  if (isOrphanVector) return { label: 'Orphan vector source', key: 'orphan_vector', tone: 'warning' }
  if (isInactive) return { label: 'Inactive', key: 'inactive', tone: 'neutral' }
  return { label: 'Healthy', key: 'healthy', tone: 'healthy' }
}

function buildIndexRecommendation(indexHealth) {
  if (!indexHealth) {
    return {
      healthy: false,
      reasons: ['Index health data is unavailable'],
      summary: 'Index health unavailable.',
    }
  }

  const failedDocs = (indexHealth?.sqlite?.recent_failed_documents || []).filter(d => d.ingestion_status === 'failed')
  const zeroChunkDocs = indexHealth?.sqlite?.documents_with_zero_chunks || []
  const zeroTextDocuments = Number(indexHealth?.sqlite?.zero_text_documents || 0)
  const drift = indexHealth?.drift || {}
  const sqliteChunks = indexHealth?.sqlite?.sqlite_chunks
  const vectorChunks = indexHealth?.vector?.vector_chunk_count ?? drift.vector_chunk_count
  const reasons = []

  if (failedDocs.length > 0) {
    reasons.push(`${failedDocs.length} failed document${failedDocs.length === 1 ? '' : 's'}`)
  }
  if (zeroChunkDocs.length > 0) {
    reasons.push(`${zeroChunkDocs.length} zero-chunk document${zeroChunkDocs.length === 1 ? '' : 's'}`)
  }
  if (zeroTextDocuments > 0) {
    reasons.push(`${zeroTextDocuments} document${zeroTextDocuments === 1 ? '' : 's'} with zero extracted text`)
  }
  if ((drift.orphan_chroma_sources || []).length > 0) {
    reasons.push(`${drift.orphan_chroma_sources.length} orphan Chroma source${drift.orphan_chroma_sources.length === 1 ? '' : 's'}`)
  }
  if ((drift.sqlite_sources_missing_vectors || []).length > 0) {
    reasons.push(`${drift.sqlite_sources_missing_vectors.length} SQLite source${drift.sqlite_sources_missing_vectors.length === 1 ? '' : 's'} missing vectors`)
  }
  if (drift.drift_detected) {
    const sqliteEligible = indexHealth?.sqlite?.sqlite_vector_eligible_chunks ?? 0
    reasons.push(`index drift detected (${vectorChunks} Chroma vs ${sqliteEligible} eligible SQLite)`)
  }

  return {
    healthy: reasons.length === 0,
    reasons,
    summary: reasons.length === 0 ? 'Index looks healthy.' : 'Reindex recommended.',
  }
}

function KnowledgePage({
  statusData,
  stats,
  indexHealth,
  search,
  setSearch,
  department,
  setDepartment,
  issue,
  setIssue,
  fileType,
  setFileType,
  usage,
  setUsage,
  onReingest,
  onRemove,
  reingestPath,
  removePath,
  reingestError,
  reingestMessage,
  removeError,
  removeMessage,
}) {
  const docs = statusData?.documents || []
  const logs = stats?.recent_logs || []
  const indexRecommendation = useMemo(() => buildIndexRecommendation(indexHealth), [indexHealth])

  const enrichedDocs = useMemo(() => {
    return docs.map(doc => {
      const usageCount = getDocumentUsageCount(doc, logs)
      const issueInfo = getDocumentIssue(doc, indexHealth)
      return {
        ...doc,
        usageCount,
        issueLabel: issueInfo.label,
        issueKey: issueInfo.key,
        issueTone: issueInfo.tone,
      }
    })
  }, [docs, logs, indexHealth])

  const filteredDocs = useMemo(() => {
    return enrichedDocs.filter(doc => {
      const searchText = normalizeKnowledgeValue(search)
      if (searchText) {
        const haystack = normalizeKnowledgeValue([
          doc.title,
          doc.file_name,
          doc.source_path,
          doc.department,
          doc.file_type,
          doc.status,
          doc.ingestion_status,
          doc.issueLabel,
        ].join(' '))
        if (!haystack.includes(searchText)) return false
      }
      if (department !== 'all' && normalizeKnowledgeValue(doc.department || 'General') !== normalizeKnowledgeValue(department)) return false
      if (issue !== 'all' && doc.issueKey !== issue) return false
      if (fileType !== 'all' && normalizeKnowledgeValue(doc.file_type || 'unknown') !== normalizeKnowledgeValue(fileType)) return false
      if (usage === 'used' && doc.usageCount === 0) return false
      if (usage === 'unused' && doc.usageCount > 0) return false
      return true
    })
  }, [enrichedDocs, search, department, issue, fileType, usage])

  const departments = useMemo(() => {
    const values = new Set()
    for (const doc of enrichedDocs) {
      const value = String(doc.department || 'General').trim()
      if (value) values.add(value)
    }
    return ['all', ...Array.from(values).sort((a, b) => a.localeCompare(b))]
  }, [enrichedDocs])
  const fileTypes = useMemo(() => {
    const values = new Set()
    for (const doc of enrichedDocs) {
      const value = String(doc.file_type || 'unknown').trim()
      if (value) values.add(value)
    }
    return ['all', ...Array.from(values).sort((a, b) => a.localeCompare(b))]
  }, [enrichedDocs])

  const activeDocs = enrichedDocs.filter(doc => normalizeKnowledgeValue(doc.status) === 'active').length
  const failedDocs = indexHealth?.sqlite?.recent_failed_documents || []
  const zeroChunkDocs = indexHealth?.sqlite?.documents_with_zero_chunks || []
  const unusedDocs = enrichedDocs.filter(doc => doc.usageCount === 0).length
  const totalChunks = enrichedDocs.reduce((sum, doc) => sum + Number(doc.chunks || 0), 0)

  const exportKnowledge = () => {
    const blob = new Blob([JSON.stringify(filteredDocs, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'spark-knowledge.json'
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <>
      <SectionHeader
        title="Knowledge"
        subtitle="Review the indexed document registry, usage, and ingestion health."
      />

      <div className={styles.metricRow}>
        <MiniMetric label="Total documents" value={docs.length} hint="All indexed source documents" />
        <MiniMetric label="Active documents" value={activeDocs} hint="Currently searchable" />
        <MiniMetric
          label="Failed documents"
          value={indexHealth ? failedDocs.length : 'N/A'}
          hint="Ingestion errors or unsupported files"
          details={failedDocs.map(d => d.title || d.file_name || d.source_path)}
        />
        <MiniMetric
          label="Zero-chunk documents"
          value={indexHealth ? zeroChunkDocs.length : 'N/A'}
          hint="Documents without extracted chunks"
          details={zeroChunkDocs.map(d => d.title || d.file_name || d.source_path)}
        />
        <MiniMetric label="Unused documents" value={unusedDocs} hint="No recent log references" />
        <MiniMetric label="Total chunks (SQLite)" value={totalChunks} hint="Full audit set including low-signal content" />
      </div>

      <div className={styles.indexRecommendation}>
        <strong>{indexRecommendation.summary}</strong>
        {indexRecommendation.reasons.length > 0 && (
          <div className={styles.indexRecommendationMeta}>{indexRecommendation.reasons.join(' · ')}</div>
        )}
      </div>

      <div className={styles.filterBar}>
        <input
          className={styles.searchInput}
          placeholder="Search documents"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
        <select className={styles.select} value={department} onChange={e => setDepartment(e.target.value)}>
          {departments.map(item => (
            <option key={item} value={item}>{item === 'all' ? 'Department' : item}</option>
          ))}
        </select>
        <select className={styles.select} value={issue} onChange={e => setIssue(e.target.value)}>
          <option value="all">Status / Issue</option>
          <option value="healthy">Healthy</option>
          <option value="failed">Failed ingestion</option>
          <option value="zero_chunks">Zero chunks</option>
          <option value="missing_vectors">Missing vectors</option>
          <option value="orphan_vector">Orphan vector source</option>
          <option value="inactive">Inactive</option>
          <option value="unknown">Unknown</option>
        </select>
        <select className={styles.select} value={fileType} onChange={e => setFileType(e.target.value)}>
          {fileTypes.map(item => (
            <option key={item} value={item}>{item === 'all' ? 'File type' : item.toUpperCase()}</option>
          ))}
        </select>
        <select className={styles.select} value={usage} onChange={e => setUsage(e.target.value)}>
          <option value="all">Used / Unused</option>
          <option value="used">Used</option>
          <option value="unused">Unused</option>
        </select>
        <button type="button" className={styles.filterButton} onClick={exportKnowledge}>Export</button>
      </div>

      {(reingestMessage || reingestError || removeMessage || removeError) && (
        <div className={(reingestError || removeError) ? styles.indexHealthWarning : styles.indexRecommendation}>
          <strong>{removeError || reingestError || removeMessage || reingestMessage}</strong>
        </div>
      )}

      <div className={styles.tableCard}>
        {filteredDocs.length === 0 ? (
          <EmptyState text={docs.length === 0 ? 'No indexed documents available yet.' : 'No documents match the current filters.'} />
        ) : (
          <div className={styles.knowledgeTableWrap}>
            <table className={styles.knowledgeTable}>
              <thead>
                <tr>
                  <th>Document</th>
                  <th>Department</th>
                  <th>Status</th>
                  <th>Issue</th>
                  <th>Chunks</th>
                  <th>Type</th>
                  <th>Last ingested</th>
                  <th>Last modified</th>
                  <th>Usage</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {filteredDocs.map(doc => (
                  <tr key={doc.document_id || doc.source_path || doc.file_name}>
                    <td>
                      <div className={styles.knowledgeDocTitle}>{getDocumentDisplayName(doc)}</div>
                      <div className={styles.knowledgeDocMeta}>{doc.source_path || doc.file_name || 'N/A'}</div>
                    </td>
                    <td>{doc.department || 'General'}</td>
                    <td>
                      <div className={styles.knowledgeStatusStack}>
                        <span className={styles.knowledgeStatusValue}>{doc.status || 'unknown'}</span>
                        <span className={styles.knowledgeStatusMeta}>{doc.ingestion_status || 'pending'}</span>
                      </div>
                    </td>
                    <td>
                      <span className={`${styles.issuePill} ${styles[`issuePill${doc.issueTone.charAt(0).toUpperCase()}${doc.issueTone.slice(1)}`] || ''}`}>
                        {doc.issueLabel}
                      </span>
                    </td>
                    <td>
                      <div className={styles.knowledgeStatusStack}>
                        <span className={styles.knowledgeStatusValue}>{doc.chunks ?? 0}</span>
                        <span className={styles.knowledgeStatusMeta}>{doc.page_count ? `${doc.page_count} pages` : 'Page count unavailable'}</span>
                      </div>
                    </td>
                    <td>{String(doc.file_type || 'unknown').toUpperCase()}</td>
                    <td>{doc.last_ingested_at || 'N/A'}</td>
                    <td>{doc.last_modified || 'N/A'}</td>
                    <td>{doc.usageCount}</td>
                    <td>
                      <div className={styles.knowledgeActions}>
                        <button
                          type="button"
                          className={styles.filterButton}
                          disabled={!doc.source_path || reingestPath === doc.source_path || removePath === doc.source_path || indexHealth?.ingestion_active}
                          onClick={() => onReingest(doc.source_path)}
                        >
                          {(reingestPath === doc.source_path || indexHealth?.ingestion_active) ? 'Re-ingesting...' : 'Re-ingest'}
                        </button>
                        <button
                          type="button"
                          className={`${styles.filterButton} ${styles.destructiveButton}`}
                          disabled={!doc.source_path || reingestPath === doc.source_path || removePath === doc.source_path}
                          onClick={() => onRemove(doc.source_path)}
                        >
                          {removePath === doc.source_path ? 'Removing...' : 'Remove'}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </>
  )
}

function getDepartmentName(log, statusData) {
  const source = log?.sources?.[0]
  const doc = (statusData?.documents || []).find(item => item.file === source)
  return doc?.department || 'General'
}

function getDateRange(logs) {
  if (!logs.length) return 'No recent logs'
  const dates = logs
    .map(log => new Date(String(log.timestamp || '').replace(' ', 'T')))
    .filter(date => !Number.isNaN(date.getTime()))
    .sort((a, b) => a - b)
  if (!dates.length) return 'No recent logs'
  const first = dates[0].toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
  const last = dates[dates.length - 1].toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
  return `${first} - ${last}`
}

function getRecentQuestions(logs) {
  const seen = new Set()
  const items = []
  for (const log of logs) {
    const question = String(log.question || '').trim()
    if (!question) continue
    const key = question.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    items.push(question)
  }
  return items.slice(0, 6)
}

function SectionHeader({ title, subtitle }) {
  return (
    <div className={styles.sectionHeader}>
      <div>
        <h2 className={styles.sectionTitle}>{title}</h2>
        {subtitle && <div className={styles.sectionSubtitle}>{subtitle}</div>}
      </div>
    </div>
  )
}

function EmptyState({ text }) {
  return <div className={styles.emptyState}>{text}</div>
}

function MiniMetric({ label, value, hint, details }) {
  return (
    <div className={styles.metricCard} title={Array.isArray(details) && details.length > 0 ? details.join('\n') : undefined}>
      <div className={styles.metricLabel}>{label}</div>
      <div className={styles.metricValue}>{value}</div>
      {hint && <div className={styles.metricHint}>{hint}</div>}
    </div>
  )
}

function SparkBarChart({ data = [] }) {
  const max = Math.max(...data, 1)
  return (
    <div className={styles.chartCard}>
      <div className={styles.chartTitle}>Search volume</div>
      <div className={styles.barRow}>
        {data.map((value, index) => (
          <div
            key={index}
            className={`${styles.bar} ${index === data.length - 1 ? styles.barActive : ''}`}
            style={{ height: `${(value / max) * 100}%` }}
            title={`Day ${index + 1}: ${value}`}
          />
        ))}
      </div>
    </div>
  )
}

function DistributionCard({ title, items }) {
  return (
    <div className={styles.chartCard}>
      <div className={styles.chartTitle}>{title}</div>
      <div className={styles.distributionList}>
        {items.map(item => (
          <div key={item.label} className={styles.distributionRow}>
            <span>{item.label}</span>
            <strong>{item.value}</strong>
          </div>
        ))}
      </div>
    </div>
  )
}

function SettingsTabs({ active, onChange }) {
  return (
    <div className={styles.subTabs}>
      {SETTINGS_TABS.map(tab => (
        <button
          key={tab.id}
          type="button"
          className={`${styles.subTabBtn} ${active === tab.id ? styles.subTabBtnActive : ''}`}
          onClick={() => onChange(tab.id)}
        >
          {tab.label}
        </button>
      ))}
    </div>
  )
}

function SuggestionCard({ category, question, icon }) {
  const iconOption = getIconOption(icon)
  return (
    <div className={styles.splashCard}>
      <div className={styles.splashTop}>
        <div className={styles.splashIcon}>
          <FontAwesomeIcon icon={iconOption.icon} />
        </div>
        <div className={styles.splashChip}>{category}</div>
      </div>
      <div className={styles.splashQuestion}>{question}</div>
    </div>
  )
}

function SearchLogsPage({ logs, statusData, search, setSearch, department, setDepartment, feedback, setFeedback, answered, setAnswered }) {
  const filtered = logs.filter(log => {
    const question = String(log.question || '').toLowerCase()
    const answer = String(log.answer || '').toLowerCase()
    const dept = getDepartmentName(log, statusData)
    if (search && !question.includes(search.toLowerCase()) && !answer.includes(search.toLowerCase())) return false
    if (department !== 'all' && dept !== department) return false
    if (feedback !== 'all' && (log.feedback || 'none') !== feedback) return false
    if (answered === 'answered' && isFallbackAnswer(log.answer)) return false
    if (answered === 'not_found' && !isFallbackAnswer(log.answer)) return false
    return true
  })

  const departments = ['all', ...new Set((statusData?.documents || []).map(d => d.department || 'General'))]
  const users = ['all', ...new Set(logs.map(log => log.user_scope || 'local_user'))]

  const exportLogs = () => {
    const blob = new Blob([JSON.stringify(filtered, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'spark-search-logs.json'
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <>
      <SectionHeader
        title="Search Logs"
        subtitle="Review individual searches, answer status, and feedback."
      />

      <div className={styles.filterBar}>
        <div className={styles.datePill}>{getDateRange(logs)}</div>
        <input
          className={styles.searchInput}
          placeholder="Search by message"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
        <button type="button" className={styles.iconButton} title="Favorite">Star</button>
        <button type="button" className={styles.iconButton} title="Thumbs up">Up</button>
        <button type="button" className={styles.iconButton} title="Thumbs down">Down</button>
        <select className={styles.select} value={department} onChange={e => setDepartment(e.target.value)}>
          {departments.map(item => <option key={item} value={item}>{item === 'all' ? 'Department' : item}</option>)}
        </select>
        <select className={styles.select} defaultValue="all">
          {users.map(item => <option key={item} value={item}>{item === 'all' ? 'Users' : item}</option>)}
        </select>
        <select className={styles.select} value={answered} onChange={e => setAnswered(e.target.value)}>
          <option value="all">Answered</option>
          <option value="answered">All answered</option>
          <option value="not_found">Answer not found</option>
        </select>
        <select className={styles.select} defaultValue="all">
          <option value="all">Types</option>
          <option value="chat">Chat</option>
          <option value="file">File Search</option>
        </select>
        <select className={styles.select} value={feedback} onChange={e => setFeedback(e.target.value)}>
          <option value="all">Feedback</option>
          <option value="up">Thumbs up</option>
          <option value="down">Thumbs down</option>
          <option value="none">Unrated</option>
        </select>
        <button type="button" className={styles.filterButton}>Written Feedback</button>
        <button
          type="button"
          className={styles.filterButton}
          onClick={() => {
            setSearch('')
            setDepartment('all')
            setFeedback('all')
            setAnswered('all')
          }}
        >
          All Filters
        </button>
        <button type="button" className={styles.filterButton} onClick={exportLogs}>Export</button>
      </div>

      <div className={styles.tableCard}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>Date &amp; Time</th>
              <th>Department</th>
              <th>User</th>
              <th>First Message</th>
              <th>Answered</th>
              <th>Type</th>
              <th>Feedback</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td colSpan="7">
                  <EmptyState text="No matching logs." />
                </td>
              </tr>
            ) : (
              filtered.map((log, index) => (
                <tr key={`${log.timestamp}-${index}`}>
                  <td>{log.timestamp || 'N/A'}</td>
                  <td>{getDepartmentName(log, statusData)}</td>
                  <td>{log.user_scope || 'local_user'}</td>
                  <td className={styles.questionCell}>{log.question || 'N/A'}</td>
                  <td>{isFallbackAnswer(log.answer) ? 'Not found' : 'All answered'}</td>
                  <td>{(log.sources?.length || 0) > 0 ? 'File Search' : 'Chat'}</td>
                  <td>{log.feedback === 'up' ? 'Up' : log.feedback === 'down' ? 'Down' : 'N/A'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </>
  )
}

function AnalyticsPage({ stats, indexHealth, indexHealthError }) {
  const logs = stats?.recent_logs || []
  const uniqueUsers = useMemo(() => new Set(logs.map(log => log.user_scope || 'local_user')).size, [logs])
  const answerNotFound = logs.filter(log => isFallbackAnswer(log.answer)).length
  const failedDocs = indexHealth?.sqlite?.recent_failed_documents || []
  const zeroChunkDocs = indexHealth?.sqlite?.documents_with_zero_chunks || []
  const ocrDocuments = Number(indexHealth?.sqlite?.ocr_documents || 0)
  const ocrFailedDocuments = Number(indexHealth?.sqlite?.ocr_failed_documents || 0)
  const zeroTextDocuments = Number(indexHealth?.sqlite?.zero_text_documents || 0)
  const pageCounts = indexHealth?.sqlite?.page_extraction_counts || {}
  const extractionMethodCounts = indexHealth?.sqlite?.extractionMethodCounts || {}
  const drift = indexHealth?.drift || {}
  const healthStatus = indexHealth?.status || 'unknown'
  const indexRecommendation = buildIndexRecommendation(indexHealth)

  return (
    <>
      <SectionHeader
        title="Analytics & Insights"
        subtitle="See how Spark is performing, and guidance on how to improve it."
      />

      <div className={styles.analyticsFilters}>
        <button type="button" className={styles.filterButton}>Custom</button>
        <div className={styles.datePill}>{getDateRange(logs)}</div>
        <button type="button" className={styles.filterButton}>Interval</button>
        <button type="button" className={styles.filterButton}>Types (All)</button>
        <button type="button" className={styles.filterButton}>Filters</button>
      </div>

      <div className={styles.metricRow}>
        <MiniMetric label="Total searches" value={stats?.ops_health?.total_logs || 0} hint="All recorded queries" />
        <MiniMetric label="Answer not found" value={answerNotFound} hint="Fallback responses" />
        <MiniMetric label="Active users" value={uniqueUsers} hint="Unique user scopes" />
      </div>

      <div className={styles.panelCard}>
        <div className={styles.panelTitleRow}>
          <div className={styles.panelTitle}>Index Health</div>
          <div className={`${styles.healthBadge} ${healthStatus === 'ok' ? styles.healthBadgeOk : styles.healthBadgeWarn}`}>
            {healthStatus === 'ok' ? 'ok' : healthStatus === 'warning' ? 'warning' : 'unavailable'}
          </div>
        </div>
        <div className={styles.indexRecommendation}>
          <strong>{indexRecommendation.summary}</strong>
          {!indexRecommendation.healthy && (
            <div className={styles.indexRecommendationMeta}>
              {formatList(indexRecommendation.reasons, 'No issues found')}
            </div>
          )}
        </div>
        {indexHealthError && <div className={styles.indexHealthWarning}>{indexHealthError}</div>}

        <div className={styles.healthGrid}>
        <MiniMetric label="SQLite active documents" value={indexHealth?.sqlite?.sqlite_active_documents ?? 'N/A'} hint="Active documents in registry" />
        <MiniMetric label="SQLite active chunks" value={indexHealth?.sqlite?.sqlite_active_chunks ?? 'N/A'} hint="Total chunks in SQLite for active docs" />
        <MiniMetric label="SQLite vector-eligible" value={indexHealth?.sqlite?.sqlite_vector_eligible_chunks ?? 'N/A'} hint="High-signal chunks classified for indexing" />
        <MiniMetric label="SQLite vector-skipped" value={indexHealth?.sqlite?.sqlite_vector_skipped_chunks ?? 'N/A'} hint="Low-signal chunks (boilerplate, TOC, etc.)" />
        <MiniMetric
          label="Vector chunk count"
          value={indexHealth?.vector?.vector_chunk_count ?? indexHealth?.drift?.vector_chunk_count ?? 'N/A'}
          hint="Chunks actually present in ChromaDB"
          details={drift.orphan_chroma_sources || []}
        />
        <MiniMetric label="OCR documents" value={ocrDocuments} hint="Documents with at least one OCR page" />
        <MiniMetric label="OCR chunks" value={indexHealth?.sqlite?.ocrChunkCount ?? 0} hint="Chunks created from OCR text" />
        <MiniMetric label="OCR pages" value={pageCounts.ocr_pages ?? 0} hint="Pages extracted with OCR" />
        <MiniMetric
          label="Zero text documents"
          value={zeroTextDocuments}
          hint="Documents with no extracted text"
          details={indexHealth?.sqlite?.zero_text_documents_list?.map(d => d.title || d.file_name) || []}
        />
        <MiniMetric
          label="OCR failed documents"
          value={ocrFailedDocuments}
          hint="Documents where OCR returned no usable text"
          details={indexHealth?.sqlite?.recent_failed_documents?.filter(d => d.ingestion_status === 'failed' && d.ingestion_error?.includes('OCR')).map(d => d.title || d.file_name) || []}
        />
      </div>

        <div className={styles.healthDetails}>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>Last ingestion run</div>
            <div className={styles.healthDetailValue}>{indexHealth?.last_ingestion_run?.status || 'N/A'}</div>
            <div className={styles.healthDetailMeta}>{indexHealth?.last_ingestion_run?.finished_at || indexHealth?.last_ingestion_run?.started_at || 'No run recorded'}</div>
          </div>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>Failed documents</div>
            <div className={styles.healthDetailValue}>{failedDocs.length}</div>
            <div className={styles.healthDetailMeta}>{formatList(failedDocs.map(doc => doc.title || doc.file_name), 'None')}</div>
          </div>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>Zero-chunk documents</div>
            <div className={styles.healthDetailValue}>{zeroChunkDocs.length}</div>
            <div className={styles.healthDetailMeta}>{formatList(zeroChunkDocs.map(doc => doc.title || doc.file_name), 'None')}</div>
          </div>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>Page extraction</div>
            <div className={styles.healthDetailValue}>{pageCounts.total_pages ?? 0}</div>
            <div className={styles.healthDetailMeta}>
              {`Text layer: ${pageCounts.text_layer_pages ?? 0} · OCR: ${pageCounts.ocr_pages ?? 0} · Failed: ${pageCounts.failed_pages ?? 0}`}
            </div>
          </div>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>Extraction methods</div>
            <div className={styles.healthDetailValue}>{Object.values(extractionMethodCounts).reduce((sum, value) => sum + Number(value || 0), 0)}</div>
            <div className={styles.healthDetailMeta}>
              {`text_layer: ${extractionMethodCounts.text_layer ?? 0} · ocr: ${extractionMethodCounts.ocr ?? 0} · ocr_failed: ${extractionMethodCounts.ocr_failed ?? 0}`}
            </div>
          </div>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>Vector skip reasons</div>
            <div className={styles.healthDetailValue}>{indexHealth?.sqlite?.sqlite_vector_skipped_chunks ?? 0}</div>
            <div className={styles.healthDetailMeta}>
              {Object.entries(indexHealth?.sqlite?.vector_skip_reason_counts || {})
                .map(([reason, count]) => `${reason}: ${count}`)
                .join(' · ') || 'None recorded'}
            </div>
          </div>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>Embedding models</div>
            <div className={styles.healthDetailValue}>{Object.keys(indexHealth?.vector?.embedding_model_ids || {}).length}</div>
            <div className={styles.healthDetailMeta}>
              {Object.entries(indexHealth?.vector?.embedding_model_ids || {})
                .map(([model, count]) => `${model}: ${count}`)
                .join(' · ') || 'None recorded'}
            </div>
          </div>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>Orphan Chroma sources</div>
            <div className={styles.healthDetailValue}>{drift.orphan_chroma_sources?.length || 0}</div>
            <div className={styles.healthDetailMeta}>{formatList(drift.orphan_chroma_sources, 'None')}</div>
          </div>
          <div className={styles.healthDetail}>
            <div className={styles.healthDetailLabel}>SQLite sources missing vectors</div>
            <div className={styles.healthDetailValue}>{drift.sqlite_sources_missing_vectors?.length || 0}</div>
            <div className={styles.healthDetailMeta}>{formatList(drift.sqlite_sources_missing_vectors, 'None')}</div>
          </div>
        </div>
      </div>

      <div className={styles.chartGrid}>
        <SparkBarChart data={stats?.volume_14d || []} />
        <DistributionCard
          title="Searches over time"
          items={[
            { label: '14-day volume', value: stats?.volume_14d?.reduce((a, b) => a + b, 0) || 0 },
            { label: 'Answer not found', value: answerNotFound },
            { label: 'Active users', value: uniqueUsers },
          ]}
        />
        <DistributionCard
          title="Feedback"
          items={[
            { label: 'Thumbs up', value: stats?.feedback_dist?.up || 0 },
            { label: 'Thumbs down', value: stats?.feedback_dist?.down || 0 },
            { label: 'Unrated', value: stats?.feedback_dist?.none || 0 },
          ]}
        />
      </div>

      <div className={styles.panelCard}>
        <div className={styles.panelTitle}>Recent logs</div>
        <div className={styles.userList}>
          {logs.slice(0, 6).map((log, index) => (
            <div key={`${log.timestamp}-${index}`} className={styles.logRow}>
              <div>
                <div className={styles.userName}>{log.question}</div>
                <div className={styles.userMeta}>{log.timestamp}</div>
              </div>
              <strong>{isFallbackAnswer(log.answer) ? 'Answer not found' : 'Answered'}</strong>
            </div>
          ))}
        </div>
      </div>
    </>
  )
}

function DepartmentsPage({ statusData }) {
  const docs = statusData?.documents || []
  const byDepartment = new Map()

  for (const doc of docs) {
    const key = doc.department || 'General'
    if (!byDepartment.has(key)) {
      byDepartment.set(key, { documents: 0, chunks: 0, latest: doc.ingested_at || 'N/A' })
    }
    const item = byDepartment.get(key)
    item.documents += 1
    item.chunks += Number(doc.chunks || 0)
    if (doc.ingested_at && doc.ingested_at > item.latest) item.latest = doc.ingested_at
  }

  const totalUsers = docs.length > 0 ? docs.length : 0

  return (
    <>
      <SectionHeader
        title="Departments"
        subtitle="Documents are grouped by department and source metadata."
      />

      <div className={styles.secondaryTabs}>
        <button type="button" className={`${styles.secondaryTab} ${styles.secondaryTabActive}`}>Users</button>
        <button type="button" className={styles.secondaryTab}>Departments</button>
      </div>

      <div className={styles.departmentGrid}>
        <div className={styles.departmentCreateCard}>
          <button type="button" className={styles.primaryButton}>Create Department</button>
          <p>Add a department to permission documents and users.</p>
        </div>

        {Array.from(byDepartment.entries()).map(([department, info], index) => (
          <div key={department} className={styles.departmentCard}>
            <div className={styles.cardMenu}>...</div>
            <div className={styles.departmentBadge}>{department}</div>
            <div className={styles.departmentCount}>{index === 0 ? `${totalUsers} Users` : `${info.documents} Users`}</div>
            <div className={styles.departmentMeta}>{info.documents} Documents</div>
            <div className={styles.departmentDescription}>Department documents available in the knowledge base.</div>
            <div className={styles.departmentFooter}>Added {info.latest}</div>
          </div>
        ))}
      </div>

      <div className={styles.panelCard}>
        <div className={styles.panelTitle}>Users</div>
        <div className={styles.userList}>
          {docs.length > 0 ? (
            <div className={styles.userRow}>
              <div>
                <div className={styles.userName}>local_user</div>
                <div className={styles.userMeta}>Current user scope</div>
              </div>
              <strong>{docs.length} documents</strong>
            </div>
          ) : (
            <EmptyState text="No users available." />
          )}
        </div>
      </div>
    </>
  )
}

function SettingsPage({
  logs,
  settingsTab,
  setSettingsTab,
  settings,
  setSettings,
  onResetSettings,
  onPurgeIndex,
  onReingestAll,
  purgeRunning,
  reingestRunning,
  purgeResult,
  reingestResult,
}) {
  const suggestions = getRecentQuestions(logs)
  const [draftCard, setDraftCard] = useState({ icon: 'sparkles', category: 'Support', question: '' })
  const [editingCardId, setEditingCardId] = useState(null)

  useEffect(() => {
    if (settingsTab !== 'splash') return
    if (settings.splashCards?.length === 0 && (suggestions.length > 0 || DEFAULT_SPLASH_CARD_PRESETS.length > 0)) {
      setSettings(prev => {
        if ((prev.splashCards || []).length > 0) return prev
        const seedCards = suggestions.length > 0
          ? suggestions.slice(0, 6).map((question, index) => ({
              id: makeId(),
              icon: SPLASH_ICON_OPTIONS[index % SPLASH_ICON_OPTIONS.length].value,
              category: index % 3 === 0 ? 'Support' : index % 3 === 1 ? 'Rates' : 'Payment',
              question,
            }))
          : DEFAULT_SPLASH_CARD_PRESETS.map(card => ({ ...card, id: makeId() }))
        return {
          ...prev,
          splashCards: seedCards,
        }
      })
    }
  }, [settingsTab, settings.splashCards, suggestions, setSettings])

  const updateSetting = useCallback((key, value) => {
    setSettings(prev => ({ ...prev, [key]: value }))
  }, [setSettings])

  const resetDraft = useCallback(() => {
    setEditingCardId(null)
    setDraftCard({ icon: 'sparkles', category: 'Support', question: '' })
  }, [])

  const saveDraftCard = useCallback(() => {
    const nextCard = {
      id: editingCardId || makeId(),
      icon: draftCard.icon || 'sparkles',
      category: (draftCard.category || 'Support').trim() || 'Support',
      question: (draftCard.question || '').trim(),
    }
    if (!nextCard.question) return
    setSettings(prev => {
      const cards = Array.isArray(prev.splashCards) ? [...prev.splashCards] : []
      if (editingCardId) {
        const index = cards.findIndex(card => card.id === editingCardId)
        if (index >= 0) cards[index] = nextCard
      } else {
        cards.unshift(nextCard)
      }
      return { ...prev, splashCards: cards.slice(0, 12) }
    })
    resetDraft()
  }, [draftCard, editingCardId, resetDraft, setSettings])

  const editCard = useCallback((card) => {
    setEditingCardId(card.id)
    setDraftCard({
      icon: card.icon || 'sparkles',
      category: card.category || 'Support',
      question: card.question || '',
    })
  }, [])

  const deleteCard = useCallback((cardId) => {
    setSettings(prev => ({
      ...prev,
      splashCards: (prev.splashCards || []).filter(card => card.id !== cardId),
    }))
    if (editingCardId === cardId) {
      resetDraft()
    }
  }, [editingCardId, resetDraft, setSettings])

  const splashCards = settings.splashCards || []
  const previewTheme = {
    ...buildThemeVars(settings),
  }

  return (
    <>
      <SectionHeader
        title="Settings"
        subtitle="Configure and customize your assistant's look and feel, response behavior, and more."
      />

      <SettingsTabs active={settingsTab} onChange={setSettingsTab} />

      {settingsTab === 'overview' && (
        <div className={styles.overviewGrid}>
          <div className={styles.overviewCard}>
            <div className={styles.settingLabel}>Delete All Vector & Chunk Data</div>
            <div className={styles.fieldHelp}>
              Purges Chroma vectors and SQLite chunk/page index data. Intake source files and admin settings are not deleted.
            </div>
            <div className={styles.actionRow}>
              <button
                type="button"
                className={`${styles.secondaryButton} ${styles.destructiveActionButton}`}
                onClick={onPurgeIndex}
                disabled={purgeRunning || reingestRunning}
              >
                {purgeRunning ? 'Deleting...' : 'Delete All Vector & Chunk Data'}
              </button>
            </div>
            {purgeResult && (
              <div className={purgeResult.ok ? styles.overviewStatusOk : styles.overviewStatusError}>
                {purgeResult.message}
              </div>
            )}
          </div>

          <div className={styles.overviewCard}>
            <div className={styles.settingLabel}>Re-ingest All Intake Data</div>
            <div className={styles.fieldHelp}>
              Re-scans intake and rebuilds supported document indexes for PDF, DOCX, TXT, MD, XLSX, CSV, and XLSM.
            </div>
            <div className={styles.actionRow}>
              <button
                type="button"
                className={styles.primaryButton}
                onClick={onReingestAll}
                disabled={purgeRunning || reingestRunning}
              >
                {reingestRunning ? 'Re-ingesting...' : 'Re-ingest All Intake Data'}
              </button>
            </div>
            {reingestResult && (
              <div className={reingestResult.ok ? styles.overviewStatusOk : styles.overviewStatusError}>
                {reingestResult.message}
              </div>
            )}
          </div>
        </div>
      )}

      {settingsTab === 'appearance' && (
        <div className={styles.settingsGrid}>
          <div className={styles.previewSettingsCard} style={previewTheme}>
            <div className={styles.settingLabel}>Live Preview</div>
            <div className={styles.appearancePreview}>
              <div className={styles.appearanceAvatarWrap}>
                <img src="/ui/logo.png" alt="Spark" className={styles.avatar} onError={e => { e.currentTarget.style.display = 'none' }} />
              </div>
              <div>
                <div className={styles.appearancePreviewTitle}>{settings.assistantName || 'SPARK'}</div>
                <div className={styles.appearancePreviewMeta}>Brand color preview and assistant identity</div>
              </div>
            </div>
            <div className={styles.appearanceSwatchRow}>
              <span className={styles.appearanceSwatch} style={{ background: settings.brandColor }} />
              <span className={styles.appearanceSwatch} style={{ background: settings.brandColorLight }} />
              <span className={styles.appearanceSwatch} style={{ background: settings.brandGold }} />
              <span className={styles.appearanceSwatch} style={{ background: settings.brandGoldWarm }} />
              <span className={styles.appearanceSwatch} style={{ background: settings.brandOrange }} />
            </div>
          </div>

          <div className={styles.settingBlock}>
            <div className={styles.settingLabel}>Assistant Name</div>
            <input
              className={styles.textInput}
              value={settings.assistantName || ''}
              onChange={e => updateSetting('assistantName', e.target.value)}
            />
            <div className={styles.fieldHelp}>This is what users see when the assistant speaks.</div>
          </div>

          <div className={styles.settingBlock}>
            <div className={styles.settingLabel}>Brand Palette</div>
            <div className={styles.paletteGrid}>
              {[
                ['brandColor', 'Primary', settings.brandColor],
                ['brandColorDark', 'Primary Dark', settings.brandColorDark],
                ['brandColorLight', 'Primary Light', settings.brandColorLight],
                ['brandGold', 'Gold', settings.brandGold],
                ['brandGoldWarm', 'Warm Gold', settings.brandGoldWarm],
                ['brandOrange', 'Orange', settings.brandOrange],
                ['brandSuccess', 'Success', settings.brandSuccess],
                ['brandInfo', 'Info', settings.brandInfo],
              ].map(([key, label, value]) => (
                <label key={key} className={styles.paletteItem}>
                  <span>{label}</span>
                  <div className={styles.paletteInputRow}>
                    <input
                      className={styles.colorInput}
                      value={value}
                      onChange={e => updateSetting(key, e.target.value)}
                    />
                    <span className={styles.colorSwatch} style={{ background: value }} />
                  </div>
                </label>
              ))}
            </div>
          </div>

          <div className={styles.settingBlock}>
            <div className={styles.settingLabel}>AI Disclaimer</div>
            <textarea
              className={styles.textArea}
              value={settings.disclaimer || ''}
              onChange={e => updateSetting('disclaimer', e.target.value)}
              rows={4}
            />
          </div>

          <div className={styles.settingBlock}>
            <div className={styles.settingLabel}>Answer Not Found Response</div>
            <textarea
              className={styles.textArea}
              value={settings.notFound || ''}
              onChange={e => updateSetting('notFound', e.target.value)}
              rows={3}
            />
          </div>

          <div className={styles.settingBlock}>
            <div className={styles.settingRow}>
              <div>
                <div className={styles.settingLabel}>Theme Actions</div>
                <div className={styles.fieldHelp}>Changes persist locally in this browser.</div>
              </div>
              <div className={styles.actionRow}>
                <button type="button" className={styles.secondaryButton} onClick={onResetSettings}>Reset to defaults</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {settingsTab === 'splash' && (
        <div className={styles.twoColumn}>
          <div className={styles.settingBlock}>
            <div className={styles.settingLabel}>{editingCardId ? 'Edit Splash Card' : 'Add Splash Card'}</div>
            <div className={styles.splashEditor}>
              <label className={styles.fieldGroup}>
                <span>Icon</span>
                <select
                  className={styles.select}
                  value={draftCard.icon}
                  onChange={e => setDraftCard(prev => ({ ...prev, icon: e.target.value }))}
                >
                  {SPLASH_ICON_OPTIONS.map(option => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
              </label>
              <label className={styles.fieldGroup}>
                <span>Category</span>
                <input
                  className={styles.textInput}
                  value={draftCard.category}
                  onChange={e => setDraftCard(prev => ({ ...prev, category: e.target.value }))}
                  placeholder="Support, Rates, Payment..."
                />
              </label>
              <label className={styles.fieldGroup}>
                <span>Question</span>
                <textarea
                  className={styles.textArea}
                  value={draftCard.question}
                  onChange={e => setDraftCard(prev => ({ ...prev, question: e.target.value }))}
                  rows={4}
                  placeholder="Type the splash question users will see."
                />
              </label>
              <div className={styles.actionRow}>
                <button type="button" className={styles.primaryButton} onClick={saveDraftCard}>
                  {editingCardId ? 'Update Card' : 'Add Card'}
                </button>
                <button type="button" className={styles.secondaryButton} onClick={resetDraft}>
                  Clear
                </button>
              </div>
            </div>
          </div>

          <div className={styles.settingBlock}>
            <div className={styles.settingLabel}>Current Splash Cards</div>
            {splashCards.length > 0 ? (
              <div className={styles.splashManageList}>
                {splashCards.map(card => {
                  const iconOption = getIconOption(card.icon)
                  return (
                    <div key={card.id} className={styles.splashManageCard}>
                      <div className={styles.splashManageHeader}>
                        <div className={styles.splashIcon}><FontAwesomeIcon icon={iconOption.icon} /></div>
                        <div className={styles.splashChip}>{card.category}</div>
                      </div>
                      <div className={styles.splashQuestion}>{card.question}</div>
                      <div className={styles.splashCardActions}>
                        <button type="button" className={styles.linkButton} onClick={() => editCard(card)}>Edit</button>
                        <button type="button" className={styles.linkButton} onClick={() => deleteCard(card.id)}>Delete</button>
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <EmptyState text="No splash cards configured yet. Add one on the left." />
            )}
          </div>
        </div>
      )}

      {settingsTab === 'licenses' && (
        <div className={styles.licenseCard}>
          <div className={styles.licenseSection}>
            <div className={styles.licenseSectionTitle}>Frontend tools and licenses</div>
            {FRONTEND_LICENSES.map(item => (
              <div className={styles.licenseRow} key={`frontend-${item.name}`}>
                <span>{item.name} {item.version}</span>
                <strong>{item.license}</strong>
              </div>
            ))}
          </div>
          <div className={styles.licenseSection}>
            <div className={styles.licenseSectionTitle}>Backend tools and licenses</div>
            {BACKEND_LICENSES.map(item => (
              <div className={styles.licenseRow} key={`backend-${item.name}`}>
                <span>{item.name} {item.version}</span>
                <strong>{item.license}</strong>
              </div>
            ))}
          </div>
          <div className={styles.licenseMeta}>Frontend versions reflect npm workspace packages. Backend versions reflect requirements.txt constraints.</div>
        </div>
      )}
    </>
  )
}

export default function AdminPanel() {
  const [token, setToken] = useState(localStorage.getItem('spark_admin_token') || '')
  const [stats, setStats] = useState(null)
  const [statusData, setStatusData] = useState(null)
  const [indexHealth, setIndexHealth] = useState(null)
  const [settings, setSettings] = useState(() => loadPersistedSettings())
  const [error, setError] = useState(null)
  const [indexHealthError, setIndexHealthError] = useState(null)
  const [loading, setLoading] = useState(false)
  const [activeTab, setActiveTab] = useState('search')
  const [settingsTab, setSettingsTab] = useState('overview')
  const [search, setSearch] = useState('')
  const [department, setDepartment] = useState('all')
  const [feedback, setFeedback] = useState('all')
  const [answered, setAnswered] = useState('all')
  const [knowledgeSearch, setKnowledgeSearch] = useState('')
  const [knowledgeDepartment, setKnowledgeDepartment] = useState('all')
  const [knowledgeIssue, setKnowledgeIssue] = useState('all')
  const [knowledgeFileType, setKnowledgeFileType] = useState('all')
  const [knowledgeUsage, setKnowledgeUsage] = useState('all')
  const [knowledgeReingestPath, setKnowledgeReingestPath] = useState('')
  const [knowledgeReingestMessage, setKnowledgeReingestMessage] = useState('')
  const [knowledgeReingestError, setKnowledgeReingestError] = useState('')
  const [knowledgeRemovePath, setKnowledgeRemovePath] = useState('')
  const [knowledgeRemoveMessage, setKnowledgeRemoveMessage] = useState('')
  const [knowledgeRemoveError, setKnowledgeRemoveError] = useState('')
  const [overviewPurgeRunning, setOverviewPurgeRunning] = useState(false)
  const [overviewReingestRunning, setOverviewReingestRunning] = useState(false)
  const [overviewPurgeResult, setOverviewPurgeResult] = useState(null)
  const [overviewReingestResult, setOverviewReingestResult] = useState(null)

  useEffect(() => {
    try {
      localStorage.setItem(ADMIN_SETTINGS_STORAGE_KEY, JSON.stringify(settings))
      window.dispatchEvent(new CustomEvent('spark-settings-updated', { detail: settings }))
    } catch {
      // ignore local storage failures
    }
  }, [settings])

  const adminThemeVars = useMemo(() => buildThemeVars(settings), [settings])

  const logout = useCallback(() => {
    localStorage.removeItem('spark_admin_token')
    setToken('')
    setStats(null)
    setStatusData(null)
    setIndexHealth(null)
    setIndexHealthError(null)
    setKnowledgeSearch('')
    setKnowledgeDepartment('all')
    setKnowledgeIssue('all')
    setKnowledgeFileType('all')
    setKnowledgeUsage('all')
    setKnowledgeReingestPath('')
    setKnowledgeReingestMessage('')
    setKnowledgeReingestError('')
    setKnowledgeRemovePath('')
    setKnowledgeRemoveMessage('')
    setKnowledgeRemoveError('')
    setOverviewPurgeRunning(false)
    setOverviewReingestRunning(false)
    setOverviewPurgeResult(null)
    setOverviewReingestResult(null)
  }, [])

  const resetSettings = useCallback(() => {
    setSettings(DEFAULT_SETTINGS)
  }, [])

  const fetchData = useCallback(async (authToken) => {
    setLoading(true)
    setError(null)
    setIndexHealthError(null)
    try {
      const [analyticsRes, statusRes, indexHealthRes] = await Promise.all([
        fetch(`${API}/admin/analytics`, { headers: { 'X-Spark-Token': authToken } }),
        fetch(`${API}/status`),
        fetch(`${API}/admin/index-health`, { headers: { 'X-Spark-Token': authToken } }),
      ])

      if (analyticsRes.status === 401) {
        logout()
        return
      }
      if (!analyticsRes.ok) {
        setError(`Server error: ${analyticsRes.status}`)
        return
      }

      const analytics = await analyticsRes.json()
      const status = statusRes.ok ? await statusRes.json() : null
      let health = null
      if (indexHealthRes.ok) {
        health = await indexHealthRes.json()
        setIndexHealthError(null)
      } else if (indexHealthRes.status === 401) {
        logout()
        return
      } else {
        setIndexHealthError(`Index health unavailable: ${indexHealthRes.status}`)
      }

      setStats(analytics)
      setStatusData(status)
      setIndexHealth(health)
      setToken(authToken)
      localStorage.setItem('spark_admin_token', authToken)
    } catch (e) {
      console.error(e)
      setError('Could not connect to Spark API.')
    } finally {
      setLoading(false)
    }
  }, [logout])

  const handleKnowledgeReingest = useCallback(async (sourcePath) => {
    if (!sourcePath || !token) return
    setKnowledgeReingestError('')
    setKnowledgeReingestMessage('')
    setKnowledgeRemoveError('')
    setKnowledgeRemoveMessage('')
    setKnowledgeReingestPath(sourcePath)
    try {
      const res = await fetch(`${API}/admin/reingest`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Spark-Token': token,
        },
        body: JSON.stringify({ source_path: sourcePath }),
      })
      if (res.status === 401) {
        logout()
        return
      }
      const payload = await res.json().catch(() => null)
      if (!res.ok) {
        if (res.status === 409 && payload?.status === 'busy') {
          setKnowledgeReingestError(payload.message)
        } else {
          setKnowledgeReingestError(payload?.detail || payload?.message || `Re-ingest failed: ${res.status}`)
        }
        return
      }
      setKnowledgeReingestMessage(payload?.message || `Re-ingest started for ${sourcePath}`)
      // Trigger immediate refresh to show "Re-ingesting..." state via ingestion_active
      fetchData(token)
    } catch (err) {
      setKnowledgeReingestError('Could not start re-ingest.')
    } finally {
      setKnowledgeReingestPath('')
    }
  }, [token, logout])

  const handleKnowledgeRemove = useCallback(async (sourcePath) => {
    if (!sourcePath || !token) return
    const confirmed = window.confirm(`Remove this file from the knowledge index?\n\n${sourcePath}\n\nThis removes chunks and vector data.`)
    if (!confirmed) return

    setKnowledgeRemoveError('')
    setKnowledgeRemoveMessage('')
    setKnowledgeReingestError('')
    setKnowledgeReingestMessage('')
    setKnowledgeRemovePath(sourcePath)
    try {
      const res = await fetch(`${API}/admin/knowledge/remove`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Spark-Token': token,
        },
        body: JSON.stringify({ source_path: sourcePath }),
      })
      if (res.status === 401) {
        logout()
        return
      }
      const payload = await res.json().catch(() => null)
      if (!res.ok) {
        setKnowledgeRemoveError(payload?.detail || `Remove failed: ${res.status}`)
        return
      }
      setKnowledgeRemoveMessage(payload?.message || `Removed ${sourcePath}`)
      await fetchData(token)
    } catch (err) {
      setKnowledgeRemoveError('Could not remove file from knowledge index.')
    } finally {
      setKnowledgeRemovePath('')
    }
  }, [token, logout, fetchData])

  const handlePurgeIndex = useCallback(async () => {
    if (!token || overviewPurgeRunning || overviewReingestRunning) return
    const confirmed = window.confirm(
      'This deletes all indexed vector and chunk data. Source files in intake are not deleted.'
    )
    if (!confirmed) return

    setOverviewPurgeRunning(true)
    setOverviewPurgeResult(null)
    setOverviewReingestResult(null)
    try {
      const res = await fetch(`${API}/admin/purge-index`, {
        method: 'POST',
        headers: { 'X-Spark-Token': token },
      })
      if (res.status === 401) {
        logout()
        return
      }
      const payload = await res.json().catch(() => null)
      if (!res.ok) {
        setOverviewPurgeResult({
          ok: false,
          message: payload?.detail || payload?.message || `Purge failed: ${res.status}`,
        })
        return
      }
      setOverviewPurgeResult({
        ok: true,
        message: payload?.message || 'Index purge completed.',
      })
      await fetchData(token)
    } catch {
      setOverviewPurgeResult({ ok: false, message: 'Could not purge index data.' })
    } finally {
      setOverviewPurgeRunning(false)
    }
  }, [token, overviewPurgeRunning, overviewReingestRunning, logout, fetchData])

  const handleReingestAll = useCallback(async () => {
    if (!token || overviewPurgeRunning || overviewReingestRunning) return
    setOverviewReingestRunning(true)
    setOverviewReingestResult(null)
    setOverviewPurgeResult(null)
    try {
      const res = await fetch(`${API}/admin/reingest-all`, {
        method: 'POST',
        headers: { 'X-Spark-Token': token },
      })
      if (res.status === 401) {
        logout()
        return
      }
      const payload = await res.json().catch(() => null)
      if (!res.ok) {
        setOverviewReingestResult({
          ok: false,
          message: payload?.detail || payload?.message || `Re-ingest failed: ${res.status}`,
        })
        return
      }
      const failedCount = Array.isArray(payload?.failed) ? payload.failed.length : 0
      const skippedCount = Array.isArray(payload?.skipped) ? payload.skipped.length : 0
      setOverviewReingestResult({
        ok: Boolean(payload?.ok),
        message: `${payload?.message || 'Re-ingest completed.'} Processed: ${payload?.documents_processed || 0}, Indexed: ${payload?.documents_indexed || 0}, Chunks: ${payload?.chunks_created || 0}, Failed/Unsupported: ${failedCount}, Skipped: ${skippedCount}.`,
      })
      await fetchData(token)
    } catch {
      setOverviewReingestResult({ ok: false, message: 'Could not run re-ingest.' })
    } finally {
      setOverviewReingestRunning(false)
    }
  }, [token, overviewPurgeRunning, overviewReingestRunning, logout, fetchData])

  useEffect(() => {
    if (token) fetchData(token)
  }, [token, fetchData])

  useEffect(() => {
    if (!token || !indexHealth?.ingestion_active) return
    const timer = setInterval(() => fetchData(token), 4000)
    return () => clearInterval(timer)
  }, [token, indexHealth?.ingestion_active, fetchData])

  const recentLogs = stats?.recent_logs || []

  if (!token) {
    return (
      <div className={styles.loginGate}>
        <div className={styles.loginCard}>
          <div className={styles.loginTitle}>Admin</div>
          <div className={styles.loginSubtitle}>Enter the admin token to view logs, analytics, departments, and settings.</div>
          <input
            autoFocus
            type="password"
            className={styles.loginInput}
            placeholder="Admin token"
            value={token}
            onChange={e => setToken(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter' && token.trim()) fetchData(token.trim())
            }}
          />
          <button
            type="button"
            className={styles.loginButton}
            onClick={() => fetchData(token.trim())}
            disabled={!token.trim()}
          >
            Open
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className={styles.root} style={adminThemeVars}>
      <header className={styles.header}>
        <div className={styles.headerTitle}>Spark</div>
        <div className={styles.headerOrg}>Renasant</div>
      </header>

      <div className={styles.layout}>
        <aside className={styles.sidebar}>
          {TABS.map(tab => (
            <button
              key={tab.id}
              type="button"
              className={`${styles.sidebarLink} ${activeTab === tab.id ? styles.sidebarLinkActive : ''}`}
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.label}
            </button>
          ))}
          <button type="button" className={styles.sidebarLink} onClick={() => { window.location.href = window.location.pathname || '/' }}>
            Back to bot
          </button>
        </aside>

        <main className={styles.main}>
          {loading && !stats && <div className={styles.state}>Connecting to analytics engine...</div>}
          {error && <div className={styles.errorState}>{error}</div>}

          {stats && (
            <>
              {activeTab === 'search' && (
                <SearchLogsPage
                  logs={recentLogs}
                  statusData={statusData}
                  search={search}
                  setSearch={setSearch}
                  department={department}
                  setDepartment={setDepartment}
                  feedback={feedback}
                  setFeedback={setFeedback}
                  answered={answered}
                  setAnswered={setAnswered}
              />
            )}

              {activeTab === 'analytics' && <AnalyticsPage stats={stats} indexHealth={indexHealth} indexHealthError={indexHealthError} />}

              {activeTab === 'knowledge' && (
                <KnowledgePage
                  statusData={statusData}
                  stats={stats}
                  indexHealth={indexHealth}
                  search={knowledgeSearch}
                  setSearch={setKnowledgeSearch}
                  department={knowledgeDepartment}
                  setDepartment={setKnowledgeDepartment}
                  issue={knowledgeIssue}
                  setIssue={setKnowledgeIssue}
                  fileType={knowledgeFileType}
                  setFileType={setKnowledgeFileType}
                  usage={knowledgeUsage}
                  setUsage={setKnowledgeUsage}
                  onReingest={handleKnowledgeReingest}
                  onRemove={handleKnowledgeRemove}
                  reingestPath={knowledgeReingestPath}
                  removePath={knowledgeRemovePath}
                  reingestError={knowledgeReingestError}
                  reingestMessage={knowledgeReingestMessage}
                  removeError={knowledgeRemoveError}
                  removeMessage={knowledgeRemoveMessage}
                />
              )}

              {activeTab === 'departments' && <DepartmentsPage statusData={statusData} />}

              {activeTab === 'settings' && (
                <SettingsPage
                  logs={recentLogs}
                  settingsTab={settingsTab}
                  setSettingsTab={setSettingsTab}
                  settings={settings}
                  setSettings={setSettings}
                  onResetSettings={resetSettings}
                  onPurgeIndex={handlePurgeIndex}
                  onReingestAll={handleReingestAll}
                  purgeRunning={overviewPurgeRunning}
                  reingestRunning={overviewReingestRunning}
                  purgeResult={overviewPurgeResult}
                  reingestResult={overviewReingestResult}
                />
              )}
            </>
          )}
        </main>
      </div>
    </div>
  )
}
