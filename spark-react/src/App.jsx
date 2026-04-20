import { useState, useEffect } from 'react'
import ChatApp from './components/ChatApp.jsx'
import ViewerApp from './components/ViewerApp.jsx'
import AdminPanel from './components/AdminPanel.jsx'

export default function App() {
  const [page, setPage] = useState('chat')
  const [viewerParams, setViewerParams] = useState(null)

  // Check if we're opened as the viewer (path or snippet in URL)
  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const path = params.get('path') || params.get('sourcePath')
    const view = params.get('view')

    if (path) {
      const rawPageNumber = params.get('pageNumber')
      const parsedPageNumber = rawPageNumber ? parseInt(rawPageNumber, 10) : null
      setViewerParams({
        path:       decodeURIComponent(path),
        sourcePath: decodeURIComponent(params.get('sourcePath') || ''),
        sourceName: decodeURIComponent(params.get('sourceName') || params.get('source') || ''),
        snippet:    decodeURIComponent(params.get('snippet') || ''),
        evidenceText: decodeURIComponent(params.get('evidenceText') || ''),
        chunkIndex: parseInt(params.get('chunk') || '0', 10),
        answer:     decodeURIComponent(params.get('answer') || ''),
        question:   decodeURIComponent(params.get('question') || ''),
        chunkText:  decodeURIComponent(params.get('chunkText') || ''),
        chunkId:    decodeURIComponent(params.get('chunkId') || ''),
        pageNumber: Number.isFinite(parsedPageNumber) ? parsedPageNumber : null,
        extractionMethod: decodeURIComponent(params.get('extractionMethod') || ''),
        hasTextLayer: params.get('hasTextLayer') === null ? null : params.get('hasTextLayer') === 'true' || params.get('hasTextLayer') === '1',
        ocrConfidence: params.get('ocrConfidence') ? Number(params.get('ocrConfidence')) : null,
      })
      setPage('viewer')
    } else if (view === 'admin') {
      setPage('admin')
    }
  }, [])

  if (page === 'admin') {
    return <AdminPanel />
  }

  if (page === 'viewer' && viewerParams) {
    return <ViewerApp
      path={viewerParams.path}
      sourcePath={viewerParams.sourcePath}
      sourceName={viewerParams.sourceName}
      snippet={viewerParams.snippet}
      evidenceText={viewerParams.evidenceText}
      chunkIndex={viewerParams.chunkIndex}
      answer={viewerParams.answer}
      question={viewerParams.question}
      chunkText={viewerParams.chunkText}
      chunkId={viewerParams.chunkId}
      pageNumber={viewerParams.pageNumber}
      extractionMethod={viewerParams.extractionMethod}
      hasTextLayer={viewerParams.hasTextLayer}
      ocrConfidence={viewerParams.ocrConfidence}
    />
  }

  return <ChatApp />
}
