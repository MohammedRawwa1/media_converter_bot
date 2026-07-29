const uploadForm = document.getElementById('uploadForm')
const fileInput = document.getElementById('fileInput')
const jobsDiv = document.getElementById('jobs')

uploadForm.addEventListener('submit', async (e) => {
  e.preventDefault()
  if (!fileInput.files.length) return
  const file = fileInput.files[0]
  const fd = new FormData()
  fd.append('file', file)
  const tokenInput = document.getElementById('uploadTokenInput')
  if (tokenInput && tokenInput.value) {
    fd.append('upload_token', tokenInput.value.trim())
  }
  const res = await fetch('/upload', { method: 'POST', body: fd })
  if (!res.ok) {
    alert('upload failed')
    return
  }
  const data = await res.json()
  const jobId = data.job_id
  addJobCard(jobId, file.name, file.size)
})

function addJobCard(jobId, filename, fileSizeBytes){
  const div = document.createElement('div')
  div.className = 'job'
  div.id = `job-${jobId}`
  div.innerHTML = `
    <div class="job-row">
      <div>
        <strong>${filename}</strong>
        <div class="meta" id="meta-${jobId}">Queued</div>
        <div class="meta size" id="size-${jobId}">${fileSizeBytes ? humanFileSize(fileSizeBytes) : ''}</div>
      </div>
      <div id="action-${jobId}"></div>
    </div>
    <div class="progress"><div class="bar bar-upload" id="bar-${jobId}" style="width:0%"></div></div>
  `
  jobsDiv.prepend(div)
  // Try WebSocket first (with reconnect/backoff). If not available or fails,
  // fall back to Server-Sent Events, then to polling.
  const wsStarted = initWebSocket(jobId)
  if (!wsStarted) {
    if (window.EventSource) {
      const es = initEventSource(jobId)
      if (!es) {
        pollStatus(jobId)
      }
    } else {
      pollStatus(jobId)
    }
  }
}

const eventSources = {}
const wsConnections = {}

function initWebSocket(jobId){
  // Returns true if a websocket connection attempt was started.
  if (!window.WebSocket) return false

  let attempts = 0
  let ws = null

  // Use the same host and port as the page - no separate WS_PORT needed.
  // In production (Railway, etc.) only one port is exposed, so we connect
  // via the main app's path-based WebSocket endpoint.
  const scheme = (location.protocol === 'https:') ? 'wss' : 'ws'
  const wsUrl = `${scheme}://${location.host}/ws/${jobId}`

  const connect = () => {
    try{
      ws = new WebSocket(wsUrl)
      ws.onopen = () => {
        attempts = 0
        wsConnections[jobId] = ws
      }
      ws.onmessage = (e) => {
        try{
          const payload = JSON.parse(e.data)
          handleUpdate(payload, jobId)
        }catch(err){ }
      }
      ws.onclose = () => {
        if (wsConnections[jobId] === ws) delete wsConnections[jobId]
        // schedule reconnect with backoff
        attempts += 1
        const base = 1000
        const max = 30000
        const timeout = Math.min(max, Math.floor(base * Math.pow(1.5, attempts)))
        setTimeout(() => connect(), timeout + Math.floor(Math.random() * 300))
      }
      ws.onerror = () => { try{ ws.close() }catch(e){} }
      return true
    }catch(err){
      return false
    }
  }

  // start first connect attempt
  return connect()
}

function initEventSource(jobId){
  try{
    const es = new EventSource(`/events/${jobId}`)
    eventSources[jobId] = es
    es.onmessage = (e) => {
      try{
        const payload = JSON.parse(e.data)
        handleUpdate(payload, jobId)
      }catch(err){
        // ignore malformed
      }
    }
    es.onerror = (err) => {
      try{ es.close() }catch(e){}
      delete eventSources[jobId]
    }
    return es
  }catch(err){
    return null
  }
}

async function pollStatus(jobId){
  const meta = document.getElementById(`meta-${jobId}`)
  const bar = document.getElementById(`bar-${jobId}`)
  const action = document.getElementById(`action-${jobId}`)
  const sizeEl = document.getElementById(`size-${jobId}`)
  let finished = false
  while(!finished){
    // if we have an active EventSource or WebSocket for this job, stop polling
    if (eventSources[jobId] || wsConnections[jobId]) break
    try{
      const res = await fetch(`/status/${jobId}`)
      if (!res.ok) throw new Error('not found')
      const j = await res.json()

      // compute progress: prefer size-based when available, fallback to time-based
      const progressTime = j.progress != null ? Number(j.progress) : 0
      const progressSize = j.progress_by_size != null ? Number(j.progress_by_size) : null
      const shownPct = progressSize !== null ? progressSize : progressTime
      const clampedPct = Math.max(0, Math.min(100, isFinite(shownPct) ? shownPct : 0))

      const isUploadPhase = j.status === 'uploading' || (j.message && j.message.toLowerCase().includes('upload'))

      // Switch bar color based on phase
      if (isUploadPhase) {
        bar.className = 'bar bar-upload'
      } else if (j.status === 'done') {
        bar.className = 'bar bar-done'
      } else if (j.status === 'error') {
        bar.className = 'bar bar-error'
      } else {
        bar.className = 'bar bar-processing'
      }

      bar.style.width = `${clampedPct}%`
      bar.textContent = `${clampedPct}%`

      // Phase-aware label
      const phaseEmoji = isUploadPhase ? '📤' : '🎬'
      let pctLabel = `${phaseEmoji} ${clampedPct}%`

      meta.textContent = `${j.message} • ${pctLabel}`

      // Size info: show upload progress during upload phase
      if (isUploadPhase && j.in_bytes != null) {
        const inB = Number(j.in_bytes) || 0
        const sentB = Math.round(inB * (clampedPct / 100))
        sizeEl.textContent = `📤 ${humanFileSize(sentB)} / ${humanFileSize(inB)} uploaded`
      } else if (j.out_bytes != null && j.in_bytes != null){
        const outB = Number(j.out_bytes) || 0
        const inB = Number(j.in_bytes) || 0
        sizeEl.textContent = `📥 ${humanFileSize(inB)} → 📤 ${humanFileSize(outB)}`
      } else if (j.in_bytes != null) {
        sizeEl.textContent = `📦 ${humanFileSize(Number(j.in_bytes)||0)}`
      } else {
        sizeEl.textContent = ''
      }

      if (j.status === 'done' && j.output){
        meta.textContent = `${j.message} • ✅ Done`
        action.innerHTML = `<a href="/download/${jobId}" class="btn">Download</a>`
        finished = true
        break
      }
      if (j.status === 'error'){
        meta.textContent = '❌ Error: ' + j.message
        bar.className = 'bar bar-error'
        finished = true
        break
      }
    }catch(err){
      meta.textContent = 'Error polling status'
      finished = true
      break
    }
    await new Promise(r => setTimeout(r, 1000))
  }
}

function handleUpdate(j, jobId){
  const meta = document.getElementById(`meta-${jobId}`)
  const bar = document.getElementById(`bar-${jobId}`)
  const action = document.getElementById(`action-${jobId}`)
  const sizeEl = document.getElementById(`size-${jobId}`)

  const progressTime = j.progress != null ? Number(j.progress) : 0
  const progressSize = j.progress_by_size != null ? Number(j.progress_by_size) : null
  const shownPct = progressSize !== null ? progressSize : progressTime
  const clampedPct = Math.max(0, Math.min(100, isFinite(shownPct) ? shownPct : 0))

  const isUploadPhase = j.status === 'uploading' || (j.message && j.message.toLowerCase().includes('upload'))

  // Switch bar color based on phase: green for upload, blue for conversion
  if (isUploadPhase) {
    bar.className = 'bar bar-upload'
  } else {
    bar.className = 'bar bar-processing'
  }

  bar.style.width = `${clampedPct}%`

  // bar text: show size/time pair when available
  if (progressSize !== null && progressTime !== null) {
    bar.textContent = `${progressSize}% / ${progressTime}%`
  } else if (progressSize !== null) {
    bar.textContent = `${progressSize}%`
  } else {
    bar.textContent = `${progressTime}%`
  }

  // ── Phase-aware label ──
  const phaseEmoji = isUploadPhase ? '📤' : '🎬'
  let pctLabel
  if (progressSize !== null && progressTime !== null) {
    pctLabel = `${phaseEmoji} ${progressSize}% (size) / ${progressTime}% (time)`
  } else if (progressSize !== null) {
    pctLabel = `${phaseEmoji} ${progressSize}%`
  } else {
    pctLabel = `${phaseEmoji} ${progressTime}%`
  }
  meta.textContent = `${j.message} • ${pctLabel}`

  // ── Size info: show in_bytes during upload, out_bytes/in_bytes when done ──
  if (isUploadPhase && j.in_bytes != null) {
    const inB = Number(j.in_bytes) || 0
    const sentB = Math.round(inB * (clampedPct / 100))
    sizeEl.textContent = `📤 ${humanFileSize(sentB)} / ${humanFileSize(inB)} uploaded`
  } else if (j.out_bytes != null && j.in_bytes != null){
    const outB = Number(j.out_bytes) || 0
    const inB = Number(j.in_bytes) || 0
    sizeEl.textContent = `📥 ${humanFileSize(inB)} → 📤 ${humanFileSize(outB)}`
  } else if (j.in_bytes != null){
    sizeEl.textContent = `📦 ${humanFileSize(Number(j.in_bytes)||0)}`
  }

  if (j.status === 'done' && j.output){
    bar.className = 'bar bar-done'
    action.innerHTML = `<a href="/download/${jobId}" class="btn">Download</a>`
    try{ if (eventSources[jobId]) eventSources[jobId].close() }catch(e){}
    delete eventSources[jobId]
    try{ if (wsConnections[jobId]) wsConnections[jobId].close() }catch(e){}
    delete wsConnections[jobId]
  }

  if (j.status === 'error'){
    bar.className = 'bar bar-error'
    meta.textContent = '❌ Error: ' + j.message
    try{ if (eventSources[jobId]) eventSources[jobId].close() }catch(e){}
    delete eventSources[jobId]
    try{ if (wsConnections[jobId]) wsConnections[jobId].close() }catch(e){}
    delete wsConnections[jobId]
  }
}

function humanFileSize(bytes){
  if (bytes == null) return ''
  const thresh = 1024
  if (Math.abs(bytes) < thresh) return bytes + ' B'
  const units = ['KB','MB','GB','TB']
  let u = -1
  do { bytes /= thresh; ++u } while(Math.abs(bytes) >= thresh && u < units.length - 1)
  return bytes.toFixed(1) + ' ' + units[u]
}
