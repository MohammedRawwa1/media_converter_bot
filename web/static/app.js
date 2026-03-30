const uploadForm = document.getElementById('uploadForm')
const fileInput = document.getElementById('fileInput')
const jobsDiv = document.getElementById('jobs')

uploadForm.addEventListener('submit', async (e) => {
  e.preventDefault()
  if (!fileInput.files.length) return
  const file = fileInput.files[0]
  const fd = new FormData()
  fd.append('file', file)
  const res = await fetch('/upload', { method: 'POST', body: fd })
  if (!res.ok) {
    alert('upload failed')
    return
  }
  const data = await res.json()
  const jobId = data.job_id
  addJobCard(jobId, file.name)
})

function addJobCard(jobId, filename){
  const div = document.createElement('div')
  div.className = 'job'
  div.id = `job-${jobId}`
  div.innerHTML = `
    <div class="job-row">
      <div>
        <strong>${filename}</strong>
        <div class="meta" id="meta-${jobId}">Queued</div>
        <div class="meta size" id="size-${jobId}"></div>
      </div>
      <div id="action-${jobId}"></div>
    </div>
    <div class="progress"><div class="bar" id="bar-${jobId}" style="width:0%"></div></div>
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

  const scheme = (location.protocol === 'https:') ? 'wss' : 'ws'
  const wsPort = (window.WS_PORT) ? window.WS_PORT : 6789
  const wsUrl = `${scheme}://${location.hostname}:${wsPort}/ws/${jobId}`

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

      // update single progress bar (represents size-based when available)
      bar.style.width = `${clampedPct}%`
      bar.textContent = `${clampedPct}%`

      // human-readable label: show both size/time when both exist
      let pctLabel = `${clampedPct}%`
      if (progressSize !== null && progressTime !== null) {
        pctLabel = `${clampedPct}% (size) / ${progressTime}% (time)`
      } else if (progressSize !== null) {
        pctLabel = `${clampedPct}% (size)`
      } else {
        pctLabel = `${progressTime}% (time)`
      }

      meta.textContent = `${j.message} • ${pctLabel}`

      // size info if available (human readable)
      const sizeEl = document.getElementById(`size-${jobId}`)
      if (j.out_bytes != null && j.in_bytes != null){
        const outB = Number(j.out_bytes) || 0
        const inB = Number(j.in_bytes) || 0
        const human = humanFileSize(outB)
        const total = humanFileSize(inB)
        sizeEl.textContent = `${human} / ${total}`
      } else if (j.in_bytes != null) {
        const inB = Number(j.in_bytes) || 0
        sizeEl.textContent = `Total: ${humanFileSize(inB)}`
      } else {
        sizeEl.textContent = ''
      }
      if (j.status === 'done' && j.output){
        action.innerHTML = `<a href="/download/${jobId}" class="btn">Download</a>`
        finished = true
        break
      }
      if (j.status === 'error'){
        meta.textContent = 'Error: ' + j.message
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

  const progressTime = j.progress != null ? Number(j.progress) : 0
  const progressSize = j.progress_by_size != null ? Number(j.progress_by_size) : null
  const shownPct = progressSize !== null ? progressSize : progressTime
  const clampedPct = Math.max(0, Math.min(100, isFinite(shownPct) ? shownPct : 0))

  bar.style.width = `${clampedPct}%`

  // bar text: show size/time pair when available
  if (progressSize !== null && progressTime !== null) {
    bar.textContent = `${progressSize}% / ${progressTime}%`
  } else if (progressSize !== null) {
    bar.textContent = `${progressSize}%`
  } else {
    bar.textContent = `${progressTime}%`
  }

  let pctLabel
  if (progressSize !== null && progressTime !== null) {
    pctLabel = `${progressSize}% (size) / ${progressTime}% (time)`
  } else if (progressSize !== null) {
    pctLabel = `${progressSize}% (size)`
  } else {
    pctLabel = `${progressTime}% (time)`
  }

  meta.textContent = `${j.message} • ${pctLabel}`

  const sizeEl = document.getElementById(`size-${jobId}`)
  if (j.out_bytes != null && j.in_bytes != null){
    const outB = Number(j.out_bytes) || 0
    const inB = Number(j.in_bytes) || 0
    sizeEl.textContent = `${humanFileSize(outB)} / ${humanFileSize(inB)}`
  } else if (j.in_bytes != null){
    sizeEl.textContent = `Total: ${humanFileSize(Number(j.in_bytes)||0)}`
  }

  if (j.status === 'done' && j.output){
    action.innerHTML = `<a href="/download/${jobId}" class="btn">Download</a>`
    try{ if (eventSources[jobId]) eventSources[jobId].close() }catch(e){}
    delete eventSources[jobId]
    try{ if (wsConnections[jobId]) wsConnections[jobId].close() }catch(e){}
    delete wsConnections[jobId]
  }

  if (j.status === 'error'){
    meta.textContent = 'Error: ' + j.message
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
