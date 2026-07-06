/**
 * script.js — Case Report Generator frontend logic
 *
 * Responsibilities:
 *  - Screenshot management: drag-drop, paste (Ctrl+V), file picker, preview grid
 *  - Form validation
 *  - Report generation (POST /generate-report with progress animation)
 *  - Result & error display
 *  - Reports table loading and delete
 *  - Toast notifications
 *  - Copy URL and Open Report buttons
 */

/* ════════════════════════════════════════════════════════════════════
   State
   ════════════════════════════════════════════════════════════════════ */
const state = {
  files: [],          // Array of { file: File, objectUrl: string }
  pdfUrl: null,
  generating: false,
};

/* ════════════════════════════════════════════════════════════════════
   DOM refs
   ════════════════════════════════════════════════════════════════════ */
const dom = {
  sourceUrl:         () => document.getElementById('sourceUrl'),
  dropZone:          () => document.getElementById('dropZone'),
  screenshotInput:   () => document.getElementById('screenshotInput'),
  previewGrid:       () => document.getElementById('previewGrid'),
  generateBtn:       () => document.getElementById('generateBtn'),
  resetBtn:          () => document.getElementById('resetBtn'),
  progressSection:   () => document.getElementById('progressSection'),
  progressBar:       () => document.getElementById('progressBar'),
  progressLabel:     () => document.getElementById('progressLabel'),
  resultSection:     () => document.getElementById('resultSection'),
  resultMeta:        () => document.getElementById('resultMeta'),
  resultUrl:         () => document.getElementById('resultUrl'),
  copyBtn:           () => document.getElementById('copyBtn'),
  openBtn:           () => document.getElementById('openBtn'),
  errorSection:      () => document.getElementById('errorSection'),
  errorMsg:          () => document.getElementById('errorMsg'),
  reportsTableBody:  () => document.getElementById('reportsTableBody'),
  refreshReportsBtn: () => document.getElementById('refreshReportsBtn'),
  urlError:          () => document.getElementById('urlError'),
  ssError:           () => document.getElementById('ssError'),
};

/* ════════════════════════════════════════════════════════════════════
   Toast helper
   ════════════════════════════════════════════════════════════════════ */
function showToast(message, type = 'info') {
  const id = 'toast-' + Date.now();
  const iconMap = {
    success: 'bi-check-circle-fill text-success',
    error:   'bi-exclamation-circle-fill text-danger',
    info:    'bi-info-circle-fill text-primary',
  };
  const icon = iconMap[type] || iconMap.info;

  const container = document.querySelector('.toast-container')
    || (() => {
      const c = document.createElement('div');
      c.className = 'toast-container position-fixed bottom-0 end-0 p-3';
      document.body.appendChild(c);
      return c;
    })();

  container.insertAdjacentHTML('beforeend', `
    <div id="${id}" class="toast align-items-center" role="alert" aria-live="assertive">
      <div class="d-flex align-items-center gap-2 p-3">
        <i class="bi ${icon}"></i>
        <span class="me-auto small">${escapeHtml(message)}</span>
        <button type="button" class="btn-close btn-close-white btn-close-sm ms-2"
                data-bs-dismiss="toast"></button>
      </div>
    </div>
  `);

  const toastEl = document.getElementById(id);
  const toast = new bootstrap.Toast(toastEl, { delay: 3500 });
  toast.show();
  toastEl.addEventListener('hidden.bs.toast', () => toastEl.remove());
}

/* ════════════════════════════════════════════════════════════════════
   Utility
   ════════════════════════════════════════════════════════════════════ */
function escapeHtml(str) {
  const d = document.createElement('div');
  d.appendChild(document.createTextNode(str));
  return d.innerHTML;
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i];
}

function formatDate(iso) {
  const d = new Date(iso);
  return isNaN(d) ? '—' : d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
}

const ALLOWED_TYPES = ['image/png', 'image/jpeg', 'image/jpg', 'image/webp'];

function isValidImage(file) {
  return ALLOWED_TYPES.includes(file.type);
}

/* ════════════════════════════════════════════════════════════════════
   Preview grid management
   ════════════════════════════════════════════════════════════════════ */
function addFiles(newFiles) {
  const MAX_TOTAL_MB = 25;
  let totalBytes = state.files.reduce((s, f) => s + f.file.size, 0);

  for (const file of newFiles) {
    if (!isValidImage(file)) {
      showToast(`"${file.name}" is not a supported image type.`, 'error');
      continue;
    }
    if (totalBytes + file.size > MAX_TOTAL_MB * 1024 * 1024) {
      showToast('Total upload size exceeds 25 MB. Some files were skipped.', 'error');
      break;
    }
    const objectUrl = URL.createObjectURL(file);
    state.files.push({ file, objectUrl });
    totalBytes += file.size;
  }
  renderPreviews();
}

function removeFile(index) {
  URL.revokeObjectURL(state.files[index].objectUrl);
  state.files.splice(index, 1);
  renderPreviews();
}

function renderPreviews() {
  const grid = dom.previewGrid();
  grid.innerHTML = '';

  if (state.files.length === 0) return;

  state.files.forEach(({ objectUrl }, i) => {
    const thumb = document.createElement('div');
    thumb.className = 'preview-thumb';
    thumb.innerHTML = `
      <img src="${objectUrl}" alt="Screenshot ${i + 1}" loading="lazy" />
      <span class="thumb-num">${i + 1}</span>
      <button
        class="remove-btn"
        title="Remove"
        data-index="${i}"
        aria-label="Remove screenshot ${i + 1}"
      >
        <i class="bi bi-x"></i>
      </button>
    `;
    thumb.querySelector('.remove-btn').addEventListener('click', (e) => {
      e.stopPropagation();
      removeFile(parseInt(e.currentTarget.dataset.index));
    });
    grid.appendChild(thumb);
  });
}

/* ════════════════════════════════════════════════════════════════════
   Drop zone event wiring
   ════════════════════════════════════════════════════════════════════ */
function initDropZone() {
  const zone = dom.dropZone();
  const input = dom.screenshotInput();

  zone.addEventListener('click', () => input.click());
  zone.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); input.click(); }
  });

  // File picker
  input.addEventListener('change', () => {
    addFiles(Array.from(input.files));
    input.value = '';   // allow re-selecting same file
  });

  // Drag-over highlight
  zone.addEventListener('dragover', (e) => {
    e.preventDefault();
    zone.classList.add('drag-over');
  });
  zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));

  // Drop
  zone.addEventListener('drop', (e) => {
    e.preventDefault();
    zone.classList.remove('drag-over');
    const files = Array.from(e.dataTransfer.files);
    if (files.length) addFiles(files);
  });

  // Paste (Ctrl+V) anywhere on the page
  document.addEventListener('paste', (e) => {
    const items = Array.from(e.clipboardData.items || []);
    const imageItems = items.filter(item => item.kind === 'file' && item.type.startsWith('image/'));
    if (imageItems.length) {
      addFiles(imageItems.map(i => i.getAsFile()).filter(Boolean));
    }
  });
}

/* ════════════════════════════════════════════════════════════════════
   Progress bar animation
   ════════════════════════════════════════════════════════════════════ */
let _progressInterval = null;

function startProgress() {
  const bar = dom.progressBar();
  bar.style.width = '5%';
  let val = 5;
  _progressInterval = setInterval(() => {
    // Ease toward 85% — never reach 100% until done
    const remaining = 85 - val;
    val += remaining * 0.06;
    bar.style.width = val + '%';
  }, 300);
}

function finishProgress() {
  clearInterval(_progressInterval);
  dom.progressBar().style.width = '100%';
}

/* ════════════════════════════════════════════════════════════════════
   UI section toggles
   ════════════════════════════════════════════════════════════════════ */
function hideAll() {
  ['progressSection', 'resultSection', 'errorSection'].forEach(id => {
    document.getElementById(id).classList.add('d-none');
  });
}

function showProgress(label = 'Generating report…') {
  hideAll();
  dom.progressSection().classList.remove('d-none');
  dom.progressLabel().textContent = label;
  startProgress();
}

function showResult(data) {
  finishProgress();
  hideAll();
  state.pdfUrl = data.pdf_url;
  dom.resultMeta().textContent =
    `${data.filename}  ·  ${data.pages} page${data.pages !== 1 ? 's' : ''}`;
  dom.resultUrl().textContent = data.pdf_url;
  dom.resultSection().classList.remove('d-none');
}

function showError(message) {
  finishProgress();
  hideAll();
  dom.errorMsg().textContent = message;
  dom.errorSection().classList.remove('d-none');
}

/* ════════════════════════════════════════════════════════════════════
   Validation
   ════════════════════════════════════════════════════════════════════ */
function validate() {
  let ok = true;

  const urlEl = dom.sourceUrl();
  const urlErr = dom.urlError();
  if (!urlEl.value.trim()) {
    urlErr.classList.remove('d-none');
    urlEl.classList.add('is-invalid');
    ok = false;
  } else {
    urlErr.classList.add('d-none');
    urlEl.classList.remove('is-invalid');
  }

  const ssErr = dom.ssError();
  if (state.files.length === 0) {
    ssErr.classList.remove('d-none');
    ok = false;
  } else {
    ssErr.classList.add('d-none');
  }

  return ok;
}

/* ════════════════════════════════════════════════════════════════════
   Generate Report
   ════════════════════════════════════════════════════════════════════ */
async function generateReport() {
  if (state.generating) return;
  if (!validate()) return;

  state.generating = true;
  dom.generateBtn().disabled = true;

  const formData = new FormData();
  formData.append('source_url', dom.sourceUrl().value.trim());
  state.files.forEach(({ file }) => formData.append('screenshots[]', file));

  showProgress('Generating PDF…');

  try {
    const response = await fetch('/generate-report', {
      method: 'POST',
      body: formData,
    });

    const data = await response.json();

    if (!response.ok || data.status !== 'success') {
      throw new Error(data.message || `Server error ${response.status}`);
    }

    showResult(data);
    showToast('Report generated successfully!', 'success');
    loadReports();    // refresh table
  } catch (err) {
    console.error('Generate error:', err);
    showError(err.message || 'Network error. Please try again.');
    showToast(err.message || 'Failed to generate report.', 'error');
  } finally {
    state.generating = false;
    dom.generateBtn().disabled = false;
  }
}

/* ════════════════════════════════════════════════════════════════════
   Reset
   ════════════════════════════════════════════════════════════════════ */
function resetForm() {
  dom.sourceUrl().value = '';
  dom.urlError().classList.add('d-none');
  dom.ssError().classList.add('d-none');
  dom.sourceUrl().classList.remove('is-invalid');

  // Revoke object URLs to free memory
  state.files.forEach(f => URL.revokeObjectURL(f.objectUrl));
  state.files = [];
  state.pdfUrl = null;
  renderPreviews();

  hideAll();
}

/* ════════════════════════════════════════════════════════════════════
   Copy / Open buttons
   ════════════════════════════════════════════════════════════════════ */
function initResultButtons() {
  dom.copyBtn().addEventListener('click', () => {
    if (!state.pdfUrl) return;
    navigator.clipboard.writeText(state.pdfUrl)
      .then(() => showToast('URL copied to clipboard!', 'success'))
      .catch(() => showToast('Could not copy to clipboard.', 'error'));
  });

  dom.openBtn().addEventListener('click', () => {
    if (state.pdfUrl) window.open(state.pdfUrl, '_blank', 'noopener');
  });
}

/* ════════════════════════════════════════════════════════════════════
   Reports table
   ════════════════════════════════════════════════════════════════════ */
async function loadReports() {
  const tbody = dom.reportsTableBody();
  tbody.innerHTML = `
    <tr>
      <td colspan="6" class="text-center text-muted py-4">
        <div class="spinner-border spinner-border-sm text-accent me-2"></div> Loading…
      </td>
    </tr>
  `;

  try {
    const res = await fetch('/reports');
    const data = await res.json();

    if (!res.ok || data.status !== 'success') {
      throw new Error(data.message || 'Failed to load reports.');
    }

    const reports = data.reports || [];
    if (reports.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="6" class="text-center text-muted py-4">
            <i class="bi bi-inbox me-2"></i>No reports yet.
          </td>
        </tr>
      `;
      return;
    }

    tbody.innerHTML = reports.map(r => `
      <tr>
        <td class="filename-cell" title="${escapeHtml(r.filename)}">${escapeHtml(r.filename)}</td>
        <td>
          <a
            href="${escapeHtml(r.source_url)}"
            target="_blank"
            rel="noopener noreferrer"
            class="url-cell text-decoration-none"
            style="max-width:180px; display:block; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;"
            title="${escapeHtml(r.source_url)}"
          >${escapeHtml(r.source_url)}</a>
        </td>
        <td class="text-muted">${r.total_pages ?? '—'}</td>
        <td class="text-muted">${r.file_size ? formatBytes(r.file_size) : '—'}</td>
        <td class="text-muted" style="white-space:nowrap;">${formatDate(r.created_at)}</td>
        <td>
          <div class="d-flex gap-1 align-items-center">
            <a
              href="${escapeHtml(r.pdf_url)}"
              target="_blank"
              rel="noopener noreferrer"
              class="btn-icon"
              title="Open PDF"
            ><i class="bi bi-box-arrow-up-right"></i></a>
            <button
              class="btn-icon danger"
              title="Delete"
              data-id="${escapeHtml(r.id)}"
              data-filename="${escapeHtml(r.filename)}"
              onclick="deleteReport(this)"
            ><i class="bi bi-trash3"></i></button>
          </div>
        </td>
      </tr>
    `).join('');

  } catch (err) {
    tbody.innerHTML = `
      <tr>
        <td colspan="6" class="text-center text-danger py-4">
          <i class="bi bi-exclamation-triangle me-2"></i>${escapeHtml(err.message)}
        </td>
      </tr>
    `;
  }
}

async function deleteReport(btn) {
  const id = btn.dataset.id;
  const filename = btn.dataset.filename;
  if (!confirm(`Delete "${filename}"?\n\nThis will remove it from S3 and the database.`)) return;

  try {
    const res = await fetch(`/delete/${id}`, { method: 'DELETE' });
    const data = await res.json();
    if (!res.ok || data.status !== 'success') throw new Error(data.message);
    showToast('Report deleted.', 'success');
    loadReports();
  } catch (err) {
    showToast(err.message || 'Delete failed.', 'error');
  }
}

/* ════════════════════════════════════════════════════════════════════
   Init
   ════════════════════════════════════════════════════════════════════ */
document.addEventListener('DOMContentLoaded', () => {
  initDropZone();
  initResultButtons();

  dom.generateBtn().addEventListener('click', generateReport);
  dom.resetBtn().addEventListener('click', resetForm);
  dom.refreshReportsBtn().addEventListener('click', loadReports);

  // Initial table load
  loadReports();
});
