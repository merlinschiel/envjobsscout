/**
 * GeoEco JobScout — Client-side application logic.
 *
 * Manages the Leaflet map, job table, sidebar filters, portfolio CRUD,
 * and scrape orchestration via the Flask REST API.
 */

// ---------------------------------------------------------------------------
//  State
// ---------------------------------------------------------------------------

const state = {
  jobs: [],
  filteredJobs: [],
  curated: [],
  stats: {},
  activeView: 'map',
  filters: {
    search: '',
    sources: [],
    skills: [],
    terms: [],
    locations: [],
    hideRemote: false,
  },
  sort: { field: 'Title', dir: 'asc' },
  map: null,
  markerCluster: null,
  markers: {},  // link -> marker lookup
};

// Per-term marker colors used on the map
const TERM_COLORS = {
  'geoökologie': '#d97706',
  'umweltwissenschaften': '#7c3aed',
  'hydrologie': '#2563eb',
  'naturschutz': '#0d9373',
  'klimaschutz': '#dc2626',
};

const SOURCE_COLORS = {
  'Greenjobs': '#0d9373',
  'Jobverde': '#7c3aed',
  'GoodJobs': '#d97706',
  'Manual': '#8e95a5',
};

// ---------------------------------------------------------------------------
//  Initialization
// ---------------------------------------------------------------------------

document.addEventListener('DOMContentLoaded', () => {
  initMap();
  loadJobs();
  loadCurated();
  loadStats();
  bindEvents();
});

function bindEvents() {
  document.querySelectorAll('.nav-btn').forEach(btn => {
    btn.addEventListener('click', () => switchView(btn.dataset.view));
  });

  document.getElementById('btn-toggle-sidebar').addEventListener('click', () => {
    document.getElementById('sidebar').classList.toggle('collapsed');
    // Map must recalculate size after sidebar animation finishes
    if (state.map) setTimeout(() => state.map.invalidateSize(), 300);
  });

  document.getElementById('search-input').addEventListener('input', e => {
    state.filters.search = e.target.value.toLowerCase();
    applyFilters();
  });

  document.getElementById('btn-scrape').addEventListener('click', () => {
    openModal('modal-scrape');
  });

  document.getElementById('btn-start-scrape').addEventListener('click', startScrape);

  document.getElementById('btn-add-manual').addEventListener('click', () => {
    openModal('modal-manual');
  });

  document.getElementById('btn-save-manual').addEventListener('click', saveManualEntry);

  document.querySelectorAll('.jobs-table th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const field = th.dataset.sort;
      if (state.sort.field === field) {
        state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
      } else {
        state.sort.field = field;
        state.sort.dir = 'asc';
      }
      document.querySelectorAll('.jobs-table th').forEach(h => h.classList.remove('sorted'));
      th.classList.add('sorted');
      th.querySelector('.sort-arrow').textContent = state.sort.dir === 'asc' ? '↑' : '↓';
      renderTable();
    });
  });

  document.getElementById('hide-remote').addEventListener('change', e => {
    state.filters.hideRemote = e.target.checked;
    applyFilters();
  });
}

// ---------------------------------------------------------------------------
//  View Switching
// ---------------------------------------------------------------------------

function switchView(view) {
  state.activeView = view;

  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.querySelector(`.nav-btn[data-view="${view}"]`).classList.add('active');

  document.querySelectorAll('.view-container').forEach(v => v.classList.remove('active'));
  document.getElementById(`view-${view}`).classList.add('active');

  if (view === 'map' && state.map) {
    setTimeout(() => state.map.invalidateSize(), 100);
  }
  if (view === 'portfolio') {
    loadCurated();
  }
}

// ---------------------------------------------------------------------------
//  Map
// ---------------------------------------------------------------------------

function initMap() {
  state.map = L.map('map', {
    zoomControl: true,
    preferCanvas: true,
  }).setView([52.52, 13.40], 7);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19,
  }).addTo(state.map);

  state.markerCluster = L.markerClusterGroup({
    maxClusterRadius: 50,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
    iconCreateFunction: cluster => {
      const count = cluster.getChildCount();
      let size = count < 10 ? 36 : count < 50 ? 44 : 52;
      return L.divIcon({
        html: `<div>${count}</div>`,
        className: 'custom-cluster-icon',
        iconSize: [size, size],
      });
    },
  });

  state.map.addLayer(state.markerCluster);
}

function renderMap() {
  state.markerCluster.clearLayers();
  state.markers = {};

  state.filteredJobs.forEach(job => {
    const lat = parseFloat(job.Lat);
    const lon = parseFloat(job.Lon);
    if (isNaN(lat) || isNaN(lon)) return;

    const firstTerm = (job.Term || '').split(',')[0].trim();
    const color = TERM_COLORS[firstTerm] || '#0d9373';

    const marker = L.circleMarker([lat, lon], {
      radius: 7,
      fillColor: color,
      color: 'rgba(0,0,0,0.15)',
      weight: 1,
      opacity: 1,
      fillOpacity: 0.85,
    });

    const isRemote = job.Remote === true || job.Remote === 'True' || String(job.Location).includes('Remote');
    const remoteTag = isRemote ? ' <span class="badge badge-remote">Remote</span>' : '';

    const popupHtml = `
      <div class="popup-title">${escapeHtml(job.Title)}</div>
      <div class="popup-detail">${escapeHtml(job.Company || 'Not specified')}</div>
      <div class="popup-detail">${escapeHtml(job.Location)}${remoteTag}</div>
      <div class="popup-detail">${escapeHtml(job.Skills || 'General')}</div>
      <div class="popup-detail">${escapeHtml(job.Term || '')}</div>
      <div class="popup-detail"><span class="badge badge-source ${(job.Source || '').toLowerCase()}">${escapeHtml(job.Source || '')}</span></div>
      <div class="popup-actions">
        <a href="${escapeHtml(job.Link)}" target="_blank" class="popup-btn secondary">Open Job ↗</a>
        <button class="popup-btn primary" onclick="saveToPortfolio('${escapeJS(job.Title)}', '${escapeJS(job.Company)}', '${escapeJS(job.Location)}', '${escapeJS(job.Link)}', '${escapeJS(job.Source)}', '${escapeJS(job.Skills)}')">Save</button>
      </div>
    `;

    marker.bindPopup(popupHtml, { maxWidth: 320, className: '' });
    state.markerCluster.addLayer(marker);
    state.markers[job.Link] = marker;
  });
}

// ---------------------------------------------------------------------------
//  Table
// ---------------------------------------------------------------------------

function renderTable() {
  const tbody = document.getElementById('jobs-tbody');
  let sorted = [...state.filteredJobs];

  const { field, dir } = state.sort;
  sorted.sort((a, b) => {
    const va = String(a[field] || '').toLowerCase();
    const vb = String(b[field] || '').toLowerCase();
    return dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
  });

  document.getElementById('table-count').textContent = `${sorted.length} results`;

  if (sorted.length === 0) {
    tbody.innerHTML = `
      <tr><td colspan="6">
        <div class="empty-state">
          <div class="empty-icon">&mdash;</div>
          <h3>No jobs found</h3>
          <p>Try adjusting your filters or run a new scrape.</p>
        </div>
      </td></tr>
    `;
    return;
  }

  tbody.innerHTML = sorted.map(job => {
    const isRemote = job.Remote === true || job.Remote === 'True' || String(job.Location).includes('Remote');
    const sourceClass = (job.Source || '').toLowerCase();

    const skillBadges = (job.Skills || 'General').split(',').map(s =>
      `<span class="badge badge-skill">${escapeHtml(s.trim())}</span>`
    ).join(' ');

    return `
      <tr data-link="${escapeHtml(job.Link)}" onclick="flyToJob('${escapeJS(job.Link)}')">
        <td class="job-title-cell" title="${escapeHtml(job.Title)}">
          ${escapeHtml(job.Title)}
          ${isRemote ? '<span class="badge badge-remote" style="margin-left:6px;">Remote</span>' : ''}
        </td>
        <td>${escapeHtml(job.Company || 'Not specified')}</td>
        <td>${escapeHtml(job.Location)}</td>
        <td><span class="badge badge-source ${sourceClass}">${escapeHtml(job.Source)}</span></td>
        <td>${skillBadges}</td>
        <td onclick="event.stopPropagation();">
          <a href="${escapeHtml(job.Link)}" target="_blank" class="job-link" style="margin-right:6px;">Open ↗</a>
          <button class="save-btn" onclick="saveToPortfolio('${escapeJS(job.Title)}', '${escapeJS(job.Company)}', '${escapeJS(job.Location)}', '${escapeJS(job.Link)}', '${escapeJS(job.Source)}', '${escapeJS(job.Skills)}')">Save</button>
        </td>
      </tr>
    `;
  }).join('');
}

function flyToJob(link) {
  const marker = state.markers[link];
  if (marker && state.activeView === 'map') {
    const latlng = marker.getLatLng();
    state.map.flyTo(latlng, 12, { duration: 0.8 });
    marker.openPopup();
  } else if (marker) {
    switchView('map');
    setTimeout(() => {
      const latlng = marker.getLatLng();
      state.map.flyTo(latlng, 12, { duration: 0.8 });
      setTimeout(() => marker.openPopup(), 900);
    }, 200);
  }
}

// ---------------------------------------------------------------------------
//  Filters
// ---------------------------------------------------------------------------

function buildFilters() {
  const sources = {}, skills = {}, terms = {}, locations = {};

  state.jobs.forEach(job => {
    const src = job.Source || 'Unknown';
    sources[src] = (sources[src] || 0) + 1;

    (job.Skills || 'General').split(',').forEach(s => {
      s = s.trim();
      if (s) skills[s] = (skills[s] || 0) + 1;
    });

    (job.Term || '').split(',').forEach(t => {
      t = t.trim();
      if (t) terms[t] = (terms[t] || 0) + 1;
    });

    const loc = job.Location || '';
    if (loc && loc !== 'Deutschland') {
      locations[loc] = (locations[loc] || 0) + 1;
    }
  });

  renderFilterGroup('source-filters', sources, 'sources');
  renderFilterGroup('skill-filters', skills, 'skills');
  renderFilterGroup('term-filters', terms, 'terms');

  // Show only the 15 most common locations
  const topLocs = Object.entries(locations)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .reduce((obj, [k, v]) => ({ ...obj, [k]: v }), {});
  renderFilterGroup('location-filters', topLocs, 'locations');
}

function renderFilterGroup(containerId, data, filterKey) {
  const container = document.getElementById(containerId);
  container.innerHTML = Object.entries(data)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => `
      <button class="filter-chip ${state.filters[filterKey].includes(name) ? 'active' : ''}"
              data-filter-key="${filterKey}" data-filter-value="${escapeHtml(name)}">
        ${escapeHtml(name)}
        <span class="count">${count}</span>
      </button>
    `).join('');

  container.querySelectorAll('.filter-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      const key = chip.dataset.filterKey;
      const val = chip.dataset.filterValue;
      const idx = state.filters[key].indexOf(val);
      if (idx >= 0) {
        state.filters[key].splice(idx, 1);
        chip.classList.remove('active');
      } else {
        state.filters[key].push(val);
        chip.classList.add('active');
      }
      applyFilters();
    });
  });
}

function applyFilters() {
  state.filteredJobs = state.jobs.filter(job => {
    if (state.filters.search) {
      const haystack = [job.Title, job.Company, job.Location, job.Skills, job.Term, job.Source]
        .join(' ').toLowerCase();
      if (!haystack.includes(state.filters.search)) return false;
    }

    if (state.filters.sources.length > 0) {
      if (!state.filters.sources.includes(job.Source)) return false;
    }

    if (state.filters.skills.length > 0) {
      const jobSkills = (job.Skills || '').split(',').map(s => s.trim());
      if (!state.filters.skills.some(s => jobSkills.includes(s))) return false;
    }

    if (state.filters.terms.length > 0) {
      const jobTerms = (job.Term || '').split(',').map(t => t.trim());
      if (!state.filters.terms.some(t => jobTerms.includes(t))) return false;
    }

    if (state.filters.locations.length > 0) {
      if (!state.filters.locations.includes(job.Location)) return false;
    }

    if (state.filters.hideRemote) {
      const isRemote = job.Remote === true || job.Remote === 'True' || String(job.Location).includes('Remote');
      if (isRemote) return false;
    }

    return true;
  });

  renderMap();
  renderTable();
}

// ---------------------------------------------------------------------------
//  Data Loading
// ---------------------------------------------------------------------------

async function loadJobs() {
  try {
    const res = await fetch('/api/jobs');
    const data = await res.json();
    state.jobs = data.jobs || [];
    state.filteredJobs = [...state.jobs];
    document.getElementById('stats-badge').textContent = `${state.jobs.length} jobs`;
    buildFilters();
    renderMap();
    renderTable();
  } catch (e) {
    console.error('Failed to load jobs:', e);
  }
}

async function loadStats() {
  try {
    const res = await fetch('/api/stats');
    state.stats = await res.json();
  } catch (e) {
    console.error('Failed to load stats:', e);
  }
}

async function loadCurated() {
  try {
    const res = await fetch('/api/curated');
    const data = await res.json();
    state.curated = data.jobs || [];
    renderPortfolio();
  } catch (e) {
    console.error('Failed to load curated:', e);
  }
}

// ---------------------------------------------------------------------------
//  Portfolio
// ---------------------------------------------------------------------------

function renderPortfolio() {
  const grid = document.getElementById('portfolio-grid');

  if (state.curated.length === 0) {
    grid.innerHTML = `
      <div class="empty-state" style="grid-column: 1 / -1;">
        <div class="empty-icon">&mdash;</div>
        <h3>Your portfolio is empty</h3>
        <p>Save interesting jobs from the Map or Table view, or add entries manually.</p>
      </div>
    `;
    return;
  }

  grid.innerHTML = state.curated.map((job, idx) => {
    const statusClass = (job.Status || 'watchlist').toLowerCase().replace(/\s/g, '-');
    return `
      <div class="portfolio-card slide-up" style="animation-delay: ${idx * 0.03}s">
        <button class="card-delete-btn" onclick="deleteCurated(${idx})" title="Remove">&times;</button>
        <div class="card-title">${escapeHtml(job.Job_Type_or_Title || job.title || 'Untitled')}</div>
        <div class="card-company">${escapeHtml(job.Company || job.company || '')}</div>
        <div class="card-detail">${escapeHtml(job.Location || job.location || 'N/A')}</div>
        <div class="card-detail"><a href="${escapeHtml(job.Link || job.link || '#')}" target="_blank" class="job-link">${job.Source || job.source || 'Link'}</a></div>
        <div class="card-detail" style="margin-top: 6px;">
          <select class="status-select" onchange="updateCuratedStatus(${idx}, this.value)">
            ${['Watchlist', 'To Review', 'Applied', 'Interview', 'Offer', 'Rejected'].map(s =>
              `<option value="${s}" ${(job.Status || '') === s ? 'selected' : ''}>${s}</option>`
            ).join('')}
          </select>
        </div>
        ${job.Notes ? `<div class="card-notes">${escapeHtml(job.Notes)}</div>` : ''}
      </div>
    `;
  }).join('');
}

async function saveToPortfolio(title, company, location, link, source, skills) {
  try {
    const res = await fetch('/api/curated', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title, company, location, link,
        source: source || 'Scraped',
        notes: `Skills: ${skills || 'General'}`,
        status: 'To Review',
      }),
    });
    if (res.ok) {
      showToast('Saved to portfolio!');
      loadCurated();
    }
  } catch (e) {
    console.error('Save failed:', e);
  }
}

async function saveManualEntry() {
  const data = {
    company: document.getElementById('manual-company').value,
    title: document.getElementById('manual-title').value,
    location: document.getElementById('manual-location').value,
    link: document.getElementById('manual-link').value,
    notes: document.getElementById('manual-notes').value,
    status: document.getElementById('manual-status').value,
    source: 'Manual',
  };

  try {
    const res = await fetch('/api/curated', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    if (res.ok) {
      closeModal('modal-manual');
      showToast('Entry added!');
      ['manual-company', 'manual-title', 'manual-location', 'manual-link', 'manual-notes'].forEach(id => {
        document.getElementById(id).value = '';
      });
      loadCurated();
    }
  } catch (e) {
    console.error('Manual save failed:', e);
  }
}

async function updateCuratedStatus(idx, status) {
  try {
    await fetch(`/api/curated/${idx}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status }),
    });
    state.curated[idx].Status = status;
  } catch (e) {
    console.error('Update failed:', e);
  }
}

async function deleteCurated(idx) {
  if (!confirm('Remove this entry from your portfolio?')) return;
  try {
    const res = await fetch(`/api/curated/${idx}`, { method: 'DELETE' });
    if (res.ok) {
      showToast('Entry removed');
      loadCurated();
    }
  } catch (e) {
    console.error('Delete failed:', e);
  }
}

// ---------------------------------------------------------------------------
//  Scraping
// ---------------------------------------------------------------------------

async function startScrape() {
  const sources = [...document.querySelectorAll('.scrape-source:checked')].map(c => c.value);
  const terms = document.getElementById('scrape-terms').value
    .split('\n')
    .map(t => t.trim())
    .filter(t => t.length > 0);

  if (sources.length === 0) {
    alert('Please select at least one source.');
    return;
  }

  closeModal('modal-scrape');

  document.getElementById('scrape-progress').style.display = 'block';
  document.getElementById('scrape-log').innerHTML = '';
  document.getElementById('scrape-bar').style.width = '0%';
  document.getElementById('scrape-percent').textContent = '0%';
  document.getElementById('btn-scrape').disabled = true;

  try {
    const res = await fetch('/api/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ terms, sources }),
    });

    if (!res.ok) {
      const err = await res.json();
      showToast('Error: ' + (err.error || 'Scrape failed'));
      document.getElementById('scrape-progress').style.display = 'none';
      document.getElementById('btn-scrape').disabled = false;
      return;
    }

    pollScrapeStatus();
  } catch (e) {
    console.error('Scrape start failed:', e);
    document.getElementById('scrape-progress').style.display = 'none';
    document.getElementById('btn-scrape').disabled = false;
  }
}

async function pollScrapeStatus() {
  const logEl = document.getElementById('scrape-log');
  let lastMsgCount = 0;

  const poll = async () => {
    try {
      const res = await fetch('/api/scrape/status');
      const data = await res.json();

      const percent = data.total > 0 ? Math.round((data.progress / data.total) * 100) : 0;
      document.getElementById('scrape-bar').style.width = `${percent}%`;
      document.getElementById('scrape-percent').textContent = `${percent}%`;

      if (data.messages.length > lastMsgCount) {
        const newMsgs = data.messages.slice(lastMsgCount);
        newMsgs.forEach(msg => {
          const line = document.createElement('div');
          line.className = 'log-line';
          line.textContent = msg;
          logEl.appendChild(line);
        });
        logEl.scrollTop = logEl.scrollHeight;
        lastMsgCount = data.messages.length;
      }

      if (data.running) {
        setTimeout(poll, 1000);
      } else {
        document.getElementById('scrape-bar').style.width = '100%';
        document.getElementById('scrape-percent').textContent = '100%';
        document.getElementById('btn-scrape').disabled = false;

        await loadJobs();
        await loadStats();

        showToast('Scrape complete!');

        setTimeout(() => {
          document.getElementById('scrape-progress').style.display = 'none';
        }, 3000);
      }
    } catch (e) {
      console.error('Poll failed:', e);
      setTimeout(poll, 2000);
    }
  };

  poll();
}

// ---------------------------------------------------------------------------
//  Modals
// ---------------------------------------------------------------------------

function openModal(id) {
  document.getElementById(id).classList.add('active');
}

function closeModal(id) {
  document.getElementById(id).classList.remove('active');
}

document.addEventListener('click', e => {
  if (e.target.classList.contains('modal-overlay')) {
    e.target.classList.remove('active');
  }
});

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    document.querySelectorAll('.modal-overlay.active').forEach(m => m.classList.remove('active'));
  }
});

// ---------------------------------------------------------------------------
//  Toast Notifications
// ---------------------------------------------------------------------------

function showToast(message) {
  const toast = document.createElement('div');
  toast.style.cssText = `
    position: fixed; bottom: 20px; right: 20px; z-index: 9999;
    padding: 12px 20px; background: #ffffff;
    border: 1px solid #e2e5ea; border-radius: 10px;
    color: #1a1f2b; font-size: 13px; font-weight: 500;
    box-shadow: 0 8px 30px rgba(0,0,0,0.1); animation: slideUp 0.3s ease-out;
    font-family: 'Inter', sans-serif;
  `;
  toast.textContent = message;
  document.body.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transition = 'opacity 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 2500);
}

// ---------------------------------------------------------------------------
//  Utilities
// ---------------------------------------------------------------------------

function escapeHtml(str) {
  if (!str) return '';
  const div = document.createElement('div');
  div.textContent = String(str);
  return div.innerHTML;
}

function escapeJS(str) {
  if (!str) return '';
  return String(str).replace(/'/g, "\\'").replace(/"/g, '\\"').replace(/\n/g, ' ');
}
