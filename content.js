(function() {
  'use strict';

  const CONFIG = {
    cacheEnabled: true,
    showLoadingIndicator: true,
    maxConcurrency: 24,
    maxDepth: 6,
    idbBatchDelay: 300,
  };

  const CACHE_EXPIRY_MS = 24 * 3600 * 1000;
  const BASE_URL        = 'https://myrient.erista.me/files/';
  const DB_NAME         = 'myrient-cache';
  const DB_VERSION      = 1;
  const STORE_SIZES     = 'sizes';
  const STORE_LISTINGS  = 'listings';

  function urlToPath(url) {
    return url.startsWith(BASE_URL) ? url.slice(BASE_URL.length) : url;
  }

  function pathToUrl(p) {
    return p.startsWith('http') ? p : BASE_URL + p;
  }

  let dbPromise = null;

  function getDB() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = e => {
        const db = e.target.result;
        if (!db.objectStoreNames.contains(STORE_SIZES))    db.createObjectStore(STORE_SIZES);
        if (!db.objectStoreNames.contains(STORE_LISTINGS)) db.createObjectStore(STORE_LISTINGS);
      };
      req.onsuccess = e => resolve(e.target.result);
      req.onerror   = e => reject(e.target.error);
    });
    return dbPromise;
  }

  function idbGetAll(store) {
    return getDB().then(db => new Promise((resolve, reject) => {
      const out = {};
      const req = db.transaction(store, 'readonly').objectStore(store).openCursor();
      req.onsuccess = e => {
        const c = e.target.result;
        if (c) { out[c.key] = c.value; c.continue(); } else resolve(out);
      };
      req.onerror = e => reject(e.target.error);
    }));
  }

  const idbPendingSizes    = new Map();
  const idbPendingListings = new Map();
  let   idbFlushTimer      = null;

  function idbScheduleFlush() {
    if (idbFlushTimer) return;
    idbFlushTimer = setTimeout(idbFlush, CONFIG.idbBatchDelay);
  }

  async function idbFlush() {
    idbFlushTimer = null;
    const db = await getDB();

    if (idbPendingSizes.size) {
      const entries = new Map(idbPendingSizes);
      idbPendingSizes.clear();
      const tx = db.transaction(STORE_SIZES, 'readwrite');
      const os = tx.objectStore(STORE_SIZES);
      for (const [k, v] of entries) os.put(v, k);
      await new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = rej; });
    }

    if (idbPendingListings.size) {
      const entries = new Map(idbPendingListings);
      idbPendingListings.clear();
      const tx = db.transaction(STORE_LISTINGS, 'readwrite');
      const os = tx.objectStore(STORE_LISTINGS);
      for (const [k, v] of entries) os.put(v, k);
      await new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = rej; });
    }
  }

  function idbClear(store) {
    return getDB().then(db => new Promise((resolve, reject) => {
      const req = db.transaction(store, 'readwrite').objectStore(store).clear();
      req.onsuccess = () => resolve();
      req.onerror   = e => reject(e.target.error);
    }));
  }

  const sizeCache    = new Map();
  const listingCache = new Map();

  const cacheReady = (async () => {
    try {
      const now = Date.now();
      const [sizes, listings] = await Promise.all([
        idbGetAll(STORE_SIZES),
        idbGetAll(STORE_LISTINGS)
      ]);
      for (const [k, v] of Object.entries(sizes))
        if (now - v.ts < CACHE_EXPIRY_MS) sizeCache.set(k, v.size);
      for (const [k, v] of Object.entries(listings))
        if (now - v.ts < CACHE_EXPIRY_MS) listingCache.set(k, v.items);
    } catch (e) {}
  })();

  function cacheSetSize(relPath, bytes) {
    sizeCache.set(relPath, bytes);
    idbPendingSizes.set(relPath, { size: bytes, ts: Date.now() });
    idbScheduleFlush();
  }

  function cacheSetListing(relPath, items) {
    listingCache.set(relPath, items);
    idbPendingListings.set(relPath, { items, ts: Date.now() });
    idbScheduleFlush();
  }

  ['myrient-folder-sizes', 'myrient-folder-listings', 'myrient-v6'].forEach(k => {
    try { localStorage.removeItem(k); } catch (_) {}
  });

  const channel = new BroadcastChannel('myrient-sizes');

  channel.onmessage = ({ data }) => {
    if (!data || data.type !== 'size') return;
    const { path, size } = data;
    cacheSetSize(path, size);
    resolveInFlight(path, size, false);
    updateRowForPath(path, size);
  };

  function broadcastSize(relPath, size) {
    try { channel.postMessage({ type: 'size', path: relPath, size }); } catch (_) {}
  }

  function updateRowForPath(relPath, size) {
    const tbody = document.querySelector('tbody');
    if (!tbody) return;
    for (const row of tbody.querySelectorAll('tr')) {
      const link = row.querySelector('td.link a');
      if (!link) continue;
      const href = link.getAttribute('href');
      if (!href) continue;
      const hrefPath = href.split('?')[0];
      const candidate = urlToPath(BASE_URL + (hrefPath.startsWith('/') ? hrefPath.slice(1) : hrefPath));
      if (candidate === relPath || urlToPath(new URL(hrefPath, window.location.href).href) === relPath) {
        updateFolderSizeCell(row, size, false);
        break;
      }
    }
  }

  const inFlight          = new Map();
  const inFlightResolvers = new Map();

  function resolveInFlight(path, size, approximate) {
    if (inFlightResolvers.has(path)) {
      for (const resolve of inFlightResolvers.get(path))
        resolve({ size, approximate });
      inFlightResolvers.delete(path);
      inFlight.delete(path);
    }
  }

  let activeRequests = 0;
  const requestQueue = [];

  function acquireSlot() {
    return new Promise(resolve => {
      if (activeRequests < CONFIG.maxConcurrency) { activeRequests++; resolve(); }
      else requestQueue.push(resolve);
    });
  }

  function releaseSlot() {
    if (requestQueue.length > 0) requestQueue.shift()();
    else activeRequests--;
  }

  async function fetchWithLimit(url) {
    await acquireSlot();
    try { return await (await fetch(url)).text(); }
    finally { releaseSlot(); }
  }

  let aborted = false;

  const SIZE_UNITS = {
    'B':1,'K':1024,'KB':1024,'KIB':1024,
    'M':1048576,'MB':1048576,'MIB':1048576,
    'G':1073741824,'GB':1073741824,'GIB':1073741824,
    'T':1099511627776,'TB':1099511627776,'TIB':1099511627776
  };

  function parseSizeToBytes(sizeStr) {
    if (!sizeStr || sizeStr === '-') return 0;
    const t = sizeStr.trim();
    if (/^\d+$/.test(t)) return +t;
    const m = t.match(/^([\d,.]+)\s*([KMGT]i?B|B)$/i);
    if (!m) return 0;
    return parseFloat(m[1].replace(/,/g, '')) * (SIZE_UNITS[m[2].toUpperCase()] || 1);
  }

  function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1000, sizes = ['B','KB','MB','GB','TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / k ** i).toFixed(1)) + ' ' + sizes[i];
  }

  const ROW_RE  = /<tr[^>]*>[\s\S]*?<\/tr>/gi;
  const LINK_RE = /class="link"[\s\S]*?href="([^"]+)"[\s\S]*?<\/a>/i;
  const SIZE_RE = /<td[^>]*>([\d,.]+\s*[KMGT]i?B|[\d,.]+\s*B|\d+|-)<\/td>/i;

  function parseDirectoryListing(html, baseUrl) {
    const items = [];
    const tbodyMatch = html.match(/<tbody>([\s\S]*?)<\/tbody>/i);
    const body = tbodyMatch ? tbodyMatch[1] : html;

    ROW_RE.lastIndex = 0;
    let rowMatch;
    while ((rowMatch = ROW_RE.exec(body)) !== null) {
      const row = rowMatch[0];
      const linkMatch = LINK_RE.exec(row);
      if (!linkMatch) continue;
      const href = linkMatch[1];
      const hrefPath = href.split('?')[0];
      if (hrefPath === '../' || hrefPath === './') continue;
      const isFolder = hrefPath.endsWith('/');
      let size = 0;
      if (!isFolder) {
        const sizeMatch = SIZE_RE.exec(row);
        size = sizeMatch ? parseSizeToBytes(sizeMatch[1]) : 0;
      }
      const absUrl = hrefPath.startsWith('http')
        ? hrefPath
        : baseUrl.replace(/\/$/, '') + '/' + hrefPath.replace(/^\//, '');
      items.push({ path: urlToPath(absUrl), isFolder, size });
    }

    if (items.length === 0) {
      const doc = new DOMParser().parseFromString(html, 'text/html');
      for (const row of doc.querySelectorAll('tbody tr')) {
        const cells = row.querySelectorAll('td');
        if (cells.length < 3 || !cells[0].classList.contains('link')) continue;
        const link = cells[0].querySelector('a');
        if (!link) continue;
        const h  = link.getAttribute('href');
        const hp = h.split('?')[0];
        if (hp === '../' || hp === './') continue;
        const iF = hp.endsWith('/');
        const fullUrl = new URL(hp, baseUrl).href;
        items.push({ path: urlToPath(fullUrl), isFolder: iF, size: iF ? 0 : parseSizeToBytes(cells[1].textContent.trim()) });
      }
    }

    return items;
  }

  async function calculateFolderSize(folderUrl, depth = 0) {
    if (aborted) return { size: 0, approximate: false };

    const folderPath = urlToPath(folderUrl);

    if (CONFIG.cacheEnabled && sizeCache.has(folderPath))
      return { size: sizeCache.get(folderPath), approximate: false };

    if (depth >= CONFIG.maxDepth) return { size: 0, approximate: true };

    if (inFlight.has(folderPath))
      return inFlight.get(folderPath);

    let resolveMe;
    const dedupPromise = new Promise(resolve => { resolveMe = resolve; });
    inFlight.set(folderPath, dedupPromise);
    if (!inFlightResolvers.has(folderPath)) inFlightResolvers.set(folderPath, []);
    inFlightResolvers.get(folderPath).push(resolveMe);

    let items;
    if (CONFIG.cacheEnabled && listingCache.has(folderPath)) {
      items = listingCache.get(folderPath);
    } else {
      try {
        const html = await fetchWithLimit(folderUrl);
        if (aborted) {
          resolveMe({ size: 0, approximate: false });
          inFlight.delete(folderPath);
          return { size: 0, approximate: false };
        }
        items = parseDirectoryListing(html, folderUrl);
        if (CONFIG.cacheEnabled) cacheSetListing(folderPath, items);
      } catch (err) {
        resolveMe({ size: 0, approximate: true });
        inFlight.delete(folderPath);
        inFlightResolvers.delete(folderPath);
        return { size: 0, approximate: true };
      }
    }

    const results = await Promise.all(
      items.map(item =>
        item.isFolder
          ? calculateFolderSize(pathToUrl(item.path), depth + 1)
          : Promise.resolve({ size: item.size, approximate: false })
      )
    );

    if (aborted) {
      resolveMe({ size: 0, approximate: false });
      inFlight.delete(folderPath);
      inFlightResolvers.delete(folderPath);
      return { size: 0, approximate: false };
    }

    const totalSize   = results.reduce((sum, r) => sum + r.size, 0);
    const approximate = results.some(r => r.approximate);

    if (CONFIG.cacheEnabled) cacheSetSize(folderPath, totalSize);

    resolveMe({ size: totalSize, approximate });
    inFlight.delete(folderPath);
    inFlightResolvers.delete(folderPath);
    if (!approximate) broadcastSize(folderPath, totalSize);

    return { size: totalSize, approximate };
  }

  function updateFolderSizeCell(row, size, approximate = false) {
    const cells = row.querySelectorAll('td');
    if (cells.length < 2) return;
    const sizeCell = cells[1];
    sizeCell.textContent = (approximate ? '~' : '') + formatBytes(size);
    sizeCell.classList.add('folder-size-calculated');
    if (approximate) sizeCell.classList.add('folder-size-approximate');
    sizeCell.setAttribute('title', size.toLocaleString() + ' bytes' + (approximate ? ' (approximate)' : ''));
    sizeCell.setAttribute('data-bytes', size);
  }

  function showLoading(row) {
    const cells = row.querySelectorAll('td');
    if (cells.length < 2) return;
    cells[1].innerHTML = '<span class="folder-size-loading">calculating...</span>';
  }

  async function processFolderRows() {
    await cacheReady;

    const tbody = document.querySelector('tbody');
    if (!tbody) return;

    const folderJobs = [];

    for (const row of Array.from(tbody.querySelectorAll('tr'))) {
      const cells = row.querySelectorAll('td');
      if (cells.length < 3 || !cells[0].classList.contains('link')) continue;
      const link = cells[0].querySelector('a');
      if (!link) continue;
      const href = link.getAttribute('href');
      const name = link.textContent.trim();
      if (name === '../' || name === './' || href === '../' || href === './') continue;

      const hrefPath = href.split('?')[0];
      const isFolder = hrefPath.endsWith('/');
      const sizeText = cells[1].textContent.trim();

      if (!isFolder && sizeText !== '-') {
        cells[1].setAttribute('data-bytes', parseSizeToBytes(sizeText));
      }

      if (isFolder && sizeText === '-') {
        const folderUrl  = new URL(hrefPath, window.location.href).href;
        const folderPath = urlToPath(folderUrl);

        if (sizeCache.has(folderPath)) {
          updateFolderSizeCell(row, sizeCache.get(folderPath));
          continue;
        }
        if (CONFIG.showLoadingIndicator) showLoading(row);
        folderJobs.push({ row, folderUrl, name });
      }
    }

    if (folderJobs.length === 0) {
      if (sizeSortAsc !== null) sortTableBySize(sizeSortAsc);
      return;
    }

    await Promise.all(
      folderJobs.map(({ row, folderUrl, name }) =>
        calculateFolderSize(folderUrl, 0)
          .then(({ size, approximate }) => {
            if (aborted) return;
            updateFolderSizeCell(row, size, approximate);
          })
          .catch(() => {
            const c = row.querySelectorAll('td');
            if (c.length >= 2) c[1].innerHTML = '<span class="folder-size-error">error</span>';
          })
      )
    );

    if (!aborted && sizeSortAsc !== null) sortTableBySize(sizeSortAsc);

    await idbFlush();
  }

  function getRowSizeBytes(row) {
    const sizeCell = row.querySelectorAll('td')[1];
    if (!sizeCell) return -1;
    const raw = sizeCell.getAttribute('data-bytes');
    if (raw !== null) return parseInt(raw, 10);
    const text = sizeCell.textContent.trim();
    if (text === '-' || text.includes('calc') || text === '-') return -1;
    return parseSizeToBytes(text.replace(/^~/, ''));
  }

  let sizeSortAsc = null;

  function sortTableBySize(ascending) {
    const tbody = document.querySelector('tbody');
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll('tr'));
    const isPinned = row => {
      const link = row.querySelector('td.link a');
      if (!link) return true;
      const href = link.getAttribute('href') || '';
      const name = link.textContent.trim();
      return name === '../' || name === './' ||
             href === '../' || href === './' ||
             href.startsWith('../?') || href.startsWith('./?');
    };
    const pinned   = rows.filter(isPinned);
    const sortable = rows.filter(r => !isPinned(r));
    sortable.sort((a, b) => {
      const sa = getRowSizeBytes(a), sb = getRowSizeBytes(b);
      if (sa === -1 && sb === -1) return 0;
      if (sa === -1) return 1;
      if (sb === -1) return -1;
      return ascending ? sa - sb : sb - sa;
    });
    [...pinned, ...sortable].forEach(r => tbody.appendChild(r));
    const sizeHeader = document.querySelector('thead th:nth-child(2)');
    if (sizeHeader) {
      const existing = sizeHeader.querySelector('.sort-indicator');
      if (existing) existing.remove();
      const ind = document.createElement('span');
      ind.className   = 'sort-indicator';
      ind.textContent = ascending ? ' +' : ' -';
      ind.style.cssText = 'color:#4CAF50;font-weight:bold;';
      sizeHeader.appendChild(ind);
    }
  }

  function installSortInterceptor() {
    const thead = document.querySelector('thead');
    if (!thead) return;
    thead.querySelectorAll('a[href*="C=S"]').forEach(link => {
      link.addEventListener('click', e => {
        e.preventDefault();
        const ascending = link.getAttribute('href').includes('O=A');
        sizeSortAsc = ascending;
        sortTableBySize(ascending);
      });
    });
  }

  function init() {
    if (!document.querySelector('table#list')) return;

    window.clearMyrientCache = async () => {
      sizeCache.clear();
      listingCache.clear();
      await Promise.all([idbClear(STORE_SIZES), idbClear(STORE_LISTINGS)]);
    };

    window.myrientCacheStats = async () => {
      const [sizes, listings] = await Promise.all([
        idbGetAll(STORE_SIZES),
        idbGetAll(STORE_LISTINGS)
      ]);
      const sB = new TextEncoder().encode(JSON.stringify(sizes)).length;
      const lB = new TextEncoder().encode(JSON.stringify(listings)).length;
      console.log('Sizes:   ', Object.keys(sizes).length, 'entries', (sB/1024).toFixed(1)+'KB');
      console.log('Listings:', Object.keys(listings).length, 'entries', (lB/1024).toFixed(1)+'KB');
      console.log('Total:   ', ((sB+lB)/1024).toFixed(1)+'KB');
    };

    installSortInterceptor();
    processFolderRows();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
