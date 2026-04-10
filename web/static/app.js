// app.js — Vanilla JS for job finder web app

// ── Profile field forms: submit via fetch so the page never reloads ──────────
// Covers: tag add, tag remove, scalar text/number save, scalar select change.
// Forms opt in by setting data-xhr="1" on the <form> element.
document.addEventListener('submit', function (e) {
  const form = e.target;
  if (form.dataset.xhr !== '1') return;

  e.preventDefault();

  const formData = new FormData(form);
  // Use getAttribute to avoid shadowing by <input name="action"> in Firefox
  const actionUrl = form.getAttribute('action');
  fetch(actionUrl, { method: 'POST', body: formData })
    .then(r => { if (!r.ok) throw new Error('Request failed'); return r.json(); })
    .then(() => {
      const action = formData.get('action');
      const value  = formData.get('value');

      if (action === 'add') {
        _tagAdd(form, value);
      } else if (action === 'remove') {
        _tagRemove(form);
      } else if (action === 'set') {
        _scalarSaved(form);
      }
    })
    .catch(err => console.error('Field update failed:', err));
});

function _tagAdd(form, value) {
  if (!value) return;
  const field    = form.querySelector('[name="field"]').value;
  // Walk up to the nearest ancestor that also contains a .tag-list sibling
  const tagList  = form.parentElement && form.parentElement.querySelector('.tag-list');
  if (!tagList) { console.warn('[tagAdd] .tag-list not found for field', field); return; }

  // Derive the css class from the existing tags (if any)
  const existingTag = tagList.querySelector('.tag');
  let extraClass = '';
  if (existingTag) {
    extraClass = [...existingTag.classList]
      .filter(c => c !== 'tag')
      .join(' ');
  }

  // Build the new tag span matching the Jinja template output
  const span = document.createElement('span');
  span.className = ['tag', extraClass].filter(Boolean).join(' ');
  span.appendChild(document.createTextNode(value + ' '));

  const removeForm = document.createElement('form');
  removeForm.method = 'post';
  removeForm.setAttribute('action', form.getAttribute('action'));
  removeForm.style.cssText = 'display:inline;margin:0;';
  removeForm.dataset.xhr = '1';

  const mkHidden = (n, v) => {
    const i = document.createElement('input');
    i.type = 'hidden'; i.name = n; i.value = v;
    return i;
  };
  removeForm.appendChild(mkHidden('field', field));
  removeForm.appendChild(mkHidden('action', 'remove'));
  removeForm.appendChild(mkHidden('value', value));

  const btn = document.createElement('button');
  btn.type = 'submit';
  btn.className = 'tag-remove';
  btn.title = 'Remove';
  btn.textContent = '×';
  removeForm.appendChild(btn);
  span.appendChild(removeForm);
  tagList.appendChild(span);

  // Clear the add input and refocus it
  const input = form.querySelector('input[name="value"]');
  if (input) { input.value = ''; input.focus(); }
}

function _tagRemove(form) {
  // The form is inside the tag <span> — remove the whole span
  const span = form.closest('.tag');
  if (span) span.remove();
}

function _scalarSaved(form) {
  // Brief visual feedback — button text for text/number fields, border flash for selects
  const btn = form.querySelector('.btn-small');
  if (btn) {
    const orig = btn.textContent;
    btn.textContent = '✓';
    setTimeout(() => { btn.textContent = orig; }, 1500);
  }
  const sel = form.querySelector('select');
  if (sel) {
    sel.style.outline = '2px solid var(--accent, #4f8ef7)';
    setTimeout(() => { sel.style.outline = ''; }, 1000);
  }
}

// Auto-save contenteditable profile fields (e.g. summary)
function saveEditableField(el) {
  const field   = el.dataset.field;
  const profile = el.dataset.profile;
  const value   = el.innerText.trim();
  const indicator = document.getElementById('summary-saved');

  const formData = new FormData();
  formData.append('field', field);
  formData.append('action', 'set');
  formData.append('value', value);

  fetch(`/profile/${profile}/field`, { method: 'POST', body: formData })
    .then(r => {
      if (r.ok && indicator) {
        indicator.style.display = 'inline';
        setTimeout(() => { indicator.style.display = 'none'; }, 2000);
      }
    })
    .catch(console.error);
}

// ── Contextual status buttons ─────────────────────────────────────────────────

// Next suggested stages given current status (save handled separately via toggleSave)
const _NEXT_ACTIONS = {
  'new':           ['applied', 'rejected'],
  'applied':       ['phone_screen', 'rejected'],
  'phone_screen':  ['interview_1', 'rejected'],
  'interview_1':   ['interview_2', 'rejected'],
  'interview_2':   ['interview_3', 'rejected'],
  'interview_3':   ['offer', 'rejected'],
  'offer':         ['withdrawn'],
  'rejected':      ['applied'],
  'withdrawn':     ['applied'],
  'missing_info':  [],
};

function _initContextualButtons(actionsRow) {
  const currentStatus = actionsRow.dataset.status || 'new';
  const nextActions   = _NEXT_ACTIONS[currentStatus] || [];
  const expanded      = actionsRow.classList.contains('status-expanded');

  // Only apply contextual show/hide to stage buttons (not the save toggle)
  actionsRow.querySelectorAll('.stage-btn[data-status-val]').forEach(btn => {
    const val      = btn.dataset.statusVal;
    const isActive = btn.classList.contains('active');
    const show     = expanded || isActive || nextActions.includes(val);
    btn.style.display = show ? '' : 'none';
  });
}

function toggleStatusExpand(moreBtn) {
  const actionsRow = moreBtn.closest('.job-card-actions');
  actionsRow.classList.toggle('status-expanded');
  moreBtn.textContent = actionsRow.classList.contains('status-expanded') ? '↑' : '···';
  _initContextualButtons(actionsRow);
}

document.addEventListener('DOMContentLoaded', function () {
  document.querySelectorAll('.job-card-actions[data-status]').forEach(_initContextualButtons);
  _initViewTransitions();
  initScorePillAnimations();
  initScrollAnimations();
  initAmbientBg();
});

// Update application status (job list page)
function setStatus(profileName, jobKey, status, btn) {
  fetch(`/profile/${profileName}/job/${jobKey}/status`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
  })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        const card       = btn.closest('.job-card');
        const actionsRow = card.querySelector('.job-card-actions');

        // Keep data-status in sync so CSS left-border color updates live
        card.dataset.status = status;

        // Update stage buttons (radio-style: only one active)
        card.querySelectorAll('.stage-btn').forEach(b => b.classList.remove('active'));
        card.querySelectorAll('.stage-btn[data-status-val="' + status + '"]').forEach(b => b.classList.add('active'));

        const badge = card.querySelector('.status-badge');
        if (badge) {
          badge.className = `status-badge status-${status}`;
          badge.textContent = status.replace(/_/g, ' ');
        }

        // Collapse expand and re-init contextual buttons for new status
        if (actionsRow) {
          actionsRow.dataset.status = status;
          actionsRow.classList.remove('status-expanded');
          const moreBtn = actionsRow.querySelector('.status-btn-more');
          if (moreBtn) moreBtn.textContent = '···';
          _initContextualButtons(actionsRow);
        }
      }
    })
    .catch(console.error);
}

// Toggle save/unsave (job list page)
function toggleSave(profileName, jobKey, btn) {
  fetch(`/profile/${profileName}/job/${jobKey}/save`, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        btn.textContent = data.saved ? '★ Saved' : '☆ Save';
        btn.classList.toggle('active', data.saved);
      }
    })
    .catch(console.error);
}

// Set application stage with radio-button behavior: clicking active stage deactivates it (→ new)
function setStage(profileName, jobKey, stage, btn) {
  const card = btn.closest('.job-card');
  const currentStatus = card ? card.dataset.status : 'new';
  const newStatus = currentStatus === stage ? 'new' : stage;
  setStatus(profileName, jobKey, newStatus, btn);
}

// Remove job from recommended list → hidden (job list page)
function removeJob(profileName, jobKey, btn) {
  fetch(`/profile/${profileName}/job/${jobKey}/hide`, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        const card = btn.closest('.job-card');
        if (card) {
          card.style.transition = 'opacity .2s, transform .2s';
          card.style.opacity = '0';
          card.style.transform = 'translateX(-6px)';
          setTimeout(() => card.remove(), 220);
        }
      }
    })
    .catch(console.error);
}

// Add hidden job back to list (detail page) → navigate back to list
function promoteJobDetail(profileName, jobKey) {
  fetch(`/profile/${profileName}/job/${jobKey}/promote`, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        window.location.href = `/profile/${profileName}`;
      }
    })
    .catch(console.error);
}

// Remove job from recommended list (detail page) → navigate back to list
function removeJobDetail(profileName, jobKey) {
  fetch(`/profile/${profileName}/job/${jobKey}/hide`, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        window.location.href = `/profile/${profileName}`;
      }
    })
    .catch(console.error);
}

// Manually promote a hidden job to the recommended list (hidden tab)
function promoteJob(profileName, jobKey, btn) {
  fetch(`/profile/${profileName}/job/${jobKey}/promote`, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        const card = btn.closest('.job-card');
        if (card) {
          card.style.transition = 'opacity .2s';
          card.style.opacity = '0';
          setTimeout(() => card.remove(), 220);
        }
      }
    })
    .catch(console.error);
}

// Update status (detail page)
function setStatusDetail(profileName, jobKey, status, _btn) {
  fetch(`/profile/${profileName}/job/${jobKey}/status`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
  })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        // Update stage buttons (radio-style)
        document.querySelectorAll('.stage-btn-detail').forEach(b => b.classList.remove('active'));
        if (document.querySelectorAll('.stage-btn-detail[data-status-val="' + status + '"]').length) {
          document.querySelectorAll('.stage-btn-detail[data-status-val="' + status + '"]').forEach(b => b.classList.add('active'));
        }

        const badge = document.getElementById('current-status-badge');
        if (badge) {
          badge.className = `status-badge status-${status}`;
          badge.textContent = status.replace(/_/g, ' ');
        }
      }
    })
    .catch(console.error);
}

// Toggle save/unsave (detail page)
function toggleSaveDetail(profileName, jobKey, btn) {
  fetch(`/profile/${profileName}/job/${jobKey}/save`, { method: 'POST' })
    .then(r => r.json())
    .then(data => {
      if (data.ok) {
        btn.textContent = data.saved ? '★ Saved' : '☆ Save';
        btn.classList.toggle('active', data.saved);
      }
    })
    .catch(console.error);
}

// Set application stage with radio-button behavior (detail page)
function setStageDetail(profileName, jobKey, stage, btn) {
  const isActive = btn.classList.contains('active');
  const newStatus = isActive ? 'new' : stage;
  setStatusDetail(profileName, jobKey, newStatus, btn);
}

// ── Match pairs modal ─────────────────────────────────────────────────────────

const _MGR_CATS  = new Set(['skill', 'experience', 'domain']);
const _CAND_CATS = new Set(['compensation', 'equity', 'benefits', 'work_arrangement', 'culture_growth']);

const _CAT_LABELS = {
  skill: 'Skill', experience: 'Experience', domain: 'Domain',
  compensation: 'Compensation', equity: 'Equity', benefits: 'Benefits',
  work_arrangement: 'Work Style', culture_growth: 'Culture & Growth',
};

const _IMP_LABELS = {
  required: 'required', preferred: 'preferred', nice_to_have: 'nice to have',
  must_have: 'must have',
};

function showPairsModal(pill, perspective) {
  const dualScores = pill.closest('.dual-scores');
  let allPairs;
  try {
    allPairs = JSON.parse(dualScores.dataset.pairs || '[]');
    if (!Array.isArray(allPairs)) allPairs = [];
  } catch (e) {
    allPairs = [];
  }

  const isMgr = perspective === 'mgr';
  const cats  = isMgr ? _MGR_CATS : _CAND_CATS;
  const pairs = allPairs.filter(p => cats.has((p.category || '').toLowerCase()));

  const modal = document.getElementById('pairs-modal');
  document.getElementById('pairs-modal-title').textContent =
    isMgr ? 'Hiring Manager View — Skills & Experience' : 'Your View — Preferences & Fit';
  document.getElementById('pairs-modal-body').innerHTML =
    pairs.length ? _renderPairs(pairs, isMgr) : '<p class="pairs-empty">No match data available for this job.</p>';

  modal.classList.add('open');
}

function _renderPairs(pairs, isMgr) {
  const jobHeader  = isMgr ? 'Job Requires'  : 'Job Offers';
  const candHeader = isMgr ? 'You Have'      : 'You Want';

  let html = `<div class="pairs-table">
    <div class="pairs-row pairs-header">
      <span></span>
      <span>Category</span>
      <span>${jobHeader}</span>
      <span>${candHeader}</span>
    </div>`;

  for (const p of pairs) {
    const hasJob  = p.job_side  && p.job_side.trim();
    const hasCand = p.candidate_side && p.candidate_side.trim();
    const isMatch = hasJob && hasCand;
    const rowCls  = isMatch ? 'pairs-match' : 'pairs-gap';
    const icon    = isMatch ? '✓' : '✗';

    const cat     = (p.category || '').toLowerCase();
    const catLabel = _CAT_LABELS[cat] || cat;

    const imp = isMgr
      ? (p.job_importance   ? `<span class="pairs-imp">${_IMP_LABELS[p.job_importance]   || p.job_importance}</span>`   : '')
      : (p.candidate_priority ? `<span class="pairs-imp">${_IMP_LABELS[p.candidate_priority] || p.candidate_priority}</span>` : '');

    const jobCell  = hasJob  ? `<span>${p.job_side}</span>`                       : `<span class="pairs-absent">—</span>`;
    const candCell = hasCand ? `<span>${p.candidate_side}</span>`                 : `<span class="pairs-absent">not in profile</span>`;

    html += `<div class="pairs-row ${rowCls}">
      <span class="pairs-icon">${icon}</span>
      <span class="pairs-cat">${catLabel}${imp}</span>
      ${jobCell}
      ${candCell}
    </div>`;
  }

  html += '</div>';
  return html;
}

function closePairsModal(event) {
  if (!event || event.target === document.getElementById('pairs-modal')) {
    document.getElementById('pairs-modal').classList.remove('open');
  }
}

// ── Full description modal ────────────────────────────────────────────────────

function openDescModal(jobCard) {
  const modal = document.getElementById('desc-modal');
  if (!modal || !jobCard) return;

  const title    = jobCard.dataset.title    || '';
  const company  = jobCard.dataset.company  || '';
  const mgrScore = parseInt(jobCard.dataset.managerScore  || '0', 10);
  const youScore = parseInt(jobCard.dataset.candidateScore || '0', 10);
  const mgrNotes = jobCard.dataset.matchNotes     || '';
  const youNotes = jobCard.dataset.candidateNotes || '';
  const desc     = jobCard.dataset.description    || '';
  const applyUrl = jobCard.dataset.applyUrl       || '';

  // Title + company
  modal.querySelector('.desc-modal-job-title').textContent = title;
  modal.querySelector('.desc-modal-company').textContent   = company;

  // Score bars — reset then animate
  const mgrFill = modal.querySelector('.desc-score-mgr');
  const youFill = modal.querySelector('.desc-score-you');
  const mgrPct  = modal.querySelector('.desc-score-mgr-pct');
  const youPct  = modal.querySelector('.desc-score-you-pct');
  if (mgrFill) { mgrFill.style.width = '0'; }
  if (youFill) { youFill.style.width = '0'; }
  if (mgrPct)  { mgrPct.textContent  = mgrScore; }
  if (youPct)  { youPct.textContent  = youScore; }

  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      if (mgrFill) mgrFill.style.width = (mgrScore / 200 * 100) + '%';
      if (youFill) youFill.style.width = (youScore / 200 * 100) + '%';
    });
  });

  // AI note panes
  function _notesToHtml(raw) {
    if (!raw) return '';
    const items = raw.split('\n')
      .map(l => l.trim().replace(/^•\s*/, ''))
      .filter(Boolean);
    if (!items.length) return '';
    return '<ul>' + items.map(l => `<li>${_escHtml(l)}</li>`).join('') + '</ul>';
  }
  modal.querySelector('.desc-modal-mgr-body').innerHTML = _notesToHtml(mgrNotes);
  modal.querySelector('.desc-modal-you-body').innerHTML = _notesToHtml(youNotes);

  // Show/hide AI row depending on whether there's content
  const aiRow = modal.querySelector('.desc-modal-ai-row');
  if (aiRow) aiRow.style.display = (mgrNotes || youNotes) ? '' : 'none';

  // Full description
  const descEl = modal.querySelector('.desc-modal-text');
  if (descEl) descEl.textContent = desc || 'No description available.';

  // Apply link
  const applyLink = modal.querySelector('.desc-modal-apply');
  if (applyLink) {
    if (applyUrl) {
      applyLink.href = applyUrl;
      applyLink.style.display = '';
    } else {
      applyLink.style.display = 'none';
    }
  }

  // Expand/collapse reset — collapse if text is long
  const expandWrap = modal.querySelector('.desc-expand-wrap');
  const expandBtn  = modal.querySelector('.desc-expand-btn');
  if (expandWrap && expandBtn) {
    expandWrap.classList.remove('expanded');
    expandBtn.textContent = 'Read more ↓';
    expandBtn.setAttribute('aria-expanded', 'false');
    expandBtn.style.display = desc && desc.length > 500 ? '' : 'none';
  }

  // Reset scroll to top
  modal.scrollTop = 0;

  modal.classList.add('open');
  document.body.style.overflow = 'hidden';
}

function toggleDescExpand() {
  const modal = document.getElementById('desc-modal');
  if (!modal) return;
  const wrap = modal.querySelector('.desc-expand-wrap');
  const btn  = modal.querySelector('.desc-expand-btn');
  if (!wrap || !btn) return;
  const isExpanded = wrap.classList.contains('expanded');
  wrap.classList.toggle('expanded', !isExpanded);
  btn.textContent = isExpanded ? 'Read more ↓' : 'Read less ↑';
  btn.setAttribute('aria-expanded', String(!isExpanded));
}

function closeDescModal(event) {
  const modal = document.getElementById('desc-modal');
  if (!modal) return;
  if (!event || event.target === modal) {
    modal.classList.remove('open');
    document.body.style.overflow = '';
  }
}

function _escHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

document.addEventListener('keydown', function (e) {
  if (e.key === 'Escape') {
    const descModal  = document.getElementById('desc-modal');
    const pairsModal = document.getElementById('pairs-modal');
    if (descModal  && descModal.classList.contains('open'))  {
      descModal.classList.remove('open');
      document.body.style.overflow = '';
    } else if (pairsModal) {
      pairsModal.classList.remove('open');
    }
  }
});

// ── ⚡ OVERDRIVE — View Transitions · Score Count-up · Scroll Stagger ──────────

// A: Cross-document View Transitions
// Tag the clicked job title link with view-transition-name just before navigation.
// The detail page h1 already has the matching name set in its template.
function _initViewTransitions() {
  document.querySelectorAll('a.job-title').forEach(link => {
    link.addEventListener('click', function () {
      // Clear any lingering VT name from other links first
      document.querySelectorAll('a.job-title').forEach(l => l.style.viewTransitionName = '');
      this.style.viewTransitionName = 'job-hero';
    });
  });

  // Prefetch detail pages on hover for near-instant navigation
  const prefetched = new Set();
  document.querySelectorAll('a.job-title').forEach(link => {
    link.addEventListener('mouseenter', function () {
      const href = this.href;
      if (!href || prefetched.has(href)) return;
      prefetched.add(href);
      const el = document.createElement('link');
      el.rel = 'prefetch';
      el.href = href;
      document.head.appendChild(el);
    }, { once: true });
  });
}

// B: Score pill count-up animation
function _easeOutQuart(t) {
  return 1 - Math.pow(1 - t, 4);
}

function _countUp(el, target, duration, delay) {
  if (target <= 0 || isNaN(target)) { el.textContent = target; return; }
  setTimeout(() => {
    const start = performance.now();
    function tick(now) {
      const elapsed  = now - start;
      const progress = Math.min(elapsed / duration, 1);
      el.textContent = Math.round(_easeOutQuart(progress) * target);
      if (progress < 1) requestAnimationFrame(tick);
      else el.textContent = target;
    }
    requestAnimationFrame(tick);
  }, delay);
}

function initScorePillAnimations() {
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

  document.querySelectorAll('.job-card').forEach((card, idx) => {
    const baseDelay = idx * 35;

    // New arc meters (jobs list page)
    card.querySelectorAll('.arc-value[data-score]').forEach((el, i) => {
      const target = parseInt(el.dataset.score, 10);
      if (!isNaN(target) && target > 0) {
        el.textContent = '0';
        _countUp(el, target, 720, baseDelay + i * 100);
      }
    });

    // Legacy score pills (detail page / fallback)
    const mgrEl = card.querySelector('.score-pill-mgr .score-pill-value');
    if (mgrEl) {
      const target = parseInt(mgrEl.dataset.score ?? mgrEl.textContent, 10);
      if (!isNaN(target) && target > 0) {
        mgrEl.textContent = '0';
        _countUp(mgrEl, target, 680, baseDelay);
      }
    }

    const youEl = card.querySelector('.score-pill-you .score-pill-value');
    if (youEl) {
      const target = parseInt(youEl.dataset.score ?? youEl.textContent, 10);
      if (!isNaN(target) && target > 0) {
        youEl.textContent = '0';
        _countUp(youEl, target, 680, baseDelay + 90);
      }
    }

    // Add glow halo to high-combined-score meters/pills (≥160)
    const combined = parseInt(card.dataset.combinedScore ?? '0', 10);
    if (combined >= 160) {
      const glowDelay = baseDelay + 800;
      setTimeout(() => {
        card.querySelector('.arc-meter-mgr')?.classList.add('glow-high');
        card.querySelector('.arc-meter-you')?.classList.add('glow-high');
        card.querySelector('.score-pill-mgr')?.classList.add('glow-high');
        card.querySelector('.score-pill-you')?.classList.add('glow-high');
      }, glowDelay);
    }
  });
}

// C: Scroll-driven card entry + ambient bg warmth
function initScrollAnimations() {
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  // IO fallback only needed when native scroll-driven animations aren't supported
  if (CSS.supports('animation-timeline: view()')) return;

  const cards = document.querySelectorAll('.job-list .job-card');
  if (!cards.length) return;

  cards.forEach(card => card.classList.add('card-io-hidden'));

  const io = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('card-io-visible');
        io.unobserve(entry.target);
      }
    });
  }, { threshold: 0.06, rootMargin: '0px 0px -30px 0px' });

  cards.forEach(card => io.observe(card));
}

function initAmbientBg() {
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

  const highScoreCards = [...document.querySelectorAll('.job-card[data-combined-score]')]
    .filter(c => parseInt(c.dataset.combinedScore ?? '0', 10) >= 130);

  if (!highScoreCards.length) return;

  const io = new IntersectionObserver((entries) => {
    if (entries.some(e => e.isIntersecting)) {
      document.body.classList.add('ambient-warm');
    }
  }, { threshold: 0.1 });

  highScoreCards.forEach(card => io.observe(card));
}

// ── Auto-save notes on blur
function saveNotes(profileName, jobKey) {
  const area = document.getElementById('notes-area');
  const indicator = document.getElementById('notes-saved');
  if (!area) return;

  fetch(`/profile/${profileName}/job/${jobKey}/notes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ notes: area.value }),
  })
    .then(r => r.json())
    .then(data => {
      if (data.ok && indicator) {
        indicator.style.display = 'block';
        setTimeout(() => { indicator.style.display = 'none'; }, 2000);
      }
    })
    .catch(console.error);
}

// ── Theme Switcher ────────────────────────────────────────────────
//
// To add a new theme, append one object to THEMES. That's it.
//
// Required fields:
//   id      — unique kebab-case string, used as html[data-theme] value
//   name    — display name shown in the theme panel
//   colors  — 4 hex strings for the navbar preview swatches: [bg, primary, surface-2, surface]
//   vars    — CSS custom-property overrides; every key must start with "--"
//
// Tip — if your primary color is light (e.g. yellow, soft orange) and white text
// on buttons looks washed out, add  "--btn-fg": "<dark-color>"  to vars.
// For dark themes, also add the structural component overrides to style.css
// (see the "Midnight theme" block there as a reference).
//
const THEMES = [
  // ──────────────────────────────────────────────────────────────── Black
  {
    id: 'black',
    name: 'Black',
    colors: ['#050505', '#00E888', '#0A0A0A', '#070707'],
    vars: {
      '--bg':            '#050505',
      '--surface':       'rgba(255,255,255,0.09)',
      '--surface-2':     'rgba(255,255,255,0.06)',
      '--surface-3':     'rgba(255,255,255,0.11)',
      '--border':        'rgba(255,255,255,0.18)',
      '--border-light':  'rgba(255,255,255,0.09)',
      '--text':          '#F0FFF8',
      '--text-muted':    '#88C8A0',
      '--text-subtle':   '#406050',
      '--primary':       '#00E888',
      '--primary-hover': '#00C870',
      '--primary-light': 'rgba(0,232,136,0.25)',
      '--accent':        '#00E888',
      '--accent-light':  'rgba(0,232,136,0.20)',
      '--teal':          '#00D8C8',
      '--teal-light':    'rgba(0,216,200,0.22)',
      '--rose':          '#FF6090',
      '--rose-light':    'rgba(255,96,144,0.28)',
      '--orange':        '#FF9040',
      '--orange-light':  'rgba(255,144,64,0.28)',
      '--success':       '#00E888',
      '--success-light': 'rgba(0,232,136,0.25)',
      '--warning':       '#FFD020',
      '--warning-light': 'rgba(255,208,32,0.28)',
      '--danger':        '#FF5050',
      '--danger-light':  'rgba(255,80,80,0.28)',
      '--info':          '#50B8FF',
      '--info-light':    'rgba(80,184,255,0.22)',
      '--sky':           '#70C8FF',
      '--sky-light':     'rgba(112,200,255,0.22)',
      '--shadow-xs':     '0 1px 4px rgba(0,0,0,.90)',
      '--shadow-sm':     '0 2px 16px rgba(0,0,0,.80)',
      '--shadow':        '0 8px 32px rgba(0,0,0,.90)',
      '--shadow-lg':     '0 20px 60px rgba(0,0,0,1)',
      '--btn-fg':        '#fff',
    },
  },
  // ──────────────────────────────────────────────────────────────── Midnight
  {
    id: 'midnight',
    name: 'Midnight',
    colors: ['#060B07', '#F0B840', '#0D1A0E', '#0A1209'],
    vars: {
      '--bg':            '#060B07',
      '--surface':       'rgba(255,255,255,0.09)',
      '--surface-2':     'rgba(255,255,255,0.06)',
      '--surface-3':     'rgba(255,255,255,0.11)',
      '--border':        'rgba(255,255,255,0.18)',
      '--border-light':  'rgba(255,255,255,0.09)',
      '--text':          '#F5EEDD',
      '--text-muted':    '#C8B070',
      '--text-subtle':   '#6A7060',
      '--primary':       '#F0B840',
      '--primary-hover': '#D09820',
      '--primary-light': 'rgba(240,184,64,0.28)',
      '--accent':        '#F0B840',
      '--accent-light':  'rgba(240,184,64,0.22)',
      '--teal':          '#70C890',
      '--teal-light':    'rgba(112,200,144,0.22)',
      '--rose':          '#F0A070',
      '--rose-light':    'rgba(240,160,112,0.25)',
      '--orange':        '#F08050',
      '--orange-light':  'rgba(240,128,80,0.25)',
      '--success':       '#60D080',
      '--success-light': 'rgba(96,208,128,0.25)',
      '--warning':       '#F0B840',
      '--warning-light': 'rgba(240,184,64,0.28)',
      '--danger':        '#F06050',
      '--danger-light':  'rgba(240,96,80,0.28)',
      '--info':          '#70C890',
      '--info-light':    'rgba(112,200,144,0.22)',
      '--sky':           '#80D0B0',
      '--sky-light':     'rgba(128,208,176,0.22)',
      '--shadow-xs':     '0 1px 4px rgba(0,0,0,.90)',
      '--shadow-sm':     '0 2px 16px rgba(0,0,0,.80)',
      '--shadow':        '0 8px 32px rgba(0,0,0,.90)',
      '--shadow-lg':     '0 20px 60px rgba(0,0,0,1)',
      '--btn-fg':        '#fff',
    },
  },
  // ───────────────────────────────────────────────────────────────── Ocean
  {
    id: 'ocean',
    name: 'Ocean',
    colors: ['#050C14', '#28A8F0', '#0A1828', '#071018'],
    vars: {
      '--bg':            '#050C14',
      '--surface':       'rgba(255,255,255,0.09)',
      '--surface-2':     'rgba(255,255,255,0.06)',
      '--surface-3':     'rgba(255,255,255,0.11)',
      '--border':        'rgba(255,255,255,0.18)',
      '--border-light':  'rgba(255,255,255,0.09)',
      '--text':          '#D8F0FF',
      '--text-muted':    '#80B8D8',
      '--text-subtle':   '#406080',
      '--primary':       '#28A8F0',
      '--primary-hover': '#1888D0',
      '--primary-light': 'rgba(40,168,240,0.28)',
      '--accent':        '#28A8F0',
      '--accent-light':  'rgba(40,168,240,0.22)',
      '--teal':          '#20D8E8',
      '--teal-light':    'rgba(32,216,232,0.22)',
      '--rose':          '#A060E0',
      '--rose-light':    'rgba(160,96,224,0.25)',
      '--orange':        '#40D0FF',
      '--orange-light':  'rgba(64,208,255,0.22)',
      '--success':       '#30E8A0',
      '--success-light': 'rgba(48,232,160,0.25)',
      '--warning':       '#40D0FF',
      '--warning-light': 'rgba(64,208,255,0.22)',
      '--danger':        '#E05090',
      '--danger-light':  'rgba(224,80,144,0.28)',
      '--info':          '#20D8E8',
      '--info-light':    'rgba(32,216,232,0.22)',
      '--sky':           '#50E0FF',
      '--sky-light':     'rgba(80,224,255,0.22)',
      '--shadow-xs':     '0 1px 4px rgba(0,0,0,.90)',
      '--shadow-sm':     '0 2px 16px rgba(0,0,0,.80)',
      '--shadow':        '0 8px 32px rgba(0,0,0,.90)',
      '--shadow-lg':     '0 20px 60px rgba(0,0,0,1)',
      '--btn-fg':        '#fff',
    },
  },
  // ───────────────────────────────────────────────────────────────── Sunset
  {
    id: 'sunset',
    name: 'Sunset',
    colors: ['#100806', '#F06020', '#200E08', '#180A04'],
    vars: {
      '--bg':            '#100806',
      '--surface':       'rgba(255,255,255,0.09)',
      '--surface-2':     'rgba(255,255,255,0.06)',
      '--surface-3':     'rgba(255,255,255,0.11)',
      '--border':        'rgba(255,255,255,0.18)',
      '--border-light':  'rgba(255,255,255,0.09)',
      '--text':          '#FFF0E0',
      '--text-muted':    '#D09060',
      '--text-subtle':   '#705030',
      '--primary':       '#F06020',
      '--primary-hover': '#D04010',
      '--primary-light': 'rgba(240,96,32,0.30)',
      '--accent':        '#F06020',
      '--accent-light':  'rgba(240,96,32,0.25)',
      '--teal':          '#F0A040',
      '--teal-light':    'rgba(240,160,64,0.25)',
      '--rose':          '#E84060',
      '--rose-light':    'rgba(232,64,96,0.28)',
      '--orange':        '#F08030',
      '--orange-light':  'rgba(240,128,48,0.28)',
      '--success':       '#A0D060',
      '--success-light': 'rgba(160,208,96,0.25)',
      '--warning':       '#F0A040',
      '--warning-light': 'rgba(240,160,64,0.28)',
      '--danger':        '#F03040',
      '--danger-light':  'rgba(240,48,64,0.28)',
      '--info':          '#F0C060',
      '--info-light':    'rgba(240,192,96,0.22)',
      '--sky':           '#F0C060',
      '--sky-light':     'rgba(240,192,96,0.22)',
      '--shadow-xs':     '0 1px 4px rgba(0,0,0,.90)',
      '--shadow-sm':     '0 2px 16px rgba(0,0,0,.80)',
      '--shadow':        '0 8px 32px rgba(0,0,0,.90)',
      '--shadow-lg':     '0 20px 60px rgba(0,0,0,1)',
      '--btn-fg':        '#fff',
    },
  },
  // ──────────────────────────────────────────────────────────────── Pine
  {
    id: 'pine',
    name: 'Pine',
    colors: ['#080F08', '#E04858', '#0D1A0D', '#0A1208'],
    vars: {
      '--bg':            '#080F08',
      '--surface':       'rgba(255,255,255,0.09)',
      '--surface-2':     'rgba(255,255,255,0.06)',
      '--surface-3':     'rgba(255,255,255,0.11)',
      '--border':        'rgba(255,255,255,0.18)',
      '--border-light':  'rgba(255,255,255,0.09)',
      '--text':          '#F0F5F0',
      '--text-muted':    '#B8C8B8',
      '--text-subtle':   '#6A806A',
      '--primary':       '#E04858',
      '--primary-hover': '#C03040',
      '--primary-light': 'rgba(224,72,88,0.30)',
      '--accent':        '#E04858',
      '--accent-light':  'rgba(224,72,88,0.25)',
      '--teal':          '#40D4F8',
      '--teal-light':    'rgba(64,212,248,0.20)',
      '--rose':          '#E04858',
      '--rose-light':    'rgba(224,72,88,0.30)',
      '--orange':        '#F5981A',
      '--orange-light':  'rgba(245,152,26,0.28)',
      '--success':       '#4EC87A',
      '--success-light': 'rgba(78,200,122,0.25)',
      '--warning':       '#F5981A',
      '--warning-light': 'rgba(245,152,26,0.28)',
      '--danger':        '#F04040',
      '--danger-light':  'rgba(240,64,64,0.28)',
      '--info':          '#40D4F8',
      '--info-light':    'rgba(64,212,248,0.20)',
      '--sky':           '#40D4F8',
      '--sky-light':     'rgba(64,212,248,0.20)',
      '--shadow-xs':     '0 1px 4px rgba(0,0,0,.90)',
      '--shadow-sm':     '0 2px 16px rgba(0,0,0,.80)',
      '--shadow':        '0 8px 32px rgba(0,0,0,.90)',
      '--shadow-lg':     '0 20px 60px rgba(0,0,0,1)',
      '--btn-fg':        '#fff',
    },
  },
  // ──────────────────────────────────────────────────────────────── Fjord
  {
    id: 'fjord',
    name: 'Fjord',
    colors: ['#050810', '#6EB8E0', '#0A1020', '#071018'],
    vars: {
      '--bg':            '#050810',
      '--surface':       'rgba(255,255,255,0.09)',
      '--surface-2':     'rgba(255,255,255,0.06)',
      '--surface-3':     'rgba(255,255,255,0.11)',
      '--border':        'rgba(255,255,255,0.18)',
      '--border-light':  'rgba(255,255,255,0.09)',
      '--text':          '#E8F0FF',
      '--text-muted':    '#90B0D0',
      '--text-subtle':   '#406080',
      '--primary':       '#D4B880',
      '--primary-hover': '#B89860',
      '--primary-light': 'rgba(212,184,128,0.28)',
      '--accent':        '#D4B880',
      '--accent-light':  'rgba(212,184,128,0.22)',
      '--teal':          '#6EB8E0',
      '--teal-light':    'rgba(110,184,224,0.22)',
      '--rose':          '#E0A080',
      '--rose-light':    'rgba(224,160,128,0.25)',
      '--orange':        '#D8C080',
      '--orange-light':  'rgba(216,192,128,0.25)',
      '--success':       '#80D890',
      '--success-light': 'rgba(128,216,144,0.25)',
      '--warning':       '#D4B880',
      '--warning-light': 'rgba(212,184,128,0.28)',
      '--danger':        '#E08080',
      '--danger-light':  'rgba(224,128,128,0.28)',
      '--info':          '#6EB8E0',
      '--info-light':    'rgba(110,184,224,0.22)',
      '--sky':           '#90D0F0',
      '--sky-light':     'rgba(144,208,240,0.22)',
      '--shadow-xs':     '0 1px 4px rgba(0,0,0,.90)',
      '--shadow-sm':     '0 2px 16px rgba(0,0,0,.80)',
      '--shadow':        '0 8px 32px rgba(0,0,0,.90)',
      '--shadow-lg':     '0 20px 60px rgba(0,0,0,1)',
      '--btn-fg':        '#fff',
    },
  },
];

const THEME_KEY = 'jobwise-theme';
const THEME_HIDDEN_KEY = 'jobwise-themes-hidden';

function _currentThemeId() {
  return document.getElementById('html-root').getAttribute('data-theme');
}

function _hiddenThemeIds() {
  try { return JSON.parse(localStorage.getItem(THEME_HIDDEN_KEY) || '[]'); } catch (_) { return []; }
}

function _saveHiddenThemeIds(ids) {
  try { localStorage.setItem(THEME_HIDDEN_KEY, JSON.stringify(ids)); } catch (_) {}
}

function _visibleThemes() {
  const hidden = _hiddenThemeIds();
  return THEMES.filter(t => !hidden.includes(t.id));
}

function _applyTheme(id) {
  document.getElementById('html-root').setAttribute('data-theme', id);
  try { localStorage.setItem(THEME_KEY, id); } catch (_) {}
  _renderThemeBtn();
  _renderThemeOptions();
}

function _renderThemeBtn() {
  const swatches = document.getElementById('themeSwatches');
  const label = document.getElementById('themeBtnLabel');
  if (!swatches || !label) return;

  const currentId = _currentThemeId();
  const theme = THEMES.find(t => t.id === currentId) || THEMES[0];

  swatches.innerHTML = theme.colors.slice(0, 3).map(c =>
    `<span class="theme-swatch-dot" style="background:${c}"></span>`
  ).join('');
  label.textContent = theme.name;
}

function _renderThemeOptions() {
  const container = document.getElementById('themeOptions');
  if (!container) return;
  const currentId = _currentThemeId();
  const hidden = _hiddenThemeIds();
  const hasHidden = hidden.length > 0;

  const optionsHtml = _visibleThemes().map(theme => {
    const active = theme.id === currentId ? ' active' : '';
    const bars = theme.colors.map(c =>
      `<span class="theme-preview-bar" style="background:${c}"></span>`
    ).join('');
    return `<div class="theme-option-wrap">
      <button class="theme-option${active}" onclick="selectTheme('${theme.id}')">
        <span class="theme-option-preview">${bars}</span>
        <span class="theme-option-name">${theme.name}</span>
      </button>
      <button class="theme-delete-btn" onclick="deleteTheme(event,'${theme.id}','${theme.name}')" title="Remove ${theme.name} theme" aria-label="Remove ${theme.name} theme">×</button>
    </div>`;
  }).join('');

  const restoreHtml = hasHidden
    ? `<button class="theme-restore-btn" onclick="restoreAllThemes()">Restore ${hidden.length} hidden theme${hidden.length > 1 ? 's' : ''}</button>`
    : '';

  container.innerHTML = optionsHtml + restoreHtml;
}

function selectTheme(id) {
  _applyTheme(id);
  closeThemePanel();
}

function deleteTheme(e, id, name) {
  e.stopPropagation();
  if (!confirm(`Remove the "${name}" theme from the list?\n\nYou can restore it later.`)) return;

  // If this theme is currently active, fall back to forest (or first available)
  if (_currentThemeId() === id) {
    const fallback = _visibleThemes().find(t => t.id !== id) || THEMES[0];
    _applyTheme(fallback.id);
  }

  const hidden = _hiddenThemeIds();
  if (!hidden.includes(id)) hidden.push(id);
  _saveHiddenThemeIds(hidden);
  _renderThemeOptions();
}

function restoreAllThemes() {
  _saveHiddenThemeIds([]);
  _renderThemeOptions();
}

function toggleThemePanel() {
  const panel = document.getElementById('themePanel');
  const btn = document.getElementById('themeBtn');
  if (!panel) return;
  const opening = panel.hidden;
  panel.hidden = !opening;
  btn.setAttribute('aria-expanded', opening ? 'true' : 'false');
  if (opening) _renderThemeOptions();
}

function closeThemePanel() {
  const panel = document.getElementById('themePanel');
  const btn = document.getElementById('themeBtn');
  if (panel) panel.hidden = true;
  if (btn) btn.setAttribute('aria-expanded', 'false');
}

// Close panel on outside click
document.addEventListener('click', function(e) {
  const switcher = document.getElementById('themeSwitcher');
  if (switcher && !switcher.contains(e.target)) closeThemePanel();
});

// ── UTC → local time helpers ──────────────────────────────────────────────────
// fmt(): convert a SQLite UTC timestamp string to local M/D/YYYY H:MMam/pm.
// Used by source badges (data-date-found) and standalone date displays (data-utc-local).
(function formatDates() {
  function fmt(utcStr) {
    if (!utcStr) return null;
    // SQLite stores CURRENT_TIMESTAMP as "YYYY-MM-DD HH:MM:SS" (UTC)
    const d = new Date(utcStr.replace(' ', 'T') + 'Z');
    if (isNaN(d)) return null;
    const mo = d.getMonth() + 1;
    const dy = d.getDate();
    const yr = d.getFullYear();
    const h24 = d.getHours();
    const mi = String(d.getMinutes()).padStart(2, '0');
    const ampm = h24 >= 12 ? 'PM' : 'AM';
    const h12 = h24 % 12 || 12;
    return `${mo}/${dy}/${yr} ${h12}:${mi}${ampm}`;
  }
  document.addEventListener('DOMContentLoaded', function () {
    // Source badges: append "@timestamp" to badge label
    document.querySelectorAll('[data-date-found]').forEach(function (el) {
      const ts = fmt(el.dataset.dateFound);
      if (ts) el.textContent = el.textContent.trim() + '@' + ts;
    });
    // Standalone date fields: replace content entirely with local time
    document.querySelectorAll('[data-utc-local]').forEach(function (el) {
      const ts = fmt(el.dataset.utcLocal);
      if (ts) el.textContent = ts;
    });
  });
})();

// Init: restore saved theme (default to pine on first visit)
(function initTheme() {
  try {
    const saved = localStorage.getItem(THEME_KEY) || 'pine';
    document.getElementById('html-root').setAttribute('data-theme', saved);
    localStorage.setItem(THEME_KEY, saved);
  } catch (_) {
    document.getElementById('html-root').setAttribute('data-theme', 'pine');
  }
  _renderThemeBtn();
})();
