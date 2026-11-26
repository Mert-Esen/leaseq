'use strict';
const $ = id => document.getElementById(id);
const make = (tag, text, className) => {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = String(text);
  if (className) node.className = className;
  return node;
};
const queuePattern = /^[A-Za-z0-9._-]{1,64}$/;
const states = ['ready', 'delayed', 'leased', 'dead', 'completed'];
const rows = new Map(), retrying = new Set();
let selected = '', generation = 0, active = false, dirty = false;
const form = $('submit-form'), payload = $('payload');
const theme = $('theme'), media = matchMedia('(prefers-color-scheme: dark)');
try { theme.value = localStorage.getItem('leaseq-theme') || 'system'; } catch {}
if (!['system', 'light', 'dark'].includes(theme.value)) theme.value = 'system';
function applyTheme() {
  document.documentElement.dataset.theme = theme.value === 'system'
    ? (media.matches ? 'dark' : 'light') : theme.value;
}
theme.addEventListener('change', () => {
  applyTheme();
  try { localStorage.setItem('leaseq-theme', theme.value); } catch {}
});
media.addEventListener('change', applyTheme);
applyTheme();
async function request(url, body, rawPayload) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch(url, {
      signal: controller.signal, cache: 'no-store',
      ...(body === undefined ? {} : {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: rawPayload === undefined ? JSON.stringify(body)
          : JSON.stringify(body).slice(0, -1) + ',"payload":' + rawPayload + '}'
      })
    });
    if (response.status === 204 && response.ok) return null;
    const data = await response.json().catch(() => null);
    if (!response.ok) {
      const error = data?.error;
      throw new Error(error?.message
        ? String(error.message) + (error.code ? ' (' + error.code + ')' : '')
        : 'Request failed: HTTP ' + response.status);
    }
    if (data === null) throw new Error('The server returned an invalid response.');
    return data;
  } catch (error) {
    if (error.name === 'AbortError') throw new Error('Request timed out. Please try again.');
    throw error;
  } finally { clearTimeout(timer); }
}
function chooseQueue(name) {
  if (selected !== name) {
    selected = name; $('jobs').replaceChildren();
  }
  for (const [key, row] of rows) row.button.setAttribute('aria-pressed', String(key === name));
  $('jobs-title').textContent = 'Jobs · ' + name;
  $('job-controls').hidden = false;
  $('jobs-title').focus();
  refresh();
}
function showStats(queues) {
  const names = new Set();
  for (const queue of queues) {
    if (!queue || typeof queue.name !== 'string' || !queuePattern.test(queue.name)) continue;
    names.add(queue.name);
    let row = rows.get(queue.name);
    if (!row) {
      const tr = make('tr'), th = make('th'), button = make('button', queue.name, 'queue-button');
      th.scope = 'row'; button.type = 'button';
      button.setAttribute('aria-pressed', String(selected === queue.name));
      button.setAttribute('aria-controls', 'job-panel');
      button.addEventListener('click', () => chooseQueue(queue.name));
      th.append(button); tr.append(th);
      const cells = states.map(() => { const cell = make('td'); tr.append(cell); return cell; });
      row = { tr, button, cells }; rows.set(queue.name, row); $('queues').append(tr);
    }
    states.forEach((state, i) => { row.cells[i].textContent = String(queue[state] ?? 0); });
  }
  if (selected && !names.has(selected)) {
    const restoreFocus = $('jobs').contains(document.activeElement) || $('job-controls').contains(document.activeElement);
    selected = ''; generation++; dirty = false;
    $('jobs').replaceChildren(); $('jobs').setAttribute('aria-busy', 'false');
    $('job-controls').hidden = true; $('jobs-title').textContent = 'Jobs';
    $('job-status').textContent = 'Select a queue to view its jobs.';
    $('action-status').textContent = '';
    if (restoreFocus) $('jobs-title').focus();
  }
  for (const [name, row] of rows) {
    if (!names.has(name)) {
      const restoreFocus = row.tr.contains(document.activeElement);
      row.tr.remove(); rows.delete(name);
      if (restoreFocus) $('jobs-title').focus();
    }
  }
  $('queue-status').textContent = names.size ? count(names.size, 'queue') : 'No queues yet. Submit a job to get started.';
  refresh(false);
}
function count(n, noun) { return n + ' ' + noun + (n === 1 ? '' : 's'); }
function refresh(supersede = true) {
  if (!selected) return;
  // Explicit refreshes invalidate active results; SSE only queues a follow-up.
  if (supersede) generation++;
  dirty = true; void drain();
}
async function drain() {
  if (active || !dirty || !selected) return;
  active = true; dirty = false;
  const currentGeneration = generation, queue = selected, state = $('state').value;
  const isCurrent = () => generation === currentGeneration && selected === queue && $('state').value === state;
  $('jobs').setAttribute('aria-busy', 'true');
  $('job-status').textContent = $('jobs').childElementCount ? 'Refreshing jobs…' : 'Loading jobs…';
  try {
    const query = new URLSearchParams({ queue, state, limit: '50' });
    const data = await request('/api/jobs?' + query);
    if (!Array.isArray(data.jobs)) throw new Error('The server returned an invalid job list.');
    if (isCurrent()) {
      renderJobs(data.jobs);
      $('job-status').textContent = data.jobs.length
        ? count(data.jobs.length, 'job') + ' shown (up to 50).' : 'No ' + state + ' jobs in this queue.';
    }
  } catch (error) {
    if (isCurrent()) $('job-status').textContent = 'Could not load jobs: ' + error.message + ' Use Refresh to try again.';
  } finally {
    active = false;
    $('jobs').setAttribute('aria-busy', 'false');
    if (dirty) void drain();
  }
}
function renderJobs(jobs) {
  const focused = document.activeElement;
  const focusId = $('jobs').contains(focused) ? focused.dataset.job : undefined;
  const fragment = document.createDocumentFragment();
  for (const job of jobs) {
    const card = make('article', undefined, 'job');
    card.append(make('h3', 'Job #' + job.id));
    card.append(make('p', 'Queue: ' + job.queue + ' · State: ' + job.state));
    card.append(make('p', 'Priority: ' + job.priority + ' · Attempts: ' + job.attempts + '/' + job.max_attempts));
    card.append(make('h4', 'Payload'), make('pre', job.payload));
    card.append(make('h4', 'Last error'), make('pre', job.last_error ?? 'None'));
    if (job.state === 'dead') {
      const key = String(job.id), button = make('button', retrying.has(key) ? 'Retrying…' : 'Retry');
      button.type = 'button'; button.dataset.job = key; button.disabled = retrying.has(key);
      button.setAttribute('aria-label', 'Retry job ' + key);
      button.addEventListener('click', () => retry(job, button));
      card.append(button);
    }
    fragment.append(card);
  }
  $('jobs').replaceChildren(fragment);
  if (focusId !== undefined) {
    const replacement = [...$('jobs').querySelectorAll('button')].find(b => b.dataset.job === focusId);
    if (replacement && !replacement.disabled) replacement.focus();
    else $('jobs-title').focus();
  }
}
async function retry(job, button) {
  const key = String(job.id);
  if (retrying.has(key)) return;
  retrying.add(key); button.disabled = true; button.textContent = 'Retrying…';
  $('action-status').textContent = 'Retrying job ' + key + '…';
  try {
    await request('/api/jobs/' + encodeURIComponent(key) + '/retry', {});
    $('action-status').textContent = 'Job ' + key + ' queued for retry.';
    refresh();
  } catch (error) { $('action-status').textContent = 'Retry failed for job ' + key + ': ' + error.message; }
  finally {
    retrying.delete(key);
    for (const b of $('jobs').querySelectorAll('button')) {
      if (b.dataset.job === key) { b.disabled = false; b.textContent = 'Retry'; }
    }
  }
}
$('state').addEventListener('change', () => { $('jobs').replaceChildren(); refresh(); });
$('refresh').addEventListener('click', () => refresh());
form.addEventListener('invalid', event => event.target.setAttribute('aria-invalid', 'true'), true);
form.addEventListener('input', event => {
  event.target.setCustomValidity?.('');
  event.target.setAttribute('aria-invalid', 'false');
});
form.addEventListener('submit', async event => {
  event.preventDefault();
  if ($('submit-job').disabled) return;
  const rawPayload = payload.value;
  try { JSON.parse(rawPayload); } catch {
    payload.setCustomValidity('Enter valid JSON, such as {}, null, 42 or "hello".');
    payload.setAttribute('aria-invalid', 'true'); payload.reportValidity(); payload.focus(); return;
  }
  const queue = $('queue').value;
  const fields = [['priority', 0, 9], ['delay_ms', 0, Number.MAX_SAFE_INTEGER], ['max_attempts', 1, 100]];
  const body = {};
  if (!queuePattern.test(queue)) {
    $('queue').setCustomValidity('Use 1–64 ASCII letters, digits, dots, underscores or hyphens.');
  }
  for (const [id, min, max] of fields) {
    const field = $(id), n = field.valueAsNumber;
    if (!Number.isSafeInteger(n) || n < min || n > max) field.setCustomValidity('Enter a whole number from ' + min + ' to ' + max + '.');
    body[id] = n;
  }
  if (!form.reportValidity()) return;
  $('submit-job').disabled = true; form.setAttribute('aria-busy', 'true');
  $('submit-status').textContent = 'Submitting job…';
  try {
    const data = await request('/api/queues/' + encodeURIComponent(queue) + '/jobs', body, rawPayload);
    $('submit-status').textContent = 'Created job #' + data.id + ' in ' + queue + '.';
    if (selected === queue) refresh();
  } catch (error) { $('submit-status').textContent = 'Submission failed: ' + error.message; }
  finally { $('submit-job').disabled = false; form.setAttribute('aria-busy', 'false'); }
});
const events = new EventSource('/api/events');
events.addEventListener('open', () => { $('connection').textContent = 'Live'; });
events.addEventListener('error', () => { $('connection').textContent = 'Disconnected'; });
events.addEventListener('stats', event => {
  try {
    const data = JSON.parse(event.data);
    if (!Array.isArray(data.queues)) throw new Error('Invalid stats');
    showStats(data.queues);
  } catch { $('queue-status').textContent = 'Invalid stats received. Waiting for the next update.'; }
});
