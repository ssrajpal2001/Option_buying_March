function showToast(msg, type = 'success') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = `toast toast-${type} show`;
  setTimeout(() => { el.classList.remove('show'); }, 4000);
}

async function updateNavBotStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    const el = document.getElementById('nav-bot-status');
    const dot = el.querySelector('.pulse-dot');
    const label = el.querySelector('span:last-child');
    if (d.bot_running) {
      el.className = 'flex items-center gap-2 text-sm px-3 py-1 rounded-full badge-live';
      dot.className = 'pulse-dot pulse-green';
      label.textContent = (d.mode || 'LIVE');
    } else {
      el.className = 'flex items-center gap-2 text-sm px-3 py-1 rounded-full badge-stopped';
      dot.className = 'pulse-dot pulse-red';
      label.textContent = 'STOPPED';
    }
  } catch(e) {}
}

if (!window.location.pathname.includes('/')) {
  setInterval(updateNavBotStatus, 10000);
  updateNavBotStatus();
}
