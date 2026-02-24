const REPO = "aveicea/todo";
const WORKFLOW_FILE = "sync.yml";
const STATUS_URL =
  "https://raw.githubusercontent.com/aveicea/todo/main/data/status.json";

// ── Status loading ──────────────────────────────────────
async function loadStatus() {
  try {
    const r = await fetch(STATUS_URL + "?t=" + Date.now());
    if (!r.ok) throw new Error("404");
    const data = await r.json();

    const syncTime = new Date(data.last_sync);
    const diffMin = Math.round((Date.now() - syncTime) / 60000);
    const timeStr =
      diffMin < 1
        ? "방금"
        : diffMin < 60
        ? `${diffMin}분 전`
        : `${Math.floor(diffMin / 60)}시간 전`;

    setText("last-sync", timeStr);
    setText("ms-count", `${data.total_ms_tasks ?? "-"}개`);
    setText("notion-count", `${data.total_notion_pages ?? "-"}개`);

    const badge = document.getElementById("badge");
    if (data.success) {
      badge.textContent = "동기화됨";
      badge.className = "badge ok";
    } else {
      badge.textContent = "오류";
      badge.className = "badge error";
    }
  } catch {
    setText("last-sync", "없음");
    const badge = document.getElementById("badge");
    badge.textContent = "미설정";
    badge.className = "badge";
  }
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

// ── Sync trigger ────────────────────────────────────────
async function triggerSync() {
  const pat = localStorage.getItem("gh_pat");
  if (!pat) {
    showSetup();
    return;
  }

  const btn = document.getElementById("sync-btn");
  btn.disabled = true;
  btn.textContent = "동기화 요청 중...";
  btn.className = "sync-btn";

  try {
    const r = await fetch(
      `https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW_FILE}/dispatches`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${pat}`,
          Accept: "application/vnd.github+json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ ref: "main" }),
      }
    );

    if (r.status === 204) {
      btn.textContent = "시작됨 ✓ (약 1분 후 완료)";
      btn.className = "sync-btn success";
      // 90초 후 상태 새로고침
      setTimeout(loadStatus, 90000);
      setTimeout(() => {
        btn.textContent = "지금 동기화";
        btn.className = "sync-btn";
        btn.disabled = false;
      }, 5000);
    } else {
      const body = await r.json().catch(() => ({}));
      const msg = body.message || `HTTP ${r.status}`;
      throw new Error(msg);
    }
  } catch (e) {
    btn.textContent = `오류: ${e.message}`;
    btn.className = "sync-btn err";
    btn.disabled = false;
    if (e.message.includes("Bad credentials") || e.message.includes("401")) {
      localStorage.removeItem("gh_pat");
      showSetup();
    }
  }
}

// ── Setup panel ─────────────────────────────────────────
function toggleSetup() {
  const panel = document.getElementById("setup-panel");
  panel.classList.toggle("visible");
}

function showSetup() {
  document.getElementById("setup-panel").classList.add("visible");
}

function savePAT() {
  const val = document.getElementById("pat-input").value.trim();
  if (!val) return;
  localStorage.setItem("gh_pat", val);
  document.getElementById("pat-input").value = "";
  document.getElementById("setup-panel").classList.remove("visible");
}

// Allow saving PAT with Enter key
document.addEventListener("DOMContentLoaded", () => {
  const input = document.getElementById("pat-input");
  if (input) {
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter") savePAT();
    });
  }
  loadStatus();
});
