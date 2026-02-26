"""
Vercel Serverless Function: /api/action
브라우저에서 직접 호출 — GitHub Actions 없이 즉시 처리
"""
from http.server import BaseHTTPRequestHandler
import json, os, requests, msal
from datetime import datetime, timezone, timedelta

# ── 모듈 레벨 스키마 캐시 (같은 인스턴스 재사용 시 빠름) ─────
_todo_schema_info = None
_planner_schema_info = None


def _env(key, default=""):
    return os.environ.get(key, default)


def _raw_to_uuid(raw):
    r = raw.replace("-", "")
    return f"{r[:8]}-{r[8:12]}-{r[12:16]}-{r[16:20]}-{r[20:]}"


NOTION_DB_ID = _raw_to_uuid(_env("NOTION_DB_ID", "dadf27b55389404296df607af4d16e26"))
PLANNER_DB_ID = _raw_to_uuid(_env("PLANNER_DB_ID", "468bf987e6cd4372abf96a8f30f165b1"))


def _notion_headers():
    return {
        "Authorization": f'Bearer {_env("NOTION_TOKEN")}',
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


# ── MS 인증 ───────────────────────────────────────────────
def _ms_token():
    app = msal.PublicClientApplication(
        _env("AZURE_CLIENT_ID"),
        authority="https://login.microsoftonline.com/consumers",
    )
    result = app.acquire_token_by_refresh_token(
        _env("AZURE_REFRESH_TOKEN"),
        scopes=[
            "https://graph.microsoft.com/Tasks.ReadWrite",
            "https://graph.microsoft.com/User.Read",
        ],
    )
    if "access_token" not in result:
        raise RuntimeError(f"MS 인증 실패: {result.get('error_description', 'unknown')}")
    return result["access_token"]


def _ms_headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_list_id(token):
    list_id = _env("MSTODO_LIST_ID")
    if list_id:
        return list_id
    r = requests.get(
        "https://graph.microsoft.com/v1.0/me/todo/lists",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["value"][0]["id"]


# ── Notion 스키마 탐지 ────────────────────────────────────
def _detect_status_values(props, prop_name):
    opts = props.get(prop_name, {}).get("status", {})
    options = opts.get("options", [])
    groups = opts.get("groups", [])
    done_group = next((g for g in groups if g.get("name") in ("Complete", "완료됨")), None)
    todo_group = next((g for g in groups if g.get("name") in ("To-do", "할 일")), None)
    done_ids = set(done_group.get("option_ids", [])) if done_group else set()
    todo_ids = set(todo_group.get("option_ids", [])) if todo_group else set()
    done = next((o["name"] for o in options if o["id"] in done_ids), None)
    todo = next((o["name"] for o in options if o["id"] in todo_ids), None)
    if not done:
        done = next((o["name"] for o in options if o["name"] in ("완료", "Done", "Completed", "완료됨")), None)
    if not todo:
        todo = next((o["name"] for o in options if o["name"] in ("시작 안 함", "Not started", "할 일", "예정", "To-do")), None)
    if not done and options:
        done = options[-1]["name"]
    if not todo and options:
        todo = options[0]["name"]
    return done, todo


def _todo_schema():
    global _todo_schema_info
    if _todo_schema_info:
        return _todo_schema_info
    r = requests.get(f"https://api.notion.com/v1/databases/{NOTION_DB_ID}", headers=_notion_headers(), timeout=30)
    r.raise_for_status()
    props = r.json()["properties"]
    title_prop = next((n for n, p in props.items() if p["type"] == "title"), "이름")
    status_prop = next((n for n, p in props.items() if p["type"] == "status"), "상태")
    date_prop = next((n for n, p in props.items() if p["type"] == "date"), None)
    id_prop = next(
        (n for n, p in props.items() if p["type"] == "rich_text" and any(k in n.lower() for k in ("todo", "id", "ms"))),
        None,
    )
    importance_prop = next((n for n, p in props.items() if p["type"] == "select" and "중요" in n), None)
    importance_options = props.get(importance_prop, {}).get("select", {}).get("options", []) if importance_prop else []
    done_value, todo_value = _detect_status_values(props, status_prop)
    _todo_schema_info = {
        "title_prop": title_prop,
        "status_prop": status_prop,
        "done_value": done_value,
        "todo_value": todo_value,
        "date_prop": date_prop,
        "id_prop": id_prop,
        "importance_prop": importance_prop,
        "importance_options": importance_options,
    }
    return _todo_schema_info


def _planner_schema():
    global _planner_schema_info
    if _planner_schema_info:
        return _planner_schema_info
    r = requests.get(f"https://api.notion.com/v1/databases/{PLANNER_DB_ID}", headers=_notion_headers(), timeout=30)
    r.raise_for_status()
    props = r.json()["properties"]
    title_prop = next((n for n, p in props.items() if p["type"] == "title"), "이름")
    date_prop = next((n for n, p in props.items() if p["type"] == "date"), None)
    checkbox_prop = next((n for n, p in props.items() if p["type"] == "checkbox" and n == "완료"), None)
    if checkbox_prop:
        comp_prop, comp_type, done_value, todo_value = checkbox_prop, "checkbox", None, None
    else:
        comp_prop = next((n for n, p in props.items() if p["type"] == "status"), "상태")
        comp_type = "status"
        done_value, todo_value = _detect_status_values(props, comp_prop)
    _planner_schema_info = {
        "title_prop": title_prop,
        "date_prop": date_prop,
        "comp_prop": comp_prop,
        "comp_type": comp_type,
        "done_value": done_value,
        "todo_value": todo_value,
    }
    return _planner_schema_info


# ── Notion API 헬퍼 ───────────────────────────────────────
def _notion_update(page_id, completed, status_prop, done_value, todo_value, comp_type="status",
                   date_prop=None, due_date=None, title_prop=None, title=None,
                   importance_prop=None, importance_value=None):
    props = {}
    if comp_type == "checkbox":
        props[status_prop] = {"checkbox": completed}
    else:
        props[status_prop] = {"status": {"name": done_value if completed else todo_value}}
    if date_prop is not None:
        props[date_prop] = {"date": {"start": due_date}} if due_date else {"date": None}
    if title_prop and title is not None:
        props[title_prop] = {"title": [{"text": {"content": title}}]}
    if importance_prop and importance_value is not None:
        props[importance_prop] = {"select": {"name": importance_value}}
    requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=_notion_headers(), json={"properties": props}, timeout=30,
    ).raise_for_status()


def _notion_create(db_id, title, completed, title_prop, status_prop, done_value, todo_value,
                   date_prop=None, due_date=None, id_prop=None, ms_task_id=None,
                   importance_prop=None, importance_value=None):
    props = {
        title_prop: {"title": [{"text": {"content": title}}]},
        status_prop: {"status": {"name": done_value if completed else todo_value}},
    }
    if date_prop and due_date:
        props[date_prop] = {"date": {"start": due_date}}
    if id_prop and ms_task_id:
        props[id_prop] = {"rich_text": [{"text": {"content": ms_task_id}}]}
    if importance_prop and importance_value:
        props[importance_prop] = {"select": {"name": importance_value}}
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=_notion_headers(),
        json={"parent": {"database_id": db_id}, "properties": props},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _ms_importance_to_notion(ms_val, options):
    names = [o["name"] for o in options]
    if not names:
        return None
    if ms_val == "high":
        for c in ("높음", "중요", "High", "Important"):
            if c in names:
                return c
        return names[-1]
    if ms_val == "low":
        for c in ("낮음", "Low"):
            if c in names:
                return c
        return names[0]
    for c in ("보통", "일반", "Normal", "Medium", "중간"):
        if c in names:
            return c
    return names[len(names) // 2] if len(names) >= 3 else None


# ── 액션 핸들러 ───────────────────────────────────────────
def handle_planner_toggle(notion_id, completed):
    s = _planner_schema()
    _notion_update(notion_id, completed, s["comp_prop"], s["done_value"], s["todo_value"], comp_type=s["comp_type"])
    return {"ok": True}


def handle_planner_update(notion_id, body):
    s = _planner_schema()
    title = body.get("title", "").strip()
    due_date_raw = body.get("due_date", "")
    due_date = None if due_date_raw in ("", "none") else due_date_raw
    completed = body.get("completed", "false").lower() == "true"
    _notion_update(
        notion_id, completed, s["comp_prop"], s["done_value"], s["todo_value"],
        comp_type=s["comp_type"],
        date_prop=s["date_prop"] if due_date_raw else None,
        due_date=due_date,
        title_prop=s["title_prop"] if title else None,
        title=title if title else None,
    )
    return {"ok": True}


def handle_planner_delete(notion_id):
    requests.patch(
        f"https://api.notion.com/v1/pages/{notion_id}",
        headers=_notion_headers(), json={"archived": True}, timeout=30,
    ).raise_for_status()
    return {"ok": True}


def handle_toggle_complete(ms_id, notion_id, completed):
    s = _todo_schema()
    token = _ms_token()
    list_id = _get_list_id(token)
    requests.patch(
        f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{ms_id}",
        headers=_ms_headers(token),
        json={"status": "completed" if completed else "notStarted"},
        timeout=30,
    ).raise_for_status()
    if notion_id:
        _notion_update(notion_id, completed, s["status_prop"], s["done_value"], s["todo_value"])
    return {"ok": True}


def handle_update(ms_id, notion_id, body):
    s = _todo_schema()
    token = _ms_token()
    list_id = _get_list_id(token)
    title = body.get("title", "").strip()
    due_date_raw = body.get("due_date", "")
    due_time = body.get("due_time", "") or None
    importance = body.get("importance", "")
    completed_str = body.get("completed", "")

    ms_body = {}
    if completed_str:
        ms_body["status"] = "completed" if completed_str.lower() == "true" else "notStarted"
    if title:
        ms_body["title"] = title
    if due_date_raw:
        if due_date_raw == "none":
            ms_body["dueDateTime"] = None
        else:
            t = due_time or "00:00"
            ms_body["dueDateTime"] = {"dateTime": f"{due_date_raw}T{t}:00.0000000", "timeZone": "Korea Standard Time"}
    if importance:
        ms_body["importance"] = importance
    if ms_body:
        requests.patch(
            f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{ms_id}",
            headers=_ms_headers(token), json=ms_body, timeout=30,
        ).raise_for_status()

    if notion_id:
        completed = completed_str.lower() == "true" if completed_str else False
        due_date = None if due_date_raw in ("", "none") else due_date_raw
        notion_imp = _ms_importance_to_notion(importance, s["importance_options"]) if importance and s["importance_prop"] else None
        _notion_update(
            notion_id, completed, s["status_prop"], s["done_value"], s["todo_value"],
            date_prop=s["date_prop"] if due_date_raw else None,
            due_date=due_date,
            title_prop=s["title_prop"] if title else None,
            title=title if title else None,
            importance_prop=s["importance_prop"] if notion_imp else None,
            importance_value=notion_imp,
        )
    return {"ok": True}


def handle_create(body):
    s = _todo_schema()
    token = _ms_token()
    list_id = _get_list_id(token)
    title = body.get("title", "").strip()
    due_date = body.get("due_date") or None
    if due_date == "none":
        due_date = None
    due_time = body.get("due_time") or None
    importance = body.get("importance", "normal") or "normal"

    ms_body = {"title": title, "status": "notStarted", "importance": importance}
    if due_date:
        t = due_time or "00:00"
        ms_body["dueDateTime"] = {"dateTime": f"{due_date}T{t}:00.0000000", "timeZone": "Korea Standard Time"}
    task_r = requests.post(
        f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks",
        headers=_ms_headers(token), json=ms_body, timeout=30,
    )
    task_r.raise_for_status()
    ms_task_id = task_r.json()["id"]

    notion_imp = _ms_importance_to_notion(importance, s["importance_options"]) if s["importance_prop"] else None
    page = _notion_create(
        NOTION_DB_ID, title, False,
        s["title_prop"], s["status_prop"], s["done_value"], s["todo_value"],
        date_prop=s["date_prop"], due_date=due_date,
        id_prop=s["id_prop"], ms_task_id=ms_task_id,
        importance_prop=s["importance_prop"], importance_value=notion_imp,
    )
    return {
        "ok": True,
        "task": {
            "ms_id": ms_task_id,
            "notion_id": page["id"],
            "title": title,
            "completed": False,
            "due_date": due_date,
            "due_time": due_time,
            "importance": importance,
        },
    }


def handle_delete(ms_id, notion_id):
    token = _ms_token()
    list_id = _get_list_id(token)
    requests.delete(
        f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{ms_id}",
        headers={"Authorization": f"Bearer {token}"}, timeout=30,
    ).raise_for_status()
    if notion_id:
        requests.patch(
            f"https://api.notion.com/v1/pages/{notion_id}",
            headers=_notion_headers(), json={"archived": True}, timeout=30,
        ).raise_for_status()
    return {"ok": True}


def route(body):
    action = body.get("action", "")
    task_id = body.get("task_id", "")
    ms_id = body.get("ms_id") or task_id
    notion_id = body.get("notion_id", "")

    if action == "planner_toggle":
        return handle_planner_toggle(task_id, body.get("completed", "false").lower() == "true")
    if action == "planner_update":
        return handle_planner_update(task_id, body)
    if action == "planner_delete":
        return handle_planner_delete(task_id)
    if action == "toggle_complete":
        return handle_toggle_complete(ms_id, notion_id, body.get("completed", "false").lower() == "true")
    if action == "update":
        return handle_update(ms_id, notion_id, body)
    if action == "create":
        return handle_create(body)
    if action == "delete":
        return handle_delete(ms_id, notion_id)
    return {"ok": False, "error": f"Unknown action: {action}"}


# ── Vercel Handler ────────────────────────────────────────
class handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Vercel이 자체 로깅 처리

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-API-Key")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(content_len) or b"{}")
        except Exception:
            body = {}

        try:
            result = route(body)
            self.send_response(200)
        except Exception as e:
            result = {"ok": False, "error": str(e)}
            self.send_response(500)

        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(result, ensure_ascii=False).encode())
