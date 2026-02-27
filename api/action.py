"""
Vercel Serverless Function: /api/action
브라우저에서 직접 호출 — Notion 직접 읽기 + MS Todo/Notion 양방향 쓰기
"""
from http.server import BaseHTTPRequestHandler
import json, os, requests, msal

# ── 모듈 레벨 스키마 캐시 (같은 인스턴스 재사용 시 빠름) ─────
_todo_schema_info = None
_planner_schema_info = None


def _env(key, default=""):
    return os.environ.get(key, default)


def _raw_to_uuid(raw):
    r = raw.replace("-", "")
    return f"{r[:8]}-{r[8:12]}-{r[12:16]}-{r[16:20]}-{r[20:]}"


NOTION_DB_ID  = _raw_to_uuid(_env("NOTION_DB_ID",  "dadf27b55389404296df607af4d16e26"))
PLANNER_DB_ID = _raw_to_uuid(_env("PLANNER_DB_ID", "468bf987e6cd4372abf96a8f30f165b1"))
BOOK_DB_ID    = _raw_to_uuid("41c3889d4617465db9df008e96ca5af1")


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
    if not id_prop:
        try:
            pr = requests.patch(
                f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
                headers=_notion_headers(),
                json={"properties": {"MS Todo ID": {"rich_text": {}}}},
                timeout=30,
            )
            if pr.ok:
                id_prop = "MS Todo ID"
        except Exception:
            pass
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
    book_raw = "41c3889d4617465db9df008e96ca5af1"
    book_rel_prop = next(
        (n for n, p in props.items()
         if p["type"] == "relation" and p.get("relation", {}).get("database_id", "").replace("-", "") == book_raw),
        None,
    )
    _planner_schema_info = {
        "title_prop": title_prop,
        "date_prop": date_prop,
        "comp_prop": comp_prop,
        "comp_type": comp_type,
        "done_value": done_value,
        "todo_value": todo_value,
        "book_rel_prop": book_rel_prop,
    }
    return _planner_schema_info


# ── Notion API 헬퍼 ───────────────────────────────────────
def _notion_query_all(db_id):
    pages, cursor = [], None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers=_notion_headers(), json=body, timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


def _page_title(page, prop_name):
    items = page["properties"].get(prop_name, {}).get("title", [])
    return "".join(t.get("plain_text", "") for t in items)


def _page_date_time(page, prop_name):
    """Notion date 필드에서 (date_str, time_str) 추출."""
    if not prop_name:
        return None, None
    d = page["properties"].get(prop_name, {}).get("date")
    if not d or not d.get("start"):
        return None, None
    start = d["start"]
    date_str = start[:10]
    time_str = None
    if len(start) > 10 and "T" in start:
        time_part = start[11:16]
        if time_part and time_part != "00:00":
            time_str = time_part
    return date_str, time_str


def _page_date(page, prop_name):
    date_str, _ = _page_date_time(page, prop_name)
    return date_str


def _page_completed(page, comp_prop, done_value, comp_type):
    prop = page["properties"].get(comp_prop, {})
    if comp_type == "checkbox":
        return prop.get("checkbox", False)
    return prop.get("status", {}).get("name", "") == done_value


def _notion_patch(page_id, props):
    requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=_notion_headers(), json={"properties": props}, timeout=30,
    ).raise_for_status()


def _importance_to_notion(ms_val, options):
    """'high'/'normal'/'low' → Notion select option name"""
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


# ── Todo 액션 핸들러 ──────────────────────────────────────
def handle_get_tasks():
    """Notion DB에서 직접 읽기 (MS Todo API 호출 없음 — 빠름)"""
    s = _todo_schema()
    pages = _notion_query_all(NOTION_DB_ID)
    tasks = []
    for page in pages:
        title = _page_title(page, s["title_prop"])
        if not title.strip():
            continue
        # MS Todo ID (Notion에 저장된 값)
        ms_id = ""
        if s["id_prop"]:
            rt = page["properties"].get(s["id_prop"], {}).get("rich_text", [])
            ms_id = "".join(t.get("plain_text", "") for t in rt)
        completed = _page_completed(page, s["status_prop"], s["done_value"], "status")
        due_date, due_time = _page_date_time(page, s["date_prop"])
        importance = "normal"
        if s["importance_prop"]:
            sel = page["properties"].get(s["importance_prop"], {}).get("select")
            if sel:
                name = sel.get("name", "")
                if name in ("높음", "High", "Important", "중요"):
                    importance = "high"
                elif name in ("낮음", "Low"):
                    importance = "low"
        tasks.append({
            "notion_id": page["id"],
            "ms_id": ms_id,
            "title": title,
            "completed": completed,
            "due_date": due_date,
            "due_time": due_time,
            "importance": importance,
        })
    tasks.sort(key=lambda x: (x["completed"], x["due_date"] or "9999-12-31", x["due_time"] or "99:99"))
    return {"ok": True, "tasks": tasks}


def handle_toggle_complete(ms_id, notion_id, completed):
    s = _todo_schema()
    # MS Todo 업데이트
    if ms_id:
        token = _ms_token()
        list_id = _get_list_id(token)
        requests.patch(
            f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{ms_id}",
            headers=_ms_headers(token),
            json={"status": "completed" if completed else "notStarted"},
            timeout=30,
        ).raise_for_status()
    # Notion 업데이트
    if notion_id:
        _notion_patch(notion_id, {
            s["status_prop"]: {"status": {"name": s["done_value"] if completed else s["todo_value"]}}
        })
    return {"ok": True}


def handle_update(ms_id, notion_id, body):
    s = _todo_schema()
    title = body.get("title", "").strip()
    due_date_raw = body.get("due_date", "")
    due_time = body.get("due_time", "") or None
    importance = body.get("importance", "")
    completed_str = body.get("completed", "")

    # MS Todo 업데이트
    if ms_id:
        token = _ms_token()
        list_id = _get_list_id(token)
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

    # Notion 업데이트
    if notion_id:
        props = {}
        if title:
            props[s["title_prop"]] = {"title": [{"text": {"content": title}}]}
        if due_date_raw:
            if due_date_raw == "none":
                if s["date_prop"]:
                    props[s["date_prop"]] = {"date": None}
            else:
                if s["date_prop"]:
                    full_dt = f"{due_date_raw}T{due_time}:00+09:00" if due_time else due_date_raw
                    props[s["date_prop"]] = {"date": {"start": full_dt}}
        if completed_str:  # 명시적으로 전달된 경우에만 완료 상태 변경
            completed = completed_str.lower() == "true"
            props[s["status_prop"]] = {"status": {"name": s["done_value"] if completed else s["todo_value"]}}
        if importance and s["importance_prop"]:
            notion_imp = _importance_to_notion(importance, s["importance_options"])
            if notion_imp:
                props[s["importance_prop"]] = {"select": {"name": notion_imp}}
        if props:
            _notion_patch(notion_id, props)

    return {"ok": True}


def handle_create(body):
    s = _todo_schema()
    title = body.get("title", "").strip()
    due_date = body.get("due_date") or None
    if due_date == "none":
        due_date = None
    due_time = body.get("due_time") or None
    importance = body.get("importance", "normal") or "normal"

    # MS Todo 생성
    token = _ms_token()
    list_id = _get_list_id(token)
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

    # Notion 생성 (MS Todo ID 함께 저장)
    notion_imp = _importance_to_notion(importance, s["importance_options"]) if s["importance_prop"] else None
    props = {
        s["title_prop"]: {"title": [{"text": {"content": title}}]},
        s["status_prop"]: {"status": {"name": s["todo_value"]}},
    }
    if due_date and s["date_prop"]:
        full_dt = f"{due_date}T{due_time}:00+09:00" if due_time else due_date
        props[s["date_prop"]] = {"date": {"start": full_dt}}
    if s["id_prop"]:
        props[s["id_prop"]] = {"rich_text": [{"text": {"content": ms_task_id}}]}
    if s["importance_prop"] and notion_imp:
        props[s["importance_prop"]] = {"select": {"name": notion_imp}}
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=_notion_headers(),
        json={"parent": {"database_id": NOTION_DB_ID}, "properties": props},
        timeout=30,
    )
    r.raise_for_status()
    notion_page_id = r.json()["id"]

    return {
        "ok": True,
        "task": {
            "notion_id": notion_page_id,
            "ms_id": ms_task_id,
            "title": title,
            "completed": False,
            "due_date": due_date,
            "due_time": due_time,
            "importance": importance,
        },
    }


def handle_delete(ms_id, notion_id):
    # MS Todo 삭제
    if ms_id:
        token = _ms_token()
        list_id = _get_list_id(token)
        requests.delete(
            f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{ms_id}",
            headers={"Authorization": f"Bearer {token}"}, timeout=30,
        ).raise_for_status()
    # Notion 아카이브
    if notion_id:
        requests.patch(
            f"https://api.notion.com/v1/pages/{notion_id}",
            headers=_notion_headers(), json={"archived": True}, timeout=30,
        ).raise_for_status()
    return {"ok": True}


# ── Planner 액션 핸들러 ───────────────────────────────────
def handle_get_planner():
    s = _planner_schema()
    book_title_map = {}
    if s["book_rel_prop"]:
        book_schema_r = requests.get(
            f"https://api.notion.com/v1/databases/{BOOK_DB_ID}",
            headers=_notion_headers(), timeout=30,
        )
        book_schema_r.raise_for_status()
        book_props = book_schema_r.json()["properties"]
        book_title_prop = next((n for n, p in book_props.items() if p["type"] == "title"), "이름")
        for bp in _notion_query_all(BOOK_DB_ID):
            book_title_map[bp["id"]] = _page_title(bp, book_title_prop)

    pages = _notion_query_all(PLANNER_DB_ID)
    tasks = []
    for page in pages:
        title = _page_title(page, s["title_prop"])
        if not title.strip():
            continue
        book_title = None
        if s["book_rel_prop"]:
            rel_ids = [r["id"] for r in page["properties"].get(s["book_rel_prop"], {}).get("relation", [])]
            if rel_ids:
                book_title = book_title_map.get(rel_ids[0])
        tasks.append({
            "notion_id": page["id"],
            "title": title,
            "book_title": book_title or None,
            "completed": _page_completed(page, s["comp_prop"], s["done_value"], s["comp_type"]),
            "due_date": _page_date(page, s["date_prop"]),
            "due_time": None,
        })
    tasks.sort(key=lambda x: (x["completed"], x["due_date"] or "9999-12-31"))
    return {"ok": True, "tasks": tasks}


def handle_planner_toggle(notion_id, completed):
    s = _planner_schema()
    if s["comp_type"] == "checkbox":
        _notion_patch(notion_id, {s["comp_prop"]: {"checkbox": completed}})
    else:
        _notion_patch(notion_id, {s["comp_prop"]: {"status": {"name": s["done_value"] if completed else s["todo_value"]}}})
    return {"ok": True}


def handle_planner_update(notion_id, body):
    s = _planner_schema()
    title = body.get("title", "").strip()
    due_date_raw = body.get("due_date", "")
    due_date = None if due_date_raw in ("", "none") else due_date_raw
    completed = body.get("completed", "false").lower() == "true"

    props = {}
    if title:
        props[s["title_prop"]] = {"title": [{"text": {"content": title}}]}
    if due_date_raw:
        if s["date_prop"]:
            props[s["date_prop"]] = {"date": {"start": due_date}} if due_date else {"date": None}
    if s["comp_type"] == "checkbox":
        props[s["comp_prop"]] = {"checkbox": completed}
    else:
        props[s["comp_prop"]] = {"status": {"name": s["done_value"] if completed else s["todo_value"]}}
    if props:
        _notion_patch(notion_id, props)
    return {"ok": True}


def handle_planner_delete(notion_id):
    requests.patch(
        f"https://api.notion.com/v1/pages/{notion_id}",
        headers=_notion_headers(), json={"archived": True}, timeout=30,
    ).raise_for_status()
    return {"ok": True}


# ── 라우터 ─────────────────────────────────────────────────
def route(body):
    action = body.get("action", "")
    task_id = body.get("task_id", "")
    ms_id = body.get("ms_id", "") or task_id
    notion_id = body.get("notion_id", "") or task_id

    if action == "get_tasks":
        return handle_get_tasks()
    if action == "get_planner":
        return handle_get_planner()
    if action == "planner_toggle":
        return handle_planner_toggle(notion_id, body.get("completed", "false").lower() == "true")
    if action == "planner_update":
        return handle_planner_update(notion_id, body)
    if action == "planner_delete":
        return handle_planner_delete(notion_id)
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
        pass

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
