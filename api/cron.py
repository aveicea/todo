"""
Vercel Cron Function: /api/cron
MS Todo → Notion 자동 동기화 (Vercel Cron으로 주기적 실행)
mapping.json 없이 Notion의 id_prop 필드로 매핑 재구성
"""
from http.server import BaseHTTPRequestHandler
import json, os, requests, msal
from datetime import datetime, timedelta


def _env(key, default=""):
    return os.environ.get(key, default)


def _raw_to_uuid(raw):
    r = raw.replace("-", "")
    return f"{r[:8]}-{r[8:12]}-{r[12:16]}-{r[16:20]}-{r[20:]}"


NOTION_DB_ID = _raw_to_uuid(_env("NOTION_DB_ID", "dadf27b55389404296df607af4d16e26"))


def _notion_headers():
    return {
        "Authorization": f'Bearer {_env("NOTION_TOKEN")}',
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


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
        raise RuntimeError(f"MS 인증 실패: {result.get('error_description')}")
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


def _get_ms_tasks(token, list_id):
    tasks, url = [], f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks"
    while url:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        r.raise_for_status()
        data = r.json()
        tasks.extend(data["value"])
        url = data.get("@odata.nextLink")
    return {t["id"]: t for t in tasks}


def _get_notion_pages():
    pages, cursor = [], None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
            headers=_notion_headers(), json=body, timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        pages.extend(data["results"])
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return {p["id"]: p for p in pages}


def _get_schema():
    r = requests.get(
        f"https://api.notion.com/v1/databases/{NOTION_DB_ID}",
        headers=_notion_headers(), timeout=30,
    )
    r.raise_for_status()
    props = r.json()["properties"]

    title_prop = next((n for n, p in props.items() if p["type"] == "title"), "이름")
    status_prop = next((n for n, p in props.items() if p["type"] == "status"), "상태")
    date_prop = next((n for n, p in props.items() if p["type"] == "date"), None)
    id_prop = next(
        (n for n, p in props.items()
         if p["type"] == "rich_text" and any(k in n.lower() for k in ("todo", "id", "ms"))),
        None,
    )
    importance_prop = next((n for n, p in props.items() if p["type"] == "select" and "중요" in n), None)
    importance_options = props.get(importance_prop, {}).get("select", {}).get("options", []) if importance_prop else []

    opts = props.get(status_prop, {}).get("status", {})
    options = opts.get("options", [])
    groups = opts.get("groups", [])
    done_group = next((g for g in groups if g.get("name") in ("Complete", "완료됨")), None)
    todo_group = next((g for g in groups if g.get("name") in ("To-do", "할 일")), None)
    done_ids = set(done_group.get("option_ids", [])) if done_group else set()
    todo_ids = set(todo_group.get("option_ids", [])) if todo_group else set()
    done_value = next((o["name"] for o in options if o["id"] in done_ids), None)
    todo_value = next((o["name"] for o in options if o["id"] in todo_ids), None)
    if not done_value:
        done_value = next((o["name"] for o in options if o["name"] in ("완료", "Done", "Completed", "완료됨")), None)
    if not todo_value:
        todo_value = next((o["name"] for o in options if o["name"] in ("시작 안 함", "Not started", "할 일", "예정", "To-do")), None)
    if not done_value and options:
        done_value = options[-1]["name"]
    if not todo_value and options:
        todo_value = options[0]["name"]

    return {
        "title_prop": title_prop,
        "status_prop": status_prop,
        "done_value": done_value,
        "todo_value": todo_value,
        "date_prop": date_prop,
        "id_prop": id_prop,
        "importance_prop": importance_prop,
        "importance_options": importance_options,
    }


def _extract_ms_due(due_dt_obj):
    if not due_dt_obj:
        return None, None
    dt_str = due_dt_obj.get("dateTime", "")
    tz = due_dt_obj.get("timeZone", "UTC")
    if not dt_str or len(dt_str) < 10:
        return None, None
    try:
        dt = datetime.fromisoformat(dt_str[:19])
        if tz.upper() == "UTC":
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
                return dt_str[:10], None
            dt = dt + timedelta(hours=9)
        date_str = dt.strftime("%Y-%m-%d")
        time_val = dt.strftime("%H:%M")
        return date_str, (None if time_val == "00:00" else time_val)
    except Exception:
        return (dt_str[:10] or None), None


def _importance_to_notion(ms_val, options):
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


def run_sync():
    stats = {"created": 0, "updated": 0, "errors": 0}

    ms_token = _ms_token()
    list_id = _get_list_id(ms_token)
    s = _get_schema()

    ms_tasks = _get_ms_tasks(ms_token, list_id)
    notion_pages = _get_notion_pages()

    # Notion id_prop 필드로 매핑 재구성 (mapping.json 불필요)
    ms_to_notion = {}
    for page_id, page in notion_pages.items():
        if not s["id_prop"]:
            break
        stored_id = "".join(
            t.get("plain_text", "")
            for t in page["properties"].get(s["id_prop"], {}).get("rich_text", [])
        ).strip()
        if stored_id and stored_id in ms_tasks:
            ms_to_notion[stored_id] = page_id

    # MS에만 있는 태스크 → Notion 생성
    for task_id, task in ms_tasks.items():
        if task_id in ms_to_notion:
            continue
        title = task.get("title", "").strip()
        if not title:
            continue
        try:
            completed = task["status"] == "completed"
            due_date, _ = _extract_ms_due(task.get("dueDateTime"))
            ms_imp = task.get("importance", "normal")
            notion_imp = _importance_to_notion(ms_imp, s["importance_options"]) if s["importance_prop"] else None

            props = {
                s["title_prop"]: {"title": [{"text": {"content": title}}]},
                s["status_prop"]: {"status": {"name": s["done_value"] if completed else s["todo_value"]}},
            }
            if due_date and s["date_prop"]:
                props[s["date_prop"]] = {"date": {"start": due_date}}
            if s["id_prop"]:
                props[s["id_prop"]] = {"rich_text": [{"text": {"content": task_id}}]}
            if s["importance_prop"] and notion_imp:
                props[s["importance_prop"]] = {"select": {"name": notion_imp}}

            r = requests.post(
                "https://api.notion.com/v1/pages",
                headers=_notion_headers(),
                json={"parent": {"database_id": NOTION_DB_ID}, "properties": props},
                timeout=30,
            )
            r.raise_for_status()
            ms_to_notion[task_id] = r.json()["id"]
            stats["created"] += 1
        except Exception:
            stats["errors"] += 1

    # 양쪽 모두 있는 태스크: 완료 상태 · 날짜 동기화 (last-write-wins)
    for ms_id, notion_id in ms_to_notion.items():
        if notion_id not in notion_pages:
            continue
        task = ms_tasks[ms_id]
        page = notion_pages[notion_id]

        ms_done = task["status"] == "completed"
        page_status = page["properties"].get(s["status_prop"], {}).get("status") or {}
        notion_done = page_status.get("name") == s["done_value"]
        ms_date, _ = _extract_ms_due(task.get("dueDateTime"))
        page_date_obj = page["properties"].get(s["date_prop"] or "", {}).get("date") if s["date_prop"] else None
        notion_date = (page_date_obj or {}).get("start", "")[:10] if page_date_obj else None

        if ms_done == notion_done and ms_date == notion_date:
            continue

        ms_time = task.get("lastModifiedDateTime", "")
        notion_time = page.get("last_edited_time", "")

        try:
            if ms_time >= notion_time:
                props = {s["status_prop"]: {"status": {"name": s["done_value"] if ms_done else s["todo_value"]}}}
                if s["date_prop"] and ms_date != notion_date:
                    props[s["date_prop"]] = {"date": {"start": ms_date}} if ms_date else {"date": None}
                requests.patch(
                    f"https://api.notion.com/v1/pages/{notion_id}",
                    headers=_notion_headers(), json={"properties": props}, timeout=30,
                ).raise_for_status()
            else:
                ms_body = {"status": "completed" if notion_done else "notStarted"}
                if s["date_prop"] and ms_date != notion_date:
                    ms_body["dueDateTime"] = (
                        {"dateTime": f"{notion_date}T00:00:00.0000000", "timeZone": "Korea Standard Time"}
                        if notion_date else None
                    )
                requests.patch(
                    f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{ms_id}",
                    headers=_ms_headers(ms_token), json=ms_body, timeout=30,
                ).raise_for_status()
            stats["updated"] += 1
        except Exception:
            stats["errors"] += 1

    return stats


class handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        try:
            stats = run_sync()
            result = {"ok": True, "stats": stats}
            code = 200
        except Exception as e:
            result = {"ok": False, "error": str(e)}
            code = 500

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(result, ensure_ascii=False).encode())
