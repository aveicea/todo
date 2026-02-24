#!/usr/bin/env python3
"""
MS Todo ↔ Notion 양방향 동기화 스크립트
GitHub Actions에서 실행됩니다.
"""

import os
import json
import msal
import requests
from datetime import datetime, timezone

# ── 환경 변수 ──────────────────────────────────────────────
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
REFRESH_TOKEN = os.environ["AZURE_REFRESH_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
MSTODO_LIST_ID = os.environ.get("MSTODO_LIST_ID", "")

_raw_db_id = os.environ.get("NOTION_DB_ID", "dadf27b55389404296df607af4d16e26").replace("-", "")
NOTION_DB_ID = f"{_raw_db_id[:8]}-{_raw_db_id[8:12]}-{_raw_db_id[12:16]}-{_raw_db_id[16:20]}-{_raw_db_id[20:]}"

SCOPES = [
    "https://graph.microsoft.com/Tasks.ReadWrite",
    "https://graph.microsoft.com/User.Read",
]
AUTHORITY = "https://login.microsoftonline.com/consumers"

MAPPING_FILE = "data/mapping.json"
STATUS_FILE = "data/status.json"

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


# ── MS 인증 ───────────────────────────────────────────────
def get_ms_token():
    app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)
    result = app.acquire_token_by_refresh_token(REFRESH_TOKEN, scopes=SCOPES)
    if "access_token" not in result:
        raise RuntimeError(f"MS 인증 실패: {result.get('error_description')}")
    return result["access_token"]


def ms_headers(token):
    return {"Authorization": f"Bearer {token}"}


# ── MS Todo API ───────────────────────────────────────────
def get_todo_lists(token):
    r = requests.get(
        "https://graph.microsoft.com/v1.0/me/todo/lists",
        headers=ms_headers(token),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["value"]


def get_todo_tasks(token, list_id):
    tasks, url = [], f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks"
    while url:
        r = requests.get(url, headers=ms_headers(token), timeout=30)
        r.raise_for_status()
        data = r.json()
        tasks.extend(data["value"])
        url = data.get("@odata.nextLink")
    return tasks


def create_todo_task(token, list_id, title, completed=False, due_date=None):
    body = {"title": title, "status": "completed" if completed else "notStarted"}
    if due_date:
        body["dueDateTime"] = {"dateTime": f"{due_date}T00:00:00.0000000", "timeZone": "UTC"}
    r = requests.post(
        f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks",
        headers={**ms_headers(token), "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def update_todo_task(token, list_id, task_id, completed=None, due_date=None):
    body = {}
    if completed is not None:
        body["status"] = "completed" if completed else "notStarted"
    if due_date is not None:
        body["dueDateTime"] = {"dateTime": f"{due_date}T00:00:00.0000000", "timeZone": "UTC"} if due_date else None
    r = requests.patch(
        f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{task_id}",
        headers={**ms_headers(token), "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


# ── Notion API ────────────────────────────────────────────
def get_db_schema(db_id):
    r = requests.get(
        f"https://api.notion.com/v1/databases/{db_id}",
        headers=NOTION_HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def find_prop(schema, prop_type):
    """데이터베이스 스키마에서 특정 유형의 첫 번째 속성 이름 반환"""
    for name, prop in schema["properties"].items():
        if prop["type"] == prop_type:
            return name
    return None


def get_notion_pages(db_id):
    pages, cursor = [], None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers=NOTION_HEADERS,
            json=body,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        pages.extend(data["results"])
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


def create_notion_page(db_id, title, completed, title_prop, status_prop, done_value, todo_value, date_prop=None, due_date=None, id_prop=None, ms_task_id=None):
    props = {
        title_prop: {"title": [{"text": {"content": title}}]},
        status_prop: {"status": {"name": done_value if completed else todo_value}},
    }
    if date_prop and due_date:
        props[date_prop] = {"date": {"start": due_date}}
    if id_prop and ms_task_id:
        props[id_prop] = {"rich_text": [{"text": {"content": ms_task_id}}]}
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS,
        json={"parent": {"database_id": db_id}, "properties": props},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def update_notion_page(page_id, completed, status_prop, done_value, todo_value, date_prop=None, due_date=None):
    props = {status_prop: {"status": {"name": done_value if completed else todo_value}}}
    if date_prop is not None:
        props[date_prop] = {"date": {"start": due_date}} if due_date else {"date": None}
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": props},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


# ── 헬퍼 ─────────────────────────────────────────────────
def get_page_title(page, title_prop):
    arr = page["properties"].get(title_prop, {}).get("title", [])
    return "".join(t.get("plain_text", "") for t in arr)


def get_page_completed(page, status_prop, done_value):
    status = page["properties"].get(status_prop, {}).get("status") or {}
    return status.get("name") == done_value


def get_page_date(page, date_prop):
    """Notion 페이지에서 날짜 속성값 반환 (YYYY-MM-DD 형식)"""
    if not date_prop:
        return None
    date_obj = page["properties"].get(date_prop, {}).get("date") or {}
    return date_obj.get("start", None)


def load_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ── 메인 ─────────────────────────────────────────────────
def main():
    os.makedirs("data", exist_ok=True)
    mapping = load_json(MAPPING_FILE, {"ms_to_notion": {}})
    ms_to_notion = mapping["ms_to_notion"]  # ms_task_id → notion_page_id

    print("🔐 MS 인증 중...")
    ms_token = get_ms_token()

    # To Do 목록 ID 결정
    list_id = MSTODO_LIST_ID
    if not list_id:
        lists = get_todo_lists(ms_token)
        list_id = lists[0]["id"]
        print(f"📋 목록 자동 선택: {lists[0]['displayName']}")

    # Notion DB 스키마에서 속성 이름 자동 탐지
    print("📐 Notion DB 스키마 확인 중...")
    schema = get_db_schema(NOTION_DB_ID)
    title_prop = find_prop(schema, "title") or "이름"
    status_prop = find_prop(schema, "status") or "상태"
    date_prop = find_prop(schema, "date")
    # MS Todo ID 속성 탐지 (rich_text 중 이름에 'todo' 또는 'id' 포함)
    id_prop = next(
        (name for name, prop in schema["properties"].items()
         if prop["type"] == "rich_text" and any(k in name.lower() for k in ("todo", "id", "ms"))),
        None
    )

    # 완료/미완료 상태값 탐지
    status_opts = schema["properties"].get(status_prop, {}).get("status", {})
    options = status_opts.get("options", [])
    groups = status_opts.get("groups", [])

    done_group = next((g for g in groups if g.get("name") in ("Complete", "완료됨")), None)
    todo_group = next((g for g in groups if g.get("name") in ("To-do", "할 일")), None)

    done_option_ids = set(done_group.get("option_ids", [])) if done_group else set()
    todo_option_ids = set(todo_group.get("option_ids", [])) if todo_group else set()

    done_value = next((o["name"] for o in options if o["id"] in done_option_ids), None)
    todo_value = next((o["name"] for o in options if o["id"] in todo_option_ids), None)

    # 그룹 탐지 실패시 옵션 이름으로 직접 탐지
    if not done_value:
        done_value = next((o["name"] for o in options if o["name"] in ("완료", "Done", "Completed", "완료됨")), None)
    if not todo_value:
        todo_value = next((o["name"] for o in options if o["name"] in ("시작 안 함", "Not started", "할 일", "예정", "To-do")), None)

    # 최후 수단: 마지막/첫 번째 옵션
    if not done_value and options:
        done_value = options[-1]["name"]
    if not todo_value and options:
        todo_value = options[0]["name"]

    print(f"  속성: title='{title_prop}', status='{status_prop}'")
    print(f"  상태값: 완료='{done_value}', 미완료='{todo_value}'")

    print("📥 데이터 가져오는 중...")
    ms_tasks = {t["id"]: t for t in get_todo_tasks(ms_token, list_id)}
    notion_pages = {p["id"]: p for p in get_notion_pages(NOTION_DB_ID)}

    # Notion의 MS Todo ID 속성으로 매핑 복구 (mapping.json 유실 무관)
    if id_prop:
        for page_id, page in notion_pages.items():
            stored_id = "".join(
                t.get("plain_text", "")
                for t in page["properties"].get(id_prop, {}).get("rich_text", [])
            ).strip()
            if stored_id and stored_id in ms_tasks and stored_id not in ms_to_notion:
                ms_to_notion[stored_id] = page_id
                print(f"  🔗 ID 속성으로 매핑 복구: {ms_tasks[stored_id].get('title', '')}")

    notion_to_ms = {v: k for k, v in ms_to_notion.items()}

    stats = {"created_in_ms": 0, "created_in_notion": 0, "updated": 0, "errors": 0}

    # ── Notion → MS Todo (Notion에만 있는 항목 생성) ──────
    for page_id, page in notion_pages.items():
        if page_id in notion_to_ms:
            continue
        title = get_page_title(page, title_prop)
        if not title.strip():
            continue
        try:
            completed = get_page_completed(page, status_prop, done_value)
            notion_date = get_page_date(page, date_prop)
            task = create_todo_task(ms_token, list_id, title, completed, due_date=notion_date)
            ms_to_notion[task["id"]] = page_id
            notion_to_ms[page_id] = task["id"]
            # Notion 페이지에 MS Todo ID 저장
            if id_prop:
                requests.patch(
                    f"https://api.notion.com/v1/pages/{page_id}",
                    headers=NOTION_HEADERS,
                    json={"properties": {id_prop: {"rich_text": [{"text": {"content": task["id"]}}]}}},
                    timeout=30,
                )
            stats["created_in_ms"] += 1
            print(f"  ➕ MS에 생성: {title}")
        except Exception as e:
            print(f"  ⚠️ MS 생성 실패 ({title}): {e}")
            stats["errors"] += 1

    # ── MS Todo → Notion (MS에만 있는 항목 생성) ──────────
    for task_id, task in ms_tasks.items():
        if task_id in ms_to_notion:
            continue
        title = task.get("title", "").strip()
        if not title:
            continue
        try:
            completed = task["status"] == "completed"
            due = task.get("dueDateTime") or {}
            due_date = due.get("dateTime", "")[:10] or None
            page = create_notion_page(
                NOTION_DB_ID, title, completed, title_prop, status_prop, done_value, todo_value,
                date_prop=date_prop, due_date=due_date,
                id_prop=id_prop, ms_task_id=task_id
            )
            ms_to_notion[task_id] = page["id"]
            notion_to_ms[page["id"]] = task_id
            stats["created_in_notion"] += 1
            print(f"  ➕ Notion에 생성: {title}")
        except Exception as e:
            print(f"  ⚠️ Notion 생성 실패 ({title}): {e}")
            stats["errors"] += 1

    # ── 완료 상태 양방향 동기화 ───────────────────────────
    for ms_id, notion_id in list(ms_to_notion.items()):
        if ms_id not in ms_tasks or notion_id not in notion_pages:
            continue
        task = ms_tasks[ms_id]
        page = notion_pages[notion_id]

        ms_done = task["status"] == "completed"
        notion_done = get_page_completed(page, status_prop, done_value)
        ms_date = (task.get("dueDateTime") or {}).get("dateTime", "")[:10] or None
        notion_date = get_page_date(page, date_prop) if date_prop else None

        if ms_done == notion_done and ms_date == notion_date:
            continue

        title = task.get("title", "?")
        ms_time = task.get("lastModifiedDateTime", "")
        notion_time = page.get("last_edited_time", "")

        try:
            if ms_time >= notion_time:
                # MS가 최신 → Notion 업데이트
                date_changed = date_prop and ms_date != notion_date
                update_notion_page(
                    notion_id, ms_done, status_prop, done_value, todo_value,
                    date_prop=date_prop if date_changed else None,
                    due_date=ms_date if date_changed else None,
                )
                msg = f"{'완료' if ms_done else todo_value}"
                if date_changed:
                    msg += f" / 날짜={ms_date}"
                print(f"  🔄 Notion 업데이트: {title} → {msg}")
            else:
                # Notion이 최신 → MS 업데이트
                date_changed = ms_date != notion_date
                update_todo_task(
                    ms_token, list_id, ms_id,
                    completed=notion_done,
                    due_date=notion_date if date_changed else None,
                )
                msg = f"{'완료' if notion_done else '미완료'}"
                if date_changed:
                    msg += f" / 날짜={notion_date}"
                print(f"  🔄 MS 업데이트: {title} → {msg}")
            stats["updated"] += 1
        except Exception as e:
            print(f"  ⚠️ 동기화 실패 ({title}): {e}")
            stats["errors"] += 1

    # ── 결과 저장 ─────────────────────────────────────────
    mapping["ms_to_notion"] = ms_to_notion
    save_json(MAPPING_FILE, mapping)

    status = {
        "last_sync": datetime.now(timezone.utc).isoformat(),
        "success": stats["errors"] == 0,
        "stats": stats,
        "total_ms_tasks": len(ms_tasks),
        "total_notion_pages": len(notion_pages),
    }
    save_json(STATUS_FILE, status)

    print(f"\n✨ 동기화 완료!")
    print(f"  MS 생성: {stats['created_in_ms']} | Notion 생성: {stats['created_in_notion']} | 업데이트: {stats['updated']} | 오류: {stats['errors']}")


if __name__ == "__main__":
    main()
