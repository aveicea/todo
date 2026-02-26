#!/usr/bin/env python3
"""
MS Todo ↔ Notion 양방향 동기화 스크립트
GitHub Actions에서 실행됩니다.
"""

import os
import json
import msal
import requests
from datetime import datetime, timezone, timedelta

# ── 환경 변수 ──────────────────────────────────────────────
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
REFRESH_TOKEN = os.environ["AZURE_REFRESH_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
MSTODO_LIST_ID = os.environ.get("MSTODO_LIST_ID", "")

_raw_db_id = os.environ.get("NOTION_DB_ID", "dadf27b55389404296df607af4d16e26").replace("-", "")
NOTION_DB_ID = f"{_raw_db_id[:8]}-{_raw_db_id[8:12]}-{_raw_db_id[12:16]}-{_raw_db_id[16:20]}-{_raw_db_id[20:]}"

_planner_raw = "468bf987e6cd4372abf96a8f30f165b1"
PLANNER_DB_ID = f"{_planner_raw[:8]}-{_planner_raw[8:12]}-{_planner_raw[12:16]}-{_planner_raw[16:20]}-{_planner_raw[20:]}"

_book_raw = "41c3889d4617465db9df008e96ca5af1"
BOOK_DB_ID = f"{_book_raw[:8]}-{_book_raw[8:12]}-{_book_raw[12:16]}-{_book_raw[16:20]}-{_book_raw[20:]}"

SCOPES = [
    "https://graph.microsoft.com/Tasks.ReadWrite",
    "https://graph.microsoft.com/User.Read",
]
AUTHORITY = "https://login.microsoftonline.com/consumers"

MAPPING_FILE = "data/mapping.json"
STATUS_FILE = "data/status.json"
PLANNER_FILE = "data/planner.json"

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


def create_todo_task(token, list_id, title, completed=False, due_date=None, due_time=None, importance="normal"):
    body = {"title": title, "status": "completed" if completed else "notStarted", "importance": importance or "normal"}
    if due_date:
        time_str = due_time or "00:00"
        body["dueDateTime"] = {"dateTime": f"{due_date}T{time_str}:00.0000000", "timeZone": "Korea Standard Time"}
    r = requests.post(
        f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks",
        headers={**ms_headers(token), "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def delete_todo_task(token, list_id, task_id):
    r = requests.delete(
        f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{task_id}",
        headers=ms_headers(token),
        timeout=30,
    )
    r.raise_for_status()


def update_todo_task(token, list_id, task_id, completed=None, due_date=None, due_time=None, title=None, importance=None):
    body = {}
    if completed is not None:
        body["status"] = "completed" if completed else "notStarted"
    if due_date is not None:
        if due_date:
            time_str = due_time or "00:00"
            body["dueDateTime"] = {"dateTime": f"{due_date}T{time_str}:00.0000000", "timeZone": "Korea Standard Time"}
        else:
            body["dueDateTime"] = None
    if title is not None:
        body["title"] = title
    if importance is not None:
        body["importance"] = importance
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


def create_notion_page(db_id, title, completed, title_prop, status_prop, done_value, todo_value, date_prop=None, due_date=None, id_prop=None, ms_task_id=None, importance_prop=None, importance_value=None):
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
        headers=NOTION_HEADERS,
        json={"parent": {"database_id": db_id}, "properties": props},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def update_notion_page(page_id, completed, status_prop, done_value, todo_value, date_prop=None, due_date=None, title_prop=None, title=None, comp_type="status", importance_prop=None, importance_value=None):
    if comp_type == "checkbox":
        props = {status_prop: {"checkbox": completed}}
    else:
        props = {status_prop: {"status": {"name": done_value if completed else todo_value}}}
    if date_prop is not None:
        props[date_prop] = {"date": {"start": due_date}} if due_date else {"date": None}
    if title_prop and title is not None:
        props[title_prop] = {"title": [{"text": {"content": title}}]}
    if importance_prop and importance_value is not None:
        props[importance_prop] = {"select": {"name": importance_value}}
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


def get_page_completed(page, prop_name, done_value=None, prop_type="status"):
    if prop_type == "checkbox":
        return bool(page["properties"].get(prop_name, {}).get("checkbox", False))
    status = page["properties"].get(prop_name, {}).get("status") or {}
    return status.get("name") == done_value


def get_page_date(page, date_prop):
    """Notion 페이지에서 날짜 속성값 반환 (YYYY-MM-DD 형식)"""
    if not date_prop:
        return None
    date_obj = page["properties"].get(date_prop, {}).get("date") or {}
    return date_obj.get("start", None)


def get_page_importance(page, importance_prop):
    if not importance_prop:
        return None
    select_val = page["properties"].get(importance_prop, {}).get("select") or {}
    return select_val.get("name") or None


def notion_importance_to_ms(option_name):
    if not option_name:
        return "normal"
    n = option_name.lower()
    if any(k in n for k in ("높", "중요", "high", "import", "urgent")):
        return "high"
    if any(k in n for k in ("낮", "low", "minor")):
        return "low"
    return "normal"


def ms_importance_to_notion(ms_val, options):
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
    if len(names) >= 3:
        return names[len(names) // 2]
    # 옵션이 2개 이하이고 "normal"에 해당하는 이름도 없으면 None 반환
    # (high/low 후보가 아닌 옵션이 있으면 그것을 사용, 없으면 None)
    high_candidates = {"높음", "중요", "High", "Important"}
    low_candidates = {"낮음", "Low"}
    neutral = [n for n in names if n not in high_candidates and n not in low_candidates]
    return neutral[0] if neutral else None


def extract_ms_due(due_dt_obj):
    """MS Todo dueDateTime에서 날짜·시간 추출 (KST = UTC+9 기준)
    MS가 UTC로 반환하는 경우 +9시간 보정해 한국 날짜로 변환."""
    if not due_dt_obj:
        return None, None
    dt_str = due_dt_obj.get("dateTime", "")
    tz = due_dt_obj.get("timeZone", "UTC")
    if not dt_str or len(dt_str) < 10:
        return None, None
    try:
        dt = datetime.fromisoformat(dt_str[:19])
        if tz.upper() == "UTC":
            dt = dt + timedelta(hours=9)  # UTC → KST 변환
        date_str = dt.strftime("%Y-%m-%d")
        time_val = dt.strftime("%H:%M")
        due_time = None if time_val == "00:00" else time_val
        return date_str, due_time
    except Exception:
        return (dt_str[:10] or None), None


def load_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ── 플래너 ───────────────────────────────────────────────
def run_planner(input_action, input_task_id):
    """플래너 Notion DB 처리 (MS Todo 연동 없이 단독 읽기/쓰기)"""
    print("\n📋 플래너 처리 중...")
    try:
        schema = get_db_schema(PLANNER_DB_ID)
    except Exception as e:
        print(f"  ⚠️ 플래너 DB 접근 실패 (Notion 통합 권한 확인 필요): {e}")
        return

    title_prop = find_prop(schema, "title") or "이름"
    date_prop = find_prop(schema, "date")

    # 완료 속성 탐지: checkbox "완료" 우선, 없으면 status 타입
    checkbox_done_prop = next(
        (name for name, prop in schema["properties"].items()
         if prop["type"] == "checkbox" and name == "완료"),
        None
    )
    if checkbox_done_prop:
        comp_prop = checkbox_done_prop
        comp_type = "checkbox"
        done_value = todo_value = None
        print(f"  ✅ 완료 속성: checkbox '{comp_prop}'")
    else:
        status_prop = find_prop(schema, "status") or "상태"
        comp_prop = status_prop
        comp_type = "status"
        status_opts = schema["properties"].get(status_prop, {}).get("status", {})
        options = status_opts.get("options", [])
        groups = status_opts.get("groups", [])
        done_group = next((g for g in groups if g.get("name") in ("Complete", "완료됨")), None)
        todo_group = next((g for g in groups if g.get("name") in ("To-do", "할 일")), None)
        done_option_ids = set(done_group.get("option_ids", [])) if done_group else set()
        todo_option_ids = set(todo_group.get("option_ids", [])) if todo_group else set()
        done_value = next((o["name"] for o in options if o["id"] in done_option_ids), None)
        todo_value = next((o["name"] for o in options if o["id"] in todo_option_ids), None)
        if not done_value:
            done_value = next((o["name"] for o in options if o["name"] in ("완료", "Done", "Completed", "완료됨")), None)
        if not todo_value:
            todo_value = next((o["name"] for o in options if o["name"] in ("시작 안 함", "Not started", "할 일", "예정", "To-do")), None)
        if not done_value and options:
            done_value = options[-1]["name"]
        if not todo_value and options:
            todo_value = options[0]["name"]
        print(f"  ✅ 완료 속성: status '{comp_prop}' (완료={done_value})")

    # 플래너 직접 액션 처리
    # Notion API는 eventually consistent이므로 PATCH 직후 GET이 stale 데이터를 반환할 수 있음.
    # 업데이트한 항목의 completed 값을 override_completed에 기록해두고 planner.json 저장 시 덮어씀.
    override_completed = {}  # notion_id → bool

    if input_action.startswith("planner_"):
        action = input_action[len("planner_"):]
        input_title = os.environ.get("INPUT_TITLE", "").strip()
        input_due_date = os.environ.get("INPUT_DUE_DATE", "").strip()
        input_due_time = os.environ.get("INPUT_DUE_TIME", "").strip()
        input_completed_str = os.environ.get("INPUT_COMPLETED", "").strip()
        print(f"  🎯 플래너 액션: {action} / id={input_task_id or '(없음)'}")

        if action == "toggle" and input_task_id:
            try:
                completed = input_completed_str.lower() == "true"
                update_notion_page(input_task_id, completed, comp_prop, done_value, todo_value, comp_type=comp_type)
                override_completed[input_task_id] = completed
                print(f"  ✅ 플래너 완료 토글: {completed}")
            except Exception as e:
                print(f"  ⚠️ 플래너 토글 실패: {e}")

        elif action == "update" and input_task_id:
            try:
                completed = input_completed_str.lower() == "true" if input_completed_str else False
                due_date = None if input_due_date in ("", "none") else input_due_date
                update_notion_page(
                    input_task_id, completed, comp_prop, done_value, todo_value,
                    date_prop=date_prop if input_due_date else None,
                    due_date=due_date,
                    title_prop=title_prop if input_title else None,
                    title=input_title if input_title else None,
                    comp_type=comp_type,
                )
                override_completed[input_task_id] = completed
                print(f"  ✅ 플래너 업데이트")
            except Exception as e:
                print(f"  ⚠️ 플래너 업데이트 실패: {e}")

        elif action == "delete" and input_task_id:
            try:
                r = requests.patch(
                    f"https://api.notion.com/v1/pages/{input_task_id}",
                    headers=NOTION_HEADERS,
                    json={"archived": True},
                    timeout=30,
                )
                r.raise_for_status()
                print(f"  🗑️ 플래너 아카이브 완료")
            except Exception as e:
                print(f"  ⚠️ 플래너 아카이브 실패: {e}")

    # 책 관계형 속성 찾기 (BOOK_DB_ID와 연결된 relation 속성)
    book_rel_prop = None
    for name, prop in schema["properties"].items():
        if prop["type"] == "relation":
            rel_db = prop.get("relation", {}).get("database_id", "").replace("-", "")
            if rel_db == _book_raw:
                book_rel_prop = name
                break
    print(f"  📚 책 관계형 속성: {book_rel_prop or '없음'}")

    # 책 제목 일괄 조회
    book_title_map = {}
    if book_rel_prop:
        try:
            book_schema = get_db_schema(BOOK_DB_ID)
            book_title_prop = find_prop(book_schema, "title") or "이름"
            book_pages = get_notion_pages(BOOK_DB_ID)
            book_title_map = {p["id"]: get_page_title(p, book_title_prop) for p in book_pages}
            print(f"  📚 책 {len(book_title_map)}권 로드 완료")
        except Exception as e:
            print(f"  ⚠️ 책 DB 조회 실패: {e}")

    # 플래너 전체 조회 후 저장
    try:
        pages = get_notion_pages(PLANNER_DB_ID)
    except Exception as e:
        print(f"  ⚠️ 플래너 데이터 조회 실패: {e}")
        return

    planner_tasks = []
    for page in pages:
        title = get_page_title(page, title_prop)
        if not title.strip():
            continue
        # 연결된 첫 번째 책 제목
        book_title = None
        if book_rel_prop:
            rel_ids = [r["id"] for r in page["properties"].get(book_rel_prop, {}).get("relation", [])]
            if rel_ids:
                book_title = book_title_map.get(rel_ids[0], "")
        page_id = page["id"]
        # Notion eventual consistency 대응: PATCH 직후 stale 데이터 반환 가능 → override 우선 사용
        comp_val = override_completed[page_id] if page_id in override_completed \
                   else get_page_completed(page, comp_prop, done_value, comp_type)
        planner_tasks.append({
            "notion_id": page_id,
            "title": title,
            "book_title": book_title or None,
            "completed": comp_val,
            "due_date": get_page_date(page, date_prop) if date_prop else None,
            "due_time": None,
        })

    planner_tasks.sort(key=lambda x: (x["completed"], x["due_date"] or "9999-12-31"))
    now_iso = datetime.now(timezone.utc).isoformat()
    save_json(PLANNER_FILE, {
        "tasks": planner_tasks,
        "last_sync": now_iso,
        "total": len(planner_tasks),
    })
    print(f"  📋 플래너 {len(planner_tasks)}개 항목 저장 완료")


# ── 메인 ─────────────────────────────────────────────────
def main():
    os.makedirs("data", exist_ok=True)

    input_action  = os.environ.get("INPUT_ACTION",  "").strip()
    input_task_id = os.environ.get("INPUT_TASK_ID", "").strip()

    # 플래너 전용 액션은 MS 동기화 불필요 → 바로 처리 후 종료
    if input_action.startswith("planner_"):
        run_planner(input_action, input_task_id)
        return

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
    importance_prop = next(
        (name for name, prop in schema["properties"].items()
         if prop["type"] == "select" and "중요" in name),
        None
    )
    importance_options = (
        schema["properties"].get(importance_prop, {}).get("select", {}).get("options", [])
        if importance_prop else []
    )
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

    print(f"  속성: title='{title_prop}', status='{status_prop}', 중요도='{importance_prop or '없음'}'")
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

    # 중복 매핑 정리: 같은 notion_id를 여러 ms_id가 가리키면 유효한 것(ms_tasks에 있는) 하나만 유지
    # (이전 버그로 생성된 stale 매핑 파일 일괄 정리)
    seen_notion_ids: dict[str, str] = {}
    for ms_id in list(ms_to_notion.keys()):
        notion_id = ms_to_notion[ms_id]
        if notion_id in seen_notion_ids:
            prev_ms_id = seen_notion_ids[notion_id]
            # ms_tasks에 있는 쪽 우선, 둘 다 없으면 나중 것 제거
            if ms_id in ms_tasks:
                ms_to_notion.pop(prev_ms_id, None)
                seen_notion_ids[notion_id] = ms_id
            else:
                ms_to_notion.pop(ms_id, None)
        else:
            seen_notion_ids[notion_id] = ms_id

    notion_to_ms = {v: k for k, v in ms_to_notion.items()}

    stats = {"created_in_ms": 0, "created_in_notion": 0, "updated": 0, "errors": 0}

    # ── 직접 업데이트 처리 (위젯에서 트리거) ─────────────
    directly_updated_ms_ids = set()
    if input_action and input_task_id:
        print(f"🎯 직접 업데이트: action={input_action}, task={input_task_id}")
        input_completed_str = os.environ.get("INPUT_COMPLETED", "").strip()
        input_title      = os.environ.get("INPUT_TITLE",      "").strip()
        input_due_date   = os.environ.get("INPUT_DUE_DATE",   "").strip()
        input_due_time   = os.environ.get("INPUT_DUE_TIME",   "").strip()
        input_importance = os.environ.get("INPUT_IMPORTANCE", "").strip()

        ms_kwargs = {}
        if input_completed_str:
            ms_kwargs["completed"] = input_completed_str.lower() == "true"
        if input_title:
            ms_kwargs["title"] = input_title
        if input_due_date:
            ms_kwargs["due_date"] = None if input_due_date == "none" else input_due_date
            if input_due_date != "none":
                ms_kwargs["due_time"] = input_due_time or None
        if input_importance:
            ms_kwargs["importance"] = input_importance

        if input_action == "create":
            # 새 태스크 생성 (MS Todo + Notion)
            title = input_title
            due_date = None if input_due_date in ("", "none") else input_due_date
            due_time = input_due_time or None
            if title:
                try:
                    task = create_todo_task(ms_token, list_id, title, due_date=due_date, due_time=due_time,
                                           importance=input_importance or "normal")
                    ms_tasks[task["id"]] = task
                    print(f"  ➕ MS Todo 생성: {title}")
                    notion_imp_val = ms_importance_to_notion(input_importance or "normal", importance_options) if importance_prop else None
                    page = create_notion_page(
                        NOTION_DB_ID, title, False, title_prop, status_prop, done_value, todo_value,
                        date_prop=date_prop, due_date=due_date,
                        id_prop=id_prop, ms_task_id=task["id"],
                        importance_prop=importance_prop, importance_value=notion_imp_val,
                    )
                    notion_pages[page["id"]] = page
                    ms_to_notion[task["id"]] = page["id"]
                    notion_to_ms[page["id"]] = task["id"]
                    directly_updated_ms_ids.add(task["id"])
                    stats["created_in_notion"] += 1
                    print(f"  ➕ Notion 생성: {title}")
                except Exception as e:
                    print(f"  ⚠️ 생성 실패: {e}")
                    stats["errors"] += 1
        elif input_action == "delete":
            # MS Todo 삭제 + Notion 아카이브
            try:
                delete_todo_task(ms_token, list_id, input_task_id)
                ms_tasks.pop(input_task_id, None)
                print(f"  🗑️ MS Todo 삭제 완료")
            except Exception as e:
                print(f"  ⚠️ MS Todo 삭제 실패: {e}")
            notion_id = ms_to_notion.get(input_task_id)
            if notion_id:
                try:
                    r = requests.patch(
                        f"https://api.notion.com/v1/pages/{notion_id}",
                        headers=NOTION_HEADERS,
                        json={"archived": True},
                        timeout=30,
                    )
                    r.raise_for_status()
                    print(f"  🗑️ Notion 아카이브 완료")
                except Exception as e:
                    print(f"  ⚠️ Notion 아카이브 실패: {e}")
            ms_to_notion.pop(input_task_id, None)
            notion_pages.pop(notion_id, None)
            directly_updated_ms_ids.add(input_task_id)
        else:
            try:
                if ms_kwargs:
                    update_todo_task(ms_token, list_id, input_task_id, **ms_kwargs)
                    # 로컬 캐시도 갱신 (이후 동기화 루프에서 덮어쓰지 않도록)
                    if input_task_id in ms_tasks:
                        if "completed" in ms_kwargs:
                            ms_tasks[input_task_id]["status"] = "completed" if ms_kwargs["completed"] else "notStarted"
                        if "title" in ms_kwargs:
                            ms_tasks[input_task_id]["title"] = ms_kwargs["title"]
                        if "due_date" in ms_kwargs:
                            d = ms_kwargs["due_date"]
                            t = ms_kwargs.get("due_time") or "00:00"
                            ms_tasks[input_task_id]["dueDateTime"] = (
                                {"dateTime": f"{d}T{t}:00.0000000", "timeZone": "Korea Standard Time"} if d else None
                            )
                print(f"  ✅ MS Todo 업데이트 완료")
            except Exception as e:
                print(f"  ⚠️ MS Todo 업데이트 실패: {e}")

        notion_id = ms_to_notion.get(input_task_id)
        if notion_id and notion_id in notion_pages:
            try:
                page = notion_pages[notion_id]
                cur_completed = get_page_completed(page, status_prop, done_value)
                cur_date = get_page_date(page, date_prop) if date_prop else None
                new_completed = ms_kwargs.get("completed", cur_completed)
                new_due_date  = ms_kwargs.get("due_date",  cur_date)
                update_notion_page(
                    notion_id, new_completed, status_prop, done_value, todo_value,
                    date_prop=date_prop if input_due_date else None,
                    due_date=new_due_date,
                    title_prop=title_prop if input_title else None,
                    title=input_title if input_title else None,
                    importance_prop=importance_prop if input_importance and importance_prop else None,
                    importance_value=ms_importance_to_notion(input_importance, importance_options) if input_importance and importance_prop else None,
                )
                # Notion 로컬 캐시 갱신 (동기화 루프가 이 태스크를 덮어쓰지 않도록)
                props = notion_pages[notion_id]["properties"]
                if "completed" in ms_kwargs:
                    status_name = done_value if new_completed else todo_value
                    if status_prop in props and props[status_prop].get("status") is not None:
                        props[status_prop]["status"]["name"] = status_name
                if "due_date" in ms_kwargs and date_prop and date_prop in props:
                    props[date_prop]["date"] = {"start": new_due_date} if new_due_date else None
                if input_title and title_prop in props:
                    title_arr = props[title_prop].get("title", [])
                    if title_arr:
                        title_arr[0].setdefault("text", {})["content"] = input_title
                        title_arr[0]["plain_text"] = input_title
                    else:
                        props[title_prop]["title"] = [{"plain_text": input_title, "text": {"content": input_title}}]
                # 타임스탬프를 현재 시각으로 설정해 동기화 루프에서 충돌 방지
                notion_pages[notion_id]["last_edited_time"] = datetime.now(timezone.utc).isoformat()
                print(f"  ✅ Notion 업데이트 완료")
            except Exception as e:
                print(f"  ⚠️ Notion 업데이트 실패: {e}")

        # 이 태스크는 이미 양쪽 모두 업데이트했으므로 동기화 루프에서 스킵
        directly_updated_ms_ids.add(input_task_id)

    # ── Notion → MS Todo (Notion에만 있는 항목 생성) ──────
    for page_id, page in notion_pages.items():
        if page_id in notion_to_ms and notion_to_ms[page_id] in ms_tasks:
            continue
        title = get_page_title(page, title_prop)
        if not title.strip():
            continue
        try:
            completed = get_page_completed(page, status_prop, done_value)
            notion_date = get_page_date(page, date_prop)
            notion_imp_name = get_page_importance(page, importance_prop)
            notion_imp_ms = notion_importance_to_ms(notion_imp_name) if notion_imp_name else "normal"
            task = create_todo_task(ms_token, list_id, title, completed, due_date=notion_date, importance=notion_imp_ms)
            ms_tasks[task["id"]] = task  # 동기화 루프에서 "없음"으로 오인하지 않도록
            # stale 매핑 제거: old_ms_id가 ms_to_notion에 남아 있으면 sync 루프가
            # "MS에 없음 → Notion 아카이브"를 실행해버리는 버그 방지
            if page_id in notion_to_ms:
                ms_to_notion.pop(notion_to_ms[page_id], None)
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
        # 매핑이 존재하면 Notion 페이지 유무와 무관하게 스킵
        # (Notion에서 삭제된 경우 아래 삭제 전파 루프에서 처리)
        if task_id in ms_to_notion:
            continue
        title = task.get("title", "").strip()
        if not title:
            continue
        try:
            completed = task["status"] == "completed"
            due_date, _ = extract_ms_due(task.get("dueDateTime"))
            ms_imp = task.get("importance", "normal")
            notion_imp_val = ms_importance_to_notion(ms_imp, importance_options) if importance_prop else None
            page = create_notion_page(
                NOTION_DB_ID, title, completed, title_prop, status_prop, done_value, todo_value,
                date_prop=date_prop, due_date=due_date,
                id_prop=id_prop, ms_task_id=task_id,
                importance_prop=importance_prop, importance_value=notion_imp_val,
            )
            notion_pages[page["id"]] = page  # 동기화 루프에서 "없음"으로 오인하지 않도록
            ms_to_notion[task_id] = page["id"]
            notion_to_ms[page["id"]] = task_id
            stats["created_in_notion"] += 1
            print(f"  ➕ Notion에 생성: {title}")
        except Exception as e:
            print(f"  ⚠️ Notion 생성 실패 ({title}): {e}")
            stats["errors"] += 1

    # ── 완료 상태 양방향 동기화 + 삭제 전파 ──────────────
    for ms_id, notion_id in list(ms_to_notion.items()):
        # 이번 실행에서 직접 업데이트한 태스크는 스킵 (이미 양쪽 모두 처리됨)
        if ms_id in directly_updated_ms_ids:
            continue

        ms_exists = ms_id in ms_tasks
        notion_exists = notion_id in notion_pages

        # 한쪽 또는 양쪽 모두 삭제된 경우
        if not ms_exists or not notion_exists:
            if not ms_exists and not notion_exists:
                # 양쪽 모두 삭제 → 매핑만 정리
                del ms_to_notion[ms_id]
                print(f"  🗑️ 매핑 정리 (양쪽 모두 삭제됨): {ms_id[:8]}...")
            elif not ms_exists:
                # MS에서 삭제됨 → Notion 페이지도 아카이브
                try:
                    r = requests.patch(
                        f"https://api.notion.com/v1/pages/{notion_id}",
                        headers=NOTION_HEADERS,
                        json={"archived": True},
                        timeout=30,
                    )
                    r.raise_for_status()
                    del ms_to_notion[ms_id]
                    stats["updated"] += 1
                    title = get_page_title(notion_pages[notion_id], title_prop)
                    print(f"  🗑️ Notion 아카이브 (MS에서 삭제됨): {title}")
                except Exception as e:
                    print(f"  ⚠️ Notion 아카이브 실패: {e}")
                    stats["errors"] += 1
            else:
                # Notion에서 삭제됨 → MS 태스크도 삭제
                try:
                    delete_todo_task(ms_token, list_id, ms_id)
                    del ms_to_notion[ms_id]
                    stats["updated"] += 1
                    title = ms_tasks[ms_id].get("title", "?")
                    print(f"  🗑️ MS 태스크 삭제 (Notion에서 삭제됨): {title}")
                except Exception as e:
                    print(f"  ⚠️ MS 태스크 삭제 실패: {e}")
                    stats["errors"] += 1
            continue

        task = ms_tasks[ms_id]
        page = notion_pages[notion_id]

        ms_done = task["status"] == "completed"
        notion_done = get_page_completed(page, status_prop, done_value)
        ms_date, _ = extract_ms_due(task.get("dueDateTime"))
        notion_date = get_page_date(page, date_prop) if date_prop else None
        ms_importance = task.get("importance", "normal")
        notion_imp_name = get_page_importance(page, importance_prop)
        notion_importance = notion_importance_to_ms(notion_imp_name) if notion_imp_name else "normal"

        if ms_done == notion_done and ms_date == notion_date and (not importance_prop or ms_importance == notion_importance):
            continue

        title = task.get("title", "?")
        ms_time = task.get("lastModifiedDateTime", "")
        notion_time = page.get("last_edited_time", "")

        try:
            if ms_time >= notion_time:
                # MS가 최신 → Notion 업데이트
                date_changed = date_prop and ms_date != notion_date
                imp_changed = importance_prop and ms_importance != notion_importance
                update_notion_page(
                    notion_id, ms_done, status_prop, done_value, todo_value,
                    date_prop=date_prop if date_changed else None,
                    due_date=ms_date if date_changed else None,
                    importance_prop=importance_prop if imp_changed else None,
                    importance_value=ms_importance_to_notion(ms_importance, importance_options) if imp_changed else None,
                )
                msg = f"{'완료' if ms_done else todo_value}"
                if date_changed:
                    msg += f" / 날짜={ms_date}"
                if imp_changed:
                    msg += f" / 중요도={ms_importance}"
                print(f"  🔄 Notion 업데이트: {title} → {msg}")
            else:
                # Notion이 최신 → MS 업데이트
                date_changed = ms_date != notion_date
                imp_changed = importance_prop and ms_importance != notion_importance
                update_todo_task(
                    ms_token, list_id, ms_id,
                    completed=notion_done,
                    # notion_date가 None이면 빈 문자열로 → MS 날짜 클리어
                    due_date=(notion_date or "") if date_changed else None,
                    importance=notion_importance if imp_changed else None,
                )
                msg = f"{'완료' if notion_done else '미완료'}"
                if date_changed:
                    msg += f" / 날짜={notion_date}"
                if imp_changed:
                    msg += f" / 중요도={notion_importance}"
                print(f"  🔄 MS 업데이트: {title} → {msg}")
            stats["updated"] += 1
        except Exception as e:
            print(f"  ⚠️ 동기화 실패 ({title}): {e}")
            stats["errors"] += 1

    # ── 결과 저장 ─────────────────────────────────────────
    mapping["ms_to_notion"] = ms_to_notion
    save_json(MAPPING_FILE, mapping)

    now_iso = datetime.now(timezone.utc).isoformat()

    status = {
        "last_sync": now_iso,
        "success": stats["errors"] == 0,
        "stats": stats,
        "total_ms_tasks": len(ms_tasks),
        "total_notion_pages": len(notion_pages),
    }
    save_json(STATUS_FILE, status)

    # tasks.json: 위젯 표시용 할 일 목록
    all_tasks = []
    for task_id, task in ms_tasks.items():
        due_date, due_time = extract_ms_due(task.get("dueDateTime"))
        all_tasks.append({
            "ms_id": task_id,
            "notion_id": ms_to_notion.get(task_id, ""),
            "title": task.get("title", ""),
            "completed": task["status"] == "completed",
            "due_date": due_date,
            "due_time": due_time,
            "importance": task.get("importance", "normal"),
        })
    all_tasks.sort(key=lambda x: (x["completed"], x["due_date"] or "9999-12-31", x["due_time"] or "99:99"))
    save_json("data/tasks.json", {
        "tasks": all_tasks,
        "last_sync": now_iso,
        "total": len(all_tasks),
        "completed": sum(1 for t in all_tasks if t["completed"]),
        "pending": sum(1 for t in all_tasks if not t["completed"]),
    })

    print(f"\n✨ 동기화 완료!")
    print(f"  MS 생성: {stats['created_in_ms']} | Notion 생성: {stats['created_in_notion']} | 업데이트: {stats['updated']} | 오류: {stats['errors']}")

    run_planner(input_action, input_task_id)


if __name__ == "__main__":
    main()
