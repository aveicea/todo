#!/usr/bin/env python3
"""
최초 1회 실행: Microsoft 인증 & GitHub Secrets에 넣을 값 출력
사용법: pip3 install msal requests && python3 setup_auth.py
"""

import msal
import requests
import webbrowser
import subprocess
import sys

CLIENT_ID = "e2fba581-3f32-42af-9142-e3f8ee6a4003"
SCOPES = [
    "https://graph.microsoft.com/Tasks.ReadWrite",
    "https://graph.microsoft.com/User.Read",
]
AUTHORITY = "https://login.microsoftonline.com/consumers"

app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)

flow = app.initiate_device_flow(scopes=SCOPES)
if "user_code" not in flow:
    raise SystemExit(f"오류: {flow}")

code = flow["user_code"]

# 클립보드에 코드 복사 (Mac)
try:
    subprocess.run(["pbcopy"], input=code.encode(), check=True)
    clipboard_ok = True
except Exception:
    clipboard_ok = False

print()
print("=" * 50)
print(f"  코드: {code}")
if clipboard_ok:
    print("  (클립보드에 자동 복사됨)")
print("=" * 50)
print()
print("브라우저가 열립니다. 그냥 로그인만 하면 됩니다.")
print()

# verification_uri_complete = 코드가 이미 포함된 URL (코드 입력 불필요)
webbrowser.open(flow.get("verification_uri_complete", flow.get("verification_uri", "https://microsoft.com/devicelogin")))

result = app.acquire_token_by_device_flow(flow)

if "access_token" not in result:
    raise SystemExit(f"인증 실패: {result.get('error_description', result)}")

print()
print("인증 성공!")
print()

# To Do 목록 조회
r = requests.get(
    "https://graph.microsoft.com/v1.0/me/todo/lists",
    headers={"Authorization": f"Bearer {result['access_token']}"},
    timeout=15,
)
r.raise_for_status()
lists = r.json()["value"]

print("Microsoft To Do 목록:")
for i, lst in enumerate(lists):
    print(f"  {i + 1}. {lst['displayName']}")

print()
choice = int(input("동기화할 목록 번호를 입력하세요: ")) - 1
selected_list = lists[choice]

print()
print("=" * 60)
print("  GitHub Secrets에 아래 값들을 추가하세요")
print("  (레포 → Settings → Secrets → Actions → New repository secret)")
print("=" * 60)
print(f"\nAZURE_CLIENT_ID\n  {CLIENT_ID}\n")
print(f"AZURE_REFRESH_TOKEN\n  {result['refresh_token']}\n")
print(f"MSTODO_LIST_ID\n  {selected_list['id']}\n")
print(f"NOTION_TOKEN\n  <Notion 통합 토큰 (secret_...를 재발급 후 입력)>\n")
print(f"NOTION_DB_ID\n  dadf27b55389404296df607af4d16e26\n")
print("=" * 60)
print("설정 완료 후 GitHub Actions에서 수동으로 한 번 실행해보세요!")
