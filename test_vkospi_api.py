import json
import requests

# 헤더 설정
INDEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301",
    "X-Requested-With": "XMLHttpRequest",
}

# 세션 초기화
session = requests.Session()
init_url = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301"
try:
    session.get(init_url, headers=INDEX_HEADERS, timeout=10)
    print("✓ 세션 초기화 완료")
except Exception as e:
    print(f"❌ 세션 초기화 실패: {e}")

# VKOSPI 데이터 요청 (기존 동작하는 코드)
url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
payload = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT01201",
    "locale": "ko_KR",
    "strtDd": "20251106",
    "endDd": "20251114",
    "indTpCd": "1",
    "idxIndCd": "300",
    "idxCd": "1",
    "idxCd2": "300",
    "tboxidxCd_finder_drvetcidx0_1": "코스피 200 변동성지수",
    "codeNmidxCd_finder_drvetcidx0_1": "코스피 200 변동성지수",
    "param1idxCd_finder_drvetcidx0_1": "",
    "csvxls_isNo": "false",
}

print("\n📊 VKOSPI 데이터 요청 중...")
print(f"Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")

try:
    response = session.post(url, headers=INDEX_HEADERS, data=payload, timeout=10)
    print(f"\n✓ 응답 상태 코드: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        print(f"\n응답 데이터 구조:")
        print(f"Keys: {list(data.keys())}")

        if "output" in data:
            print(f"\noutput 키의 데이터 수: {len(data['output'])}")
            if data['output']:
                print(f"\n첫 번째 데이터 샘플:")
                print(json.dumps(data['output'][0], indent=2, ensure_ascii=False))

        if "block1" in data:
            print(f"\nblock1 키의 데이터 수: {len(data['block1'])}")
            if data['block1']:
                print(f"\n첫 번째 데이터 샘플:")
                print(json.dumps(data['block1'][0], indent=2, ensure_ascii=False))
    else:
        print(f"❌ 오류 응답: {response.text}")

except Exception as e:
    print(f"❌ 요청 실패: {e}")
