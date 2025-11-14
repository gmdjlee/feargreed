import json
import sys
from datetime import datetime
from functools import reduce

import pandas as pd
import requests
from pykrx import stock

# 한글 출력 문제 해결
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# 헤더 설정
OPTION_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/contents/MMC/ISIF/isif/MMCISIF013.cmd",
}

INDEX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301",
}

# 페이로드 템플릿
OPTION_PAYLOAD = {
    "inqTpCd": "2",
    "prtType": "QTY",
    "prtCheck": "SU",
    "isuCd02": "KR___OPK2I",
    "isuCd": "KR___OPK2I",
    "aggBasTpCd": "",
    "prodId": "KR___OPK2I",
    "bld": "dbms/MDC/STAT/standard/MDCSTAT13102",
}

INDEX_PAYLOAD = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT01201",
    "locale": "ko_KR",
    "param1idxCd_finder_drvetcidx0_1": "",
    "csvxls_isNo": "false",
}

# 지수 매핑
INDEX_MAP = {
    "5년국채": {"indTpCd": "D", "idxIndCd": "896", "idxCd": "D", "idxCd2": "896"},
    "10년국채": {"indTpCd": "1", "idxIndCd": "309", "idxCd": "1", "idxCd2": "309"},
    "VKOSPI": {"indTpCd": "1", "idxIndCd": "300", "idxCd": "1", "idxCd2": "300"},
}

# 지수 전체 이름
INDEX_NAMES = {
    "5년국채": "5년 국채선물 추종 지수",
    "10년국채": "10년국채선물지수",
    "VKOSPI": "코스피 200 변동성지수",
}


def to_date_str(val):
    """날짜를 YYYY-MM-DD 형식으로 변환"""
    if isinstance(val, str):
        if "/" in val:
            return val.replace("/", "-")
        try:
            return datetime.strptime(val, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return val
    return val.strftime("%Y-%m-%d") if hasattr(val, "strftime") else val


def fetch(session, url, headers, payload):
    """데이터 조회"""
    try:
        response = session.post(url, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        return response.json() if response.text else None
    except (json.JSONDecodeError, requests.exceptions.RequestException):
        return None


class BaseFetcher:
    """데이터 조회 기본 클래스"""

    def __init__(self, init_url, headers):
        self.url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.session = requests.Session()
        self.headers = headers
        self._init_session(init_url)

    def _init_session(self, init_url):
        """세션 초기화"""
        try:
            self.session.get(init_url, headers=self.headers, timeout=10)
        except Exception:
            pass


class OptionData(BaseFetcher):
    """옵션 거래량 조회"""

    def __init__(self):
        super().__init__(
            "https://data.krx.co.kr/contents/MMC/ISIF/isif/MMCISIF013.cmd",
            OPTION_HEADERS,
        )

    def get(self, start_date, end_date, option_type="C"):
        """옵션 데이터 조회 (C: Call, P: Put)"""
        if option_type not in ["C", "P"]:
            raise ValueError(f"Invalid option_type: {option_type}")

        payload = OPTION_PAYLOAD.copy()
        payload.update({"strtDd": start_date, "endDd": end_date, "isuOpt": option_type})
        return fetch(self.session, self.url, self.headers, payload)

    def parse(self, data):
        """데이터 파싱"""
        if not data:
            return None

        df = pd.DataFrame(data.get("block1") or data.get("output", []))
        if df.empty:
            return None

        # 컬럼명 변경
        df.rename(
            columns={
                "TRD_DD": "거래일",
                "A07": "기관합계",
                "A08": "기타법인",
                "A09": "개인",
                "A12": "외국인합계",
                "AMT_OR_QTY": "전체",
            },
            inplace=True,
        )

        # 날짜 형식 변환
        if "거래일" in df.columns:
            df["거래일"] = df["거래일"].apply(to_date_str)

        # 숫자 변환
        for col in ["기관합계", "기타법인", "개인", "외국인합계", "전체"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: int(str(x).replace(",", "")) if isinstance(x, str) else x
                )

        return df


class IndexData(BaseFetcher):
    """지수 데이터 조회"""

    def __init__(self):
        super().__init__(
            "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301",
            INDEX_HEADERS,
        )

    def get(self, start_date, end_date, index_key):
        """지수 데이터 조회"""
        if index_key not in INDEX_MAP:
            raise ValueError(f"Invalid index_key: {index_key}")

        info = INDEX_MAP[index_key]
        name = INDEX_NAMES[index_key]

        payload = INDEX_PAYLOAD.copy()
        payload.update(
            {
                "strtDd": start_date,
                "endDd": end_date,
                "indTpCd": info["indTpCd"],
                "idxIndCd": info["idxIndCd"],
                "idxCd": info["idxCd"],
                "idxCd2": info["idxCd2"],
                "tboxidxCd_finder_drvetcidx0_1": name,
                "codeNmidxCd_finder_drvetcidx0_1": name,
            }
        )
        return fetch(self.session, self.url, self.headers, payload)

    def parse(self, data):
        """데이터 파싱"""
        if not data:
            return None

        df = pd.DataFrame(data.get("block1") or data.get("output", []))
        if df.empty:
            return None

        # 컬럼명 변경
        df.rename(
            columns={
                "TRD_DD": "거래일",
                "CLSPRC_IDX": "종가",
                "CMPPREVDD_IDX": "대비",
                "FLUC_RT": "등락률",
                "OPNPRC_IDX": "시가",
                "HGPRC_IDX": "고가",
                "LWPRC_IDX": "저가",
            },
            inplace=True,
        )

        # 날짜 형식 변환
        if "거래일" in df.columns:
            df["거래일"] = df["거래일"].apply(to_date_str)

        # 컬럼 순서 정렬
        cols = ["거래일", "종가", "대비", "등락률", "시가", "고가", "저가"]
        return df[[c for c in cols if c in df.columns]]


def get_market_indices(start, end):
    """코스피, 코스닥 지수 데이터 수집"""
    indices = {}
    for ticker, name in [("1001", "KOSPI"), ("2001", "KOSDAQ")]:
        df = stock.get_index_ohlcv(start, end, ticker).reset_index()
        indices[name] = df[["날짜", "종가"]].rename(columns={"날짜": "거래일", "종가": name})
        indices[name]["거래일"] = indices[name]["거래일"].apply(to_date_str)
    return indices["KOSPI"], indices["KOSDAQ"]


def combine_data(start, end, debug=False):
    """모든 데이터를 조합하여 JSON 생성"""
    # 데이터 수집
    opt = OptionData()
    call = opt.parse(opt.get(start, end, "C"))
    put = opt.parse(opt.get(start, end, "P"))

    idx = IndexData()
    bond5y = idx.parse(idx.get(start, end, "5년국채"))
    bond10y = idx.parse(idx.get(start, end, "10년국채"))
    vkospi = idx.parse(idx.get(start, end, "VKOSPI"))

    kospi, kosdaq = get_market_indices(start, end)

    # 유효성 검사
    if any(df is None or df.empty for df in [call, put, bond5y, bond10y, vkospi, kospi, kosdaq]):
        return None

    # 옵션 5일 이동평균 계산
    for df, col in [(call, "Call Option"), (put, "Put Option")]:
        df.sort_values("거래일", inplace=True)
        df.reset_index(drop=True, inplace=True)
        df[col] = df["전체"].rolling(5).mean()

    # 디버그 출력
    if debug:
        print(f"\n{'='*80}\nCall 옵션 5일 이동평균\n{'='*80}")
        print(call[["거래일", "전체", "Call Option"]].to_string(index=False))

    # 데이터 병합
    dfs = [
        bond5y[["거래일", "종가"]].rename(columns={"종가": "5년 국채선물 추종 지수"}),
        bond10y[["거래일", "종가"]].rename(columns={"종가": "10년국채선물지수"}),
        vkospi[["거래일", "종가"]].rename(columns={"종가": "코스피 200 변동성지수"}),
        kospi, kosdaq,
        call[["거래일", "Call Option"]],
        put[["거래일", "Put Option"]],
    ]

    result = reduce(lambda l, r: l.merge(r, on="거래일", how="outer"), dfs)
    return result.sort_values("거래일").reset_index(drop=True)


def save_csv(df, filename):
    """CSV 파일 저장"""
    if df is not None and not df.empty:
        df.to_csv(filename, index=False, encoding="utf-8-sig")


def main(debug=False):
    """메인 함수"""
    start, end = "20251103", "20251108"

    # 개별 데이터 저장
    opt = OptionData()
    for typ, name in [("C", "call"), ("P", "put")]:
        save_csv(opt.parse(opt.get(start, end, typ)), f"kospi200_{name}_option_{start}_{end}.csv")

    idx = IndexData()
    for key, name in [("5년국채", "bond_5year"), ("10년국채", "bond_10year"), ("VKOSPI", "vkospi200")]:
        save_csv(idx.parse(idx.get(start, end, key)), f"{name}_index_{start}_{end}.csv")

    # 조합 데이터 생성 및 저장
    combined = combine_data(start, end, debug)
    if combined is not None and not combined.empty:
        combined.to_json(f"combined_data_{start}_{end}.json", orient="records", force_ascii=False, indent=2)
        combined.to_csv(f"combined_data_{start}_{end}.csv", index=False, encoding="utf-8-sig", sep="\t")
        if debug:
            print(f"\n{'='*80}\n최종 조합 데이터\n{'='*80}")
            print(combined.to_string(index=False))


if __name__ == "__main__":
    main()
