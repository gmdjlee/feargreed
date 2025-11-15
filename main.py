import json
import sys
from datetime import datetime
from functools import reduce

import pandas as pd
import requests
from sklearn.preprocessing import MinMaxScaler

# 한글 출력 설정
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# === 설정 ===
# 헤더
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

# 페이로드
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
    "5년국채": {"type": "derivative", "indTpCd": "D", "idxIndCd": "896", "idxCd": "D", "idxCd2": "896"},
    "10년국채": {"type": "derivative", "indTpCd": "1", "idxIndCd": "309", "idxCd": "1", "idxCd2": "309"},
    "VKOSPI": {"type": "derivative", "indTpCd": "1", "idxIndCd": "300", "idxCd": "1", "idxCd2": "300"},
    "KOSPI": {"type": "market", "indIdx": "1", "indIdx2": "001"},
    "KOSDAQ": {"type": "market", "indIdx": "2", "indIdx2": "001"},
}

INDEX_NAMES = {
    "5년국채": "5년 국채선물 추종 지수",
    "10년국채": "10년국채선물지수",
    "VKOSPI": "코스피 200 변동성지수",
    "KOSPI": "코스피",
    "KOSDAQ": "코스닥",
}


# === 유틸리티 ===
def to_date(val):
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
        res = session.post(url, headers=headers, data=payload, timeout=10)
        res.raise_for_status()
        return res.json() if res.text else None
    except (json.JSONDecodeError, requests.exceptions.RequestException):
        return None


def to_num(x):
    """문자열을 숫자로 변환 (쉼표 제거)"""
    if isinstance(x, str) and x:
        return float(x.replace(",", ""))
    return x


# === 데이터 수집 ===
class BaseFetcher:
    def __init__(self, init_url, headers):
        self.url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.session = requests.Session()
        self.headers = headers
        try:
            self.session.get(init_url, headers=headers, timeout=10)
        except Exception:
            pass


class OptionData(BaseFetcher):
    def __init__(self):
        super().__init__("https://data.krx.co.kr/contents/MMC/ISIF/isif/MMCISIF013.cmd", OPTION_HEADERS)

    def get(self, start, end, opt_type="C"):
        if opt_type not in ["C", "P"]:
            raise ValueError(f"Invalid opt_type: {opt_type}")
        payload = OPTION_PAYLOAD.copy()
        payload.update({"strtDd": start, "endDd": end, "isuOpt": opt_type})
        return fetch(self.session, self.url, self.headers, payload)

    def parse(self, data):
        if not data:
            return None
        df = pd.DataFrame(data.get("block1") or data.get("output", []))
        if df.empty:
            return None

        df.rename(columns={
            "TRD_DD": "거래일",
            "A07": "기관",
            "A08": "법인",
            "A09": "개인",
            "A12": "외국인",
            "AMT_OR_QTY": "전체",
        }, inplace=True)

        df["거래일"] = df["거래일"].apply(to_date)
        for col in ["기관", "법인", "개인", "외국인", "전체"]:
            if col in df.columns:
                df[col] = df[col].apply(to_num).astype(int)
        return df


class IndexData(BaseFetcher):
    def __init__(self):
        super().__init__(
            "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301",
            INDEX_HEADERS,
        )

    def get(self, start, end, key):
        if key not in INDEX_MAP:
            raise ValueError(f"Invalid key: {key}")

        info = INDEX_MAP[key]
        name = INDEX_NAMES[key]

        if info["type"] == "market":
            payload = {
                "bld": "dbms/MDC/STAT/standard/MDCSTAT00301",
                "locale": "ko_KR",
                "tboxindIdx_finder_equidx0_4": name,
                "indIdx": info["indIdx"],
                "indIdx2": info["indIdx2"],
                "codeNmindIdx_finder_equidx0_4": name,
                "param1indIdx_finder_equidx0_4": "",
                "strtDd": start,
                "endDd": end,
                "share": "2",
                "money": "3",
                "csvxls_isNo": "false",
            }
        else:
            payload = INDEX_PAYLOAD.copy()
            payload.update({
                "strtDd": start,
                "endDd": end,
                "indTpCd": info["indTpCd"],
                "idxIndCd": info["idxIndCd"],
                "idxCd": info["idxCd"],
                "idxCd2": info["idxCd2"],
                "tboxidxCd_finder_drvetcidx0_1": name,
                "codeNmidxCd_finder_drvetcidx0_1": name,
            })

        return fetch(self.session, self.url, self.headers, payload)

    def parse(self, data):
        if not data:
            return None
        df = pd.DataFrame(data.get("block1") or data.get("output", []))
        if df.empty:
            return None

        df.rename(columns={
            "TRD_DD": "거래일",
            "CLSPRC_IDX": "종가",
            "CMPPREVDD_IDX": "대비",
            "FLUC_RT": "등락률",
            "OPNPRC_IDX": "시가",
            "HGPRC_IDX": "고가",
            "LWPRC_IDX": "저가",
        }, inplace=True)

        df["거래일"] = df["거래일"].apply(to_date)
        for col in ["종가", "대비", "등락률", "시가", "고가", "저가"]:
            if col in df.columns:
                df[col] = df[col].apply(to_num)
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 존재하는 컬럼만 반환
        cols = ["거래일", "종가", "대비", "등락률", "시가", "고가", "저가"]
        return df[[c for c in cols if c in df.columns]]


# === 데이터 조합 ===
def combine(start, end):
    """모든 데이터를 조합"""
    opt = OptionData()
    call = opt.parse(opt.get(start, end, "C"))
    put = opt.parse(opt.get(start, end, "P"))

    idx = IndexData()
    b5y = idx.parse(idx.get(start, end, "5년국채"))
    b10y = idx.parse(idx.get(start, end, "10년국채"))
    vix = idx.parse(idx.get(start, end, "VKOSPI"))
    kp = idx.parse(idx.get(start, end, "KOSPI"))
    kq = idx.parse(idx.get(start, end, "KOSDAQ"))

    if any(df is None or df.empty for df in [call, put, b5y, b10y, vix]):
        return None

    # 옵션 5일 이동평균
    for df, col in [(call, "Call"), (put, "Put")]:
        df.sort_values("거래일", inplace=True)
        df.reset_index(drop=True, inplace=True)
        df[col] = df["전체"].rolling(5).mean()

    # 병합
    dfs = [
        b5y[["거래일", "종가"]].rename(columns={"종가": "5년국채"}),
        b10y[["거래일", "종가"]].rename(columns={"종가": "10년국채"}),
        vix[["거래일", "종가"]].rename(columns={"종가": "VIX"}),
        call[["거래일", "Call"]],
        put[["거래일", "Put"]],
    ]

    if kp is not None and not kp.empty:
        dfs.append(kp[["거래일", "종가"]].rename(columns={"종가": "KOSPI"}))
    if kq is not None and not kq.empty:
        dfs.append(kq[["거래일", "종가"]].rename(columns={"종가": "KOSDAQ"}))

    return reduce(lambda l, r: l.merge(r, on="거래일", how="outer"), dfs).sort_values("거래일").reset_index(drop=True)


# === Fear & Greed 분석 ===
def calc_rsi(df, col, window=10):
    delta = df[col].diff(1)
    gain = delta.where(delta > 0, 0).rolling(window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    return df


def calc_fg(df, idx_col, vix_col, call_col, put_col, b5_col, b10_col):
    df['MA125'] = df[idx_col].rolling(125).mean()
    df['Mom'] = (df[idx_col] - df['MA125']) / df['MA125'] * 100
    df['PCR'] = df[put_col] / df[call_col]
    df['Vol'] = df[vix_col]
    df['Spread'] = df[b10_col] - df[b5_col]

    scaler = MinMaxScaler()
    df[['Mom', 'PCR', 'Vol', 'Spread', 'RSI']] = scaler.fit_transform(
        df[['Mom', 'PCR', 'Vol', 'Spread', 'RSI']]
    )

    df['FG'] = (df['Mom'] * 0.2 + (1 - df['PCR']) * 0.2 +
                (1 - df['Vol']) * 0.2 + df['Spread'] * 0.2 + df['RSI'] * 0.2)
    return df


def calc_macd(df, col, short=12, long=26, signal=9):
    df['EMA_S'] = df[col].ewm(span=short, adjust=False).mean()
    df['EMA_L'] = df[col].ewm(span=long, adjust=False).mean()
    df['MACD'] = df['EMA_S'] - df['EMA_L']
    df['Signal'] = df['MACD'].ewm(span=signal, adjust=False).mean()
    df['Osc'] = df['MACD'] - df['Signal']
    return df


def analyze(df):
    """Fear & Greed 분석"""
    df['거래일'] = pd.to_datetime(df['거래일'])

    # 수치 변환
    for col in ['5년국채', '10년국채', 'VIX', 'KOSPI', 'KOSDAQ', 'Call', 'Put']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # NaN 제거 (필수 컬럼만)
    req = ['5년국채', '10년국채', 'VIX', 'Call', 'Put']
    df = df.dropna(subset=req).copy()

    if len(df) == 0:
        print("❌ 데이터 없음")
        return None, None

    if len(df) < 125:
        print(f"⚠️  데이터 {len(df)}일 (권장: 125일 이상)")

    kp_df, kq_df = None, None

    # KOSPI 분석
    if 'KOSPI' in df.columns and df['KOSPI'].notna().any():
        kp_df = df.copy()
        kp_df = calc_rsi(kp_df, 'KOSPI')
        kp_df = calc_fg(kp_df, 'KOSPI', 'VIX', 'Call', 'Put', '5년국채', '10년국채')
        kp_df = calc_macd(kp_df, 'FG')
        kp_df = kp_df.dropna().copy()

        if len(kp_df) > 0:
            print(f"\n{'='*80}\nKOSPI Fear & Greed Index\n{'='*80}")
            print(kp_df[['거래일', 'KOSPI', 'FG', 'Osc']].tail(20).to_string(index=False))
        else:
            kp_df = None

    # KOSDAQ 분석
    if 'KOSDAQ' in df.columns and df['KOSDAQ'].notna().any():
        kq_df = df.copy()
        kq_df = calc_rsi(kq_df, 'KOSDAQ')
        kq_df = calc_fg(kq_df, 'KOSDAQ', 'VIX', 'Call', 'Put', '5년국채', '10년국채')
        kq_df = calc_macd(kq_df, 'FG')
        kq_df = kq_df.dropna().copy()

        if len(kq_df) > 0:
            print(f"\n{'='*80}\nKOSDAQ Fear & Greed Index\n{'='*80}")
            print(kq_df[['거래일', 'KOSDAQ', 'FG', 'Osc']].tail(20).to_string(index=False))
        else:
            kq_df = None

    if kp_df is None and kq_df is None:
        print("❌ 분석 실패")
        return None, None

    return kp_df, kq_df


# === 메인 ===
def main():
    start, end = "20250303", "20250310"

    print(f"\n{'='*80}")
    print(f"Fear & Greed Index 분석: {start} ~ {end}")
    print(f"{'='*80}\n")

    # 데이터 수집
    df = combine(start, end)
    if df is None or df.empty:
        print("❌ 데이터 수집 실패")
        return

    print(f"✓ 조합 데이터: {len(df)} 행\n")
    print(df.to_string(index=False))

    # 분석
    analyze(df)


if __name__ == "__main__":
    main()
