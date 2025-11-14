import json
import sys
from datetime import datetime
from functools import reduce

import pandas as pd
import requests
from pykrx import stock
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

# 한글 출력 문제 해결
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# matplotlib 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic' if sys.platform == "win32" else 'AppleGothic'
plt.rcParams['axes.unicode_minus'] = False

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


def calc_rsi(df, col, window=10):
    """RSI 계산"""
    delta = df[col].diff(1)
    gain = delta.where(delta > 0, 0).rolling(window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window).mean()
    rs = gain / loss
    df['RSI_10'] = 100 - (100 / (1 + rs))
    return df


def calc_fear_greed(df, idx_col, vix_col, call_col, put_col, bond5_col, bond10_col):
    """Fear & Greed Index 계산"""
    df['125_MA'] = df[idx_col].rolling(125).mean()
    df['Momentum'] = (df[idx_col] - df['125_MA']) / df['125_MA'] * 100
    df['Put_Call_Ratio'] = df[put_col] / df[call_col]
    df['Market_Volatility'] = df[vix_col]
    df['Bond_Yield_Diff'] = df[bond10_col] - df[bond5_col]

    scaler = MinMaxScaler()
    df[['Momentum', 'Put_Call_Ratio', 'Market_Volatility', 'Bond_Yield_Diff', 'RSI_10']] = scaler.fit_transform(
        df[['Momentum', 'Put_Call_Ratio', 'Market_Volatility', 'Bond_Yield_Diff', 'RSI_10']]
    )

    df['Fear_Greed_Index'] = (
        df['Momentum'] * 0.2 +
        (1 - df['Put_Call_Ratio']) * 0.2 +
        (1 - df['Market_Volatility']) * 0.2 +
        df['Bond_Yield_Diff'] * 0.2 +
        df['RSI_10'] * 0.2
    )
    return df


def calc_macd(df, col, short=12, long=26, signal=9):
    """MACD 오실레이터 계산"""
    df['Short_EMA'] = df[col].ewm(span=short, adjust=False).mean()
    df['Long_EMA'] = df[col].ewm(span=long, adjust=False).mean()
    df['MACD'] = df['Short_EMA'] - df['Long_EMA']
    df['Signal_Line'] = df['MACD'].ewm(span=signal, adjust=False).mean()
    df['Oscillator'] = df['MACD'] - df['Signal_Line']
    return df


def plot_fear_greed(df, idx_col, title, filename):
    """Fear & Greed 오실레이터와 지수 그래프"""
    recent = df[df['거래일'] >= (df['거래일'].max() - pd.DateOffset(months=6))]

    fig, ax1 = plt.subplots(figsize=(14, 7))
    ax1.plot(recent['거래일'], recent['Oscillator'], label='Fear & Greed Oscillator', color='b')
    ax1.set_xlabel('거래일')
    ax1.set_ylabel('Fear & Greed Oscillator', color='b')
    ax1.tick_params(axis='y', labelcolor='b')
    ax1.grid(True)
    ax1.legend(loc='upper left')

    ax2 = ax1.twinx()
    ax2.plot(recent['거래일'], recent[idx_col], label=f'{idx_col} Index', color='g')
    ax2.set_ylabel(f'{idx_col} Index', color='g')
    ax2.tick_params(axis='y', labelcolor='g')
    ax2.legend(loc='upper right')

    plt.title(title)
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()


def analyze_fear_greed(combined_df):
    """Fear & Greed 분석 수행"""
    # 날짜를 datetime으로 변환
    combined_df['거래일'] = pd.to_datetime(combined_df['거래일'])

    # 수치형 변환 및 NaN 제거
    numeric_cols = ['5년 국채선물 추종 지수', '10년국채선물지수', '코스피 200 변동성지수',
                    'KOSPI', 'KOSDAQ', 'Call Option', 'Put Option']
    for col in numeric_cols:
        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')

    combined_df = combined_df.dropna().copy()

    # KOSPI 분석
    kospi_df = combined_df.copy()
    kospi_df = calc_rsi(kospi_df, 'KOSPI')
    kospi_df = calc_fear_greed(kospi_df, 'KOSPI', '코스피 200 변동성지수', 'Call Option', 'Put Option',
                               '5년 국채선물 추종 지수', '10년국채선물지수')
    kospi_df = calc_macd(kospi_df, 'Fear_Greed_Index')

    # KOSDAQ 분석
    kosdaq_df = combined_df.copy()
    kosdaq_df = calc_rsi(kosdaq_df, 'KOSDAQ')
    kosdaq_df = calc_fear_greed(kosdaq_df, 'KOSDAQ', '코스피 200 변동성지수', 'Call Option', 'Put Option',
                                '5년 국채선물 추종 지수', '10년국채선물지수')
    kosdaq_df = calc_macd(kosdaq_df, 'Fear_Greed_Index')

    # 그래프 생성
    plot_fear_greed(kospi_df, 'KOSPI', 'Fear & Greed Oscillator and KOSPI Index (Recent 6 Months)',
                   'fear_greed_kospi.png')
    plot_fear_greed(kosdaq_df, 'KOSDAQ', 'Fear & Greed Oscillator and KOSDAQ Index (Recent 6 Months)',
                   'fear_greed_kosdaq.png')

    # 결과 저장
    kospi_df.to_csv('fear_greed_kospi.csv', index=False, encoding='utf-8-sig')
    kosdaq_df.to_csv('fear_greed_kosdaq.csv', index=False, encoding='utf-8-sig')

    return kospi_df, kosdaq_df


def main(debug=False, analyze=True):
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

        # Fear & Greed 분석 실행
        if analyze:
            print(f"\n{'='*80}\nFear & Greed 분석 시작\n{'='*80}")
            kospi_fg, kosdaq_fg = analyze_fear_greed(combined)
            print("✓ KOSPI Fear & Greed 분석 완료: fear_greed_kospi.csv, fear_greed_kospi.png")
            print("✓ KOSDAQ Fear & Greed 분석 완료: fear_greed_kosdaq.csv, fear_greed_kosdaq.png")


if __name__ == "__main__":
    main()
