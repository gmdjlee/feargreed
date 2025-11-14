import json
import sys
from datetime import datetime

import pandas as pd
import requests
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# ========== 설정 ==========

OPTION_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/contents/MMC/ISIF/isif/MMCISIF013.cmd",
}

BOND_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301",
}

OPTION_PAYLOAD = {
    "inqTpCd": "2",
    "prtType": "QTY",
    "prtCheck": "SU",
    "isuCd02": "KR___OPK2I",
    "isuCd": "KR___OPK2I",
    "aggBasTpCd": "",
    "isuOpt": "C",
    "prodId": "KR___OPK2I",
    "bld": "dbms/MDC/STAT/standard/MDCSTAT13102",
}

BOND_PAYLOAD = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT01201",
    "locale": "ko_KR",
    "indTpCd": "D",
    "idxIndCd": "896",
    "tboxidxCd_finder_drvetcidx0_1": "5년 국채선물 추종 지수",
    "idxCd": "D",
    "idxCd2": "896",
    "codeNmidxCd_finder_drvetcidx0_1": "5년 국채선물 추종 지수",
    "param1idxCd_finder_drvetcidx0_1": "",
    "csvxls_isNo": "flase",
}

BOND_IDX = {
    "5y": {"name": "5년 국채선물 추종 지수", "code": "896"},
    "10y": {"name": "10년국채선물지수", "code": "897"},
}

MARKET_IDX = {
    "KOSPI": {"name": "KOSPI", "code": "001"},
    "KOSDAQ": {"name": "KOSDAQ", "code": "301"},
}

VIX_PAYLOAD = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT30301",
    "locale": "ko_KR",
    "trdDd": "",
    "share": "1",
    "money": "1",
}


# ========== 유틸리티 함수 ==========


def fmt_date(date_str):
    """YYYYMMDD -> YYYY-MM-DD"""
    try:
        return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return date_str


def init_session(url, headers):
    """세션 초기화"""
    session = requests.Session()
    try:
        session.get(url, headers=headers, timeout=10)
    except Exception as e:
        print(f"세션 초기화 오류: {e}")
    return session


# ========== 데이터 수집 클래스 ==========


class KRXDataFetcher:
    """KRX 데이터 수집 통합 클래스"""

    def __init__(self):
        self.base_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    def fetch_option(self, start_date, end_date, opt_type="C"):
        """옵션 거래량 조회 (C: Call, P: Put)"""
        session = init_session(
            "https://data.krx.co.kr/contents/MMC/ISIF/isif/MMCISIF013.cmd",
            OPTION_HEADERS
        )

        payload = OPTION_PAYLOAD.copy()
        payload.update({"strtDd": start_date, "endDd": end_date, "isuOpt": opt_type})

        return self._post_request(session, payload, OPTION_HEADERS, opt_type)

    def fetch_bond(self, start_date, end_date, bond_type="5y"):
        """채권 지수 조회 (5y, 10y)"""
        session = init_session(
            "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301",
            BOND_HEADERS
        )

        idx = BOND_IDX[bond_type]
        payload = BOND_PAYLOAD.copy()
        payload.update({
            "strtDd": start_date,
            "endDd": end_date,
            "idxIndCd": idx["code"],
            "idxCd2": idx["code"],
            "tboxidxCd_finder_drvetcidx0_1": idx["name"],
            "codeNmidxCd_finder_drvetcidx0_1": idx["name"],
        })

        return self._post_request(session, payload, BOND_HEADERS, bond_type)

    def fetch_market_index(self, start_date, end_date, market="KOSPI"):
        """시장 지수 조회 (KOSPI, KOSDAQ)"""
        session = init_session(
            "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
            BOND_HEADERS
        )

        idx = MARKET_IDX[market]
        payload = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT00101",
            "locale": "ko_KR",
            "mktId": "STK" if market == "KOSPI" else "KSQ",
            "idxIndMidclssCd": idx["code"],
            "trdDd": end_date,
            "strtDd": start_date,
            "endDd": end_date,
            "share": "1",
            "money": "1",
        }

        return self._post_request(session, payload, BOND_HEADERS, market)

    def fetch_vix(self, start_date, end_date):
        """KOSPI 200 변동성지수 조회"""
        session = init_session(
            "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020301",
            BOND_HEADERS
        )

        payload = VIX_PAYLOAD.copy()
        payload.update({"strtDd": start_date, "endDd": end_date})

        return self._post_request(session, payload, BOND_HEADERS, "VIX")

    def _post_request(self, session, payload, headers, data_type):
        """공통 POST 요청 처리"""
        try:
            resp = session.post(self.base_url, headers=headers, data=payload, timeout=10)
            resp.raise_for_status()

            if not resp.text:
                print(f"{data_type} 데이터 없음")
                return None

            return resp.json()
        except json.JSONDecodeError as e:
            print(f"{data_type} JSON 파싱 오류: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"{data_type} 요청 오류: {e}")
            return None

    def parse_data(self, data, data_type):
        """데이터 파싱"""
        if not data:
            return None

        df = pd.DataFrame(data.get("block1") or data.get("output") or [])
        if df.empty:
            return None

        # 날짜 처리
        if "TRD_DD" in df.columns:
            df["거래일"] = df["TRD_DD"].apply(
                lambda x: x.replace("/", "-") if "/" in str(x) else fmt_date(str(x))
            )

        # 숫자 처리
        num_cols = ["A07", "A08", "A09", "A12", "AMT_OR_QTY"]
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: int(str(x).replace(",", "")) if isinstance(x, str) else x
                )

        # 컬럼명 매핑
        if "A07" in df.columns:
            df = df.rename(columns={
                "A07": "기관합계",
                "A08": "기타법인",
                "A09": "개인",
                "A12": "외국인합계",
                "AMT_OR_QTY": "전체",
            })

        print(f"\n{data_type} 데이터 조회 완료 ({len(df)}건)")
        return df


# ========== 데이터 결합 ==========


def create_combine_data(call_df, put_df, bond5y_df, bond10y_df, market_df, vix_df, market="KOSPI"):
    """모든 데이터를 날짜 기준으로 병합하여 combine_data 생성"""
    print(f"\n{'='*60}")
    print(f"{market} combine_data 생성 중...")
    print(f"{'='*60}")

    # 날짜 컬럼 통일
    for df in [call_df, put_df, bond5y_df, bond10y_df, market_df, vix_df]:
        if df is not None and "거래일" in df.columns:
            df["Date"] = pd.to_datetime(df["거래일"])

    # Call/Put 옵션 데이터 병합
    if call_df is not None and put_df is not None:
        call_df = call_df.rename(columns={"전체": "Call Option"})
        put_df = put_df.rename(columns={"전체": "Put Option"})
        option_df = pd.merge(
            call_df[["Date", "Call Option"]],
            put_df[["Date", "Put Option"]],
            on="Date",
            how="outer"
        )
    else:
        print("오류: Call/Put 옵션 데이터 없음")
        return None

    # 채권 데이터 병합
    if bond5y_df is not None:
        bond5y_df = bond5y_df.rename(columns={"IDX_CLSPRC": "5년 국채선물 추종 지수"})
        combine = pd.merge(option_df, bond5y_df[["Date", "5년 국채선물 추종 지수"]], on="Date", how="outer")
    else:
        combine = option_df.copy()

    if bond10y_df is not None:
        bond10y_df = bond10y_df.rename(columns={"IDX_CLSPRC": "10년국채선물지수"})
        combine = pd.merge(combine, bond10y_df[["Date", "10년국채선물지수"]], on="Date", how="outer")

    # 시장 지수 병합
    if market_df is not None:
        market_df = market_df.rename(columns={"CLSPRC_IDX": market})
        combine = pd.merge(combine, market_df[["Date", market]], on="Date", how="outer")
    else:
        print(f"오류: {market} 지수 데이터 없음")
        return None

    # VIX 병합
    if vix_df is not None:
        vix_df = vix_df.rename(columns={"CLSPRC": "코스피 200 변동성지수"})
        combine = pd.merge(combine, vix_df[["Date", "코스피 200 변동성지수"]], on="Date", how="outer")
    else:
        print("오류: VIX 데이터 없음")
        return None

    # 날짜 정렬 및 NaN 제거
    combine = combine.sort_values("Date").reset_index(drop=True)

    print(f"\n결합 완료: {len(combine)}건")
    print(f"컬럼: {list(combine.columns)}")

    return combine


# ========== Fear & Greed Index 계산 ==========


def calc_rsi(df, col, window=10):
    """RSI 계산"""
    delta = df[col].diff(1)
    gain = delta.where(delta > 0, 0).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    df.loc[:, "RSI_10"] = 100 - (100 / (1 + rs))
    return df


def calc_fg_index(df, idx_col, vix_col, call_col, put_col, b5y_col, b10y_col):
    """Fear & Greed Index 계산"""
    # 지표 계산
    df.loc[:, "125_MA"] = df[idx_col].rolling(window=125).mean()
    df.loc[:, "Momentum"] = (df[idx_col] - df["125_MA"]) / df["125_MA"] * 100
    df.loc[:, "Put_Call_Ratio"] = df[put_col] / df[call_col]
    df.loc[:, "Market_Volatility"] = df[vix_col]
    df.loc[:, "Bond_Yield_Diff"] = df[b10y_col] - df[b5y_col]

    # 정규화
    scaler = MinMaxScaler()
    cols = ["Momentum", "Put_Call_Ratio", "Market_Volatility", "Bond_Yield_Diff", "RSI_10"]
    df[cols] = scaler.fit_transform(df[cols])

    # 가중 평균
    df.loc[:, "Fear_Greed_Index"] = (
        df["Momentum"] * 0.2 +
        (1 - df["Put_Call_Ratio"]) * 0.2 +
        (1 - df["Market_Volatility"]) * 0.2 +
        df["Bond_Yield_Diff"] * 0.2 +
        df["RSI_10"] * 0.2
    )
    return df


def calc_macd(df, col, short=12, long=26, signal=9):
    """MACD 오실레이터 계산"""
    df.loc[:, "Short_EMA"] = df[col].ewm(span=short, adjust=False).mean()
    df.loc[:, "Long_EMA"] = df[col].ewm(span=long, adjust=False).mean()
    df.loc[:, "MACD"] = df["Short_EMA"] - df["Long_EMA"]
    df.loc[:, "Signal_Line"] = df["MACD"].ewm(span=signal, adjust=False).mean()
    df.loc[:, "Oscillator"] = df["MACD"] - df["Signal_Line"]
    return df


def plot_fg_oscillator(df, date_col, idx_col, name="KOSPI", months=6):
    """Fear & Greed 오실레이터 그래프 생성"""
    recent = df[df[date_col] >= (df[date_col].max() - pd.DateOffset(months=months))]

    fig, ax1 = plt.subplots(figsize=(14, 7))

    ax1.plot(recent[date_col], recent["Oscillator"], label=f"FG Oscillator ({name})", color="b")
    ax1.set_xlabel("거래일")
    ax1.set_ylabel(f"FG Oscillator ({name})", color="b")
    ax1.tick_params(axis="y", labelcolor="b")
    ax1.grid(True)
    ax1.legend(loc="upper left")

    ax2 = ax1.twinx()
    ax2.plot(recent[date_col], recent[idx_col], label=f"{name} Index", color="g")
    ax2.set_ylabel(f"{name} Index", color="g")
    ax2.tick_params(axis="y", labelcolor="g")
    ax2.legend(loc="upper right")

    plt.title(f"Fear & Greed Oscillator - {name} ({months}M)")
    plt.tight_layout()

    filename = f"{name.lower()}_fg_oscillator.png"
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    print(f"그래프 저장: {filename}")
    plt.show()
    plt.close()


def analyze_fg_index(data, market="KOSPI"):
    """Fear & Greed Index 분석

    필수 컬럼: Date/거래일, KOSPI/KOSDAQ, Call Option, Put Option,
               5년 국채선물 추종 지수, 10년국채선물지수, 코스피 200 변동성지수
    """
    print(f"\n{'='*60}")
    print(f"{market} Fear & Greed Index 분석")
    print(f"{'='*60}")

    df = data.copy()

    # 날짜 변환
    if "Date" in df.columns:
        df["거래일"] = pd.to_datetime(df["Date"])
    elif "거래일" in df.columns:
        df["거래일"] = pd.to_datetime(df["거래일"])
    else:
        print("오류: 날짜 컬럼 없음")
        return None

    # 숫자 변환
    num_cols = ["5년 국채선물 추종 지수", "10년국채선물지수", "코스피 200 변동성지수",
                market, "Call Option", "Put Option"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna().copy()
    if df.empty:
        print(f"오류: {market} 유효 데이터 없음")
        return None

    # 지표 계산
    df = calc_rsi(df, market)
    df = calc_fg_index(
        df, market, "코스피 200 변동성지수", "Call Option", "Put Option",
        "5년 국채선물 추종 지수", "10년국채선물지수"
    )
    df = calc_macd(df, "Fear_Greed_Index")

    # 그래프 생성
    plot_fg_oscillator(df, "거래일", market, market, 6)

    # 통계 출력
    fg = df["Fear_Greed_Index"]
    osc = df["Oscillator"]
    print(f"\nFG Index - 평균: {fg.mean():.4f}, 최대: {fg.max():.4f}, 최소: {fg.min():.4f}")
    print(f"Oscillator - 평균: {osc.mean():.4f}")

    return df


# ========== 메인 함수 ==========


def main():
    print("="*60)
    print("KRX 파생상품 데이터 조회 & Fear & Greed Index 분석")
    print("="*60)

    start = "20240101"
    end = "20241231"

    fetcher = KRXDataFetcher()

    print(f"\n조회 기간: {fmt_date(start)} ~ {fmt_date(end)}")

    # 1. 옵션 데이터
    call_data = fetcher.fetch_option(start, end, "C")
    call_df = fetcher.parse_data(call_data, "Call")

    put_data = fetcher.fetch_option(start, end, "P")
    put_df = fetcher.parse_data(put_data, "Put")

    # 2. 채권 데이터
    bond5y_data = fetcher.fetch_bond(start, end, "5y")
    bond5y_df = fetcher.parse_data(bond5y_data, "5년 국채")

    bond10y_data = fetcher.fetch_bond(start, end, "10y")
    bond10y_df = fetcher.parse_data(bond10y_data, "10년 국채")

    # 3. KOSPI 지수
    kospi_data = fetcher.fetch_market_index(start, end, "KOSPI")
    kospi_idx_df = fetcher.parse_data(kospi_data, "KOSPI 지수")

    # 4. VIX
    vix_data = fetcher.fetch_vix(start, end)
    vix_df = fetcher.parse_data(vix_data, "VIX")

    # 조회 결과 요약
    print(f"\n{'='*60}")
    print("데이터 조회 결과")
    print(f"{'='*60}")
    print(f"Call 옵션: {'✓' if call_df is not None else '✗'}")
    print(f"Put 옵션: {'✓' if put_df is not None else '✗'}")
    print(f"5년 국채: {'✓' if bond5y_df is not None else '✗'}")
    print(f"10년 국채: {'✓' if bond10y_df is not None else '✗'}")
    print(f"KOSPI 지수: {'✓' if kospi_idx_df is not None else '✗'}")
    print(f"VIX: {'✓' if vix_df is not None else '✗'}")

    # combine_data 생성
    combine_data = create_combine_data(
        call_df, put_df, bond5y_df, bond10y_df, kospi_idx_df, vix_df, market="KOSPI"
    )

    # Fear & Greed Index 분석
    if combine_data is not None:
        kospi_fg = analyze_fg_index(combine_data, "KOSPI")

        if kospi_fg is not None:
            print(f"\n{'='*60}")
            print("분석 완료!")
            print(f"{'='*60}")
    else:
        print("\n오류: combine_data 생성 실패")


if __name__ == "__main__":
    main()
