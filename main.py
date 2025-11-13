import json
import sys
from datetime import datetime

import pandas as pd
import requests

# 한글 출력 문제 해결
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

OPTION_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/contents/MMC/ISIF/isif/MMCISIF013.cmd",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# 옵션 데이터용 페이로드 템플릿 (날짜와 isuOpt는 사용 시 추가)
OPTION_PAYLOAD_TEMPLATE = {
    "inqTpCd": "2",
    "prtType": "QTY",
    "prtCheck": "SU",
    "isuCd02": "KR___OPK2I",
    "isuCd": "KR___OPK2I",
    "aggBasTpCd": "",
    "prodId": "KR___OPK2I",
    "bld": "dbms/MDC/STAT/standard/MDCSTAT13102",
}

BOND_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://data.krx.co.kr",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# 채권 지수용 페이로드 템플릿 (날짜는 사용 시 추가)
BOND_PAYLOAD_TEMPLATE = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT01201",
    "locale": "ko_KR",
    "param1idxCd_finder_drvetcidx0_1": "",
    "csvxls_isNo": "false",
}

# 채권 지수 타입 매핑
BOND_INDEX_MAPPING = {
    "5년 국채선물 추종 지수": {
        "name": "5년 국채선물 추종 지수",
        "indTpCd": "D",
        "idxIndCd": "896",
        "idxCd": "D",
        "idxCd2": "896",
    },
    "10년국채선물지수": {
        "name": "10년국채선물지수",
        "indTpCd": "1",
        "idxIndCd": "309",
        "idxCd": "1",
        "idxCd2": "309",
    },
}


# 공통 유틸리티 함수
def format_date(date_str):
    """날짜 형식 변환 (YYYYMMDD -> YYYY-MM-DD)"""
    try:
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return date_obj.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return date_str


def fetch_data(session, url, headers, payload):
    """공통 데이터 조회 함수"""
    try:
        response = session.post(url, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        return response.json() if response.text else None
    except (json.JSONDecodeError, requests.exceptions.RequestException):
        return None


class KOSPI200OptionVolume:
    """코스피200 옵션 거래량 조회 클래스"""

    def __init__(self):
        self.base_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.session = requests.Session()
        self.headers = OPTION_HEADERS
        self._initialize_session()

    def _initialize_session(self):
        """세션 초기화 - 페이지 방문하여 쿠키 획득"""
        try:
            init_url = "https://data.krx.co.kr/contents/MMC/ISIF/isif/MMCISIF013.cmd"
            self.session.get(init_url, headers=self.headers, timeout=10)
        except Exception:
            pass

    def get_option_volume(self, start_date, end_date, option_type="C"):
        """
        코스피200 옵션 거래량 조회

        Parameters:
        -----------
        start_date : str
            시작일 (YYYYMMDD 형식)
        end_date : str
            종료일 (YYYYMMDD 형식)
        option_type : str, optional
            옵션 타입 (기본값: "C")
            - "C": Call 옵션
            - "P": Put 옵션

        Returns:
        --------
        dict : 옵션 거래량 데이터
        """
        if option_type not in ["C", "P"]:
            raise ValueError(f"Invalid option_type: {option_type}. Must be 'C' or 'P'.")

        payload = OPTION_PAYLOAD_TEMPLATE.copy()
        payload.update({
            "strtDd": start_date,
            "endDd": end_date,
            "isuOpt": option_type,  # C: Call, P: Put
        })

        return fetch_data(self.session, self.base_url, self.headers, payload)

    def parse_and_display(self, data):
        """데이터 파싱 및 출력"""
        if not data:
            return None

        df = pd.DataFrame(data.get("block1") or data.get("output", []))
        if df.empty:
            return None

        # 날짜 형식 변환
        if "TRD_DD" in df.columns:
            df["거래일"] = df["TRD_DD"].apply(
                lambda x: x.replace("/", "-") if "/" in str(x) else format_date(str(x))
            )

        # 숫자 데이터 정리
        numeric_columns = ["A07", "A08", "A09", "A12", "AMT_OR_QTY"]
        for col in [c for c in numeric_columns if c in df.columns]:
            df[col] = df[col].apply(
                lambda x: int(str(x).replace(",", "")) if isinstance(x, str) else x
            )

        # 컬럼명 매핑
        if "A07" in df.columns:
            df.rename(columns={
                "A07": "기관합계",
                "A08": "기타법인",
                "A09": "개인",
                "A12": "외국인합계",
                "AMT_OR_QTY": "전체",
            }, inplace=True)

        return df


class BondIndexData:
    """채권 지수 데이터 조회 클래스"""

    def __init__(self):
        self.base_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.session = requests.Session()
        self.headers = BOND_HEADERS
        self._initialize_session()

    def _initialize_session(self):
        """세션 초기화 - 페이지 방문하여 쿠키 획득"""
        try:
            init_url = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301"
            self.session.get(init_url, headers=self.headers, timeout=10)
        except Exception:
            pass

    def get_bond_index(self, start_date, end_date, index_type="5년 국채선물 추종 지수"):
        """
        채권 지수 데이터 조회

        Parameters:
        -----------
        start_date : str
            시작일 (YYYYMMDD 형식)
        end_date : str
            종료일 (YYYYMMDD 형식)
        index_type : str, optional
            채권 지수 타입 (기본값: "5년 국채선물 추종 지수")
            - "5년 국채선물 추종 지수"
            - "10년국채선물지수"

        Returns:
        --------
        dict : 채권 지수 데이터를 포함하는 딕셔너리
        """
        if index_type not in BOND_INDEX_MAPPING:
            valid_types = ", ".join(BOND_INDEX_MAPPING.keys())
            raise ValueError(f"Invalid index_type: {index_type}. Must be one of: {valid_types}")

        index_info = BOND_INDEX_MAPPING[index_type]
        payload = BOND_PAYLOAD_TEMPLATE.copy()
        payload.update({
            "strtDd": start_date,
            "endDd": end_date,
            "indTpCd": index_info["indTpCd"],
            "idxIndCd": index_info["idxIndCd"],
            "idxCd": index_info["idxCd"],
            "idxCd2": index_info["idxCd2"],
            "tboxidxCd_finder_drvetcidx0_1": index_info["name"],
            "codeNmidxCd_finder_drvetcidx0_1": index_info["name"],
        })

        return fetch_data(self.session, self.base_url, self.headers, payload)

    def parse_and_display(self, data):
        """데이터 파싱"""
        if not data:
            return None

        df = pd.DataFrame(data.get("block1") or data.get("output", []))
        if df.empty:
            return None

        # 날짜 형식 변환
        if "TRD_DD" in df.columns:
            df["거래일"] = df["TRD_DD"].apply(
                lambda x: x.replace("/", "-") if "/" in str(x) else format_date(str(x))
            )

        return df


def main():
    """메인 함수 - 옵션 데이터와 채권 지수 데이터 조회 예시"""
    start_date, end_date = "20251103", "20251108"

    # 옵션 거래량 조회
    option_volume = KOSPI200OptionVolume()
    call_result = option_volume.get_option_volume(start_date, end_date, option_type="C")
    call_df = option_volume.parse_and_display(call_result)
    if isinstance(call_df, pd.DataFrame) and not call_df.empty:
        call_df.to_csv(f"kospi200_call_option_{start_date}_{end_date}.csv", index=False, encoding="utf-8-sig")

    put_result = option_volume.get_option_volume(start_date, end_date, option_type="P")
    put_df = option_volume.parse_and_display(put_result)
    if isinstance(put_df, pd.DataFrame) and not put_df.empty:
        put_df.to_csv(f"kospi200_put_option_{start_date}_{end_date}.csv", index=False, encoding="utf-8-sig")

    # 채권 지수 조회
    bond_index = BondIndexData()
    bond_5y_result = bond_index.get_bond_index(start_date, end_date, index_type="5년 국채선물 추종 지수")
    bond_5y_df = bond_index.parse_and_display(bond_5y_result)
    if isinstance(bond_5y_df, pd.DataFrame) and not bond_5y_df.empty:
        bond_5y_df.to_csv(f"bond_5year_index_{start_date}_{end_date}.csv", index=False, encoding="utf-8-sig")

    bond_10y_result = bond_index.get_bond_index(start_date, end_date, index_type="10년국채선물지수")
    bond_10y_df = bond_index.parse_and_display(bond_10y_result)
    if isinstance(bond_10y_df, pd.DataFrame) and not bond_10y_df.empty:
        bond_10y_df.to_csv(f"bond_10year_index_{start_date}_{end_date}.csv", index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
