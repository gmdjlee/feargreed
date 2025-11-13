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

# 옵션 데이터용 페이로드 템플릿 (날짜는 사용 시 추가)
OPTION_PAYLOAD_TEMPLATE = {
    "inqTpCd": "2",
    "prtType": "QTY",
    "prtCheck": "SU",
    "isuCd02": "KR___OPK2I",
    "isuCd": "KR___OPK2I",
    "aggBasTpCd": "",
    "isuOpt": "C",  # C: Call 옵션, P: Put 옵션
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
    "indTpCd": "D",
    "idxIndCd": "896",
    "tboxidxCd_finder_drvetcidx0_1": "5년 국채선물 추종 지수",
    "idxCd": "D",
    "idxCd2": "896",
    "codeNmidxCd_finder_drvetcidx0_1": "5년 국채선물 추종 지수",
    "param1idxCd_finder_drvetcidx0_1": "",
    "csvxls_isNo": "flase",
}

# 채권 지수 타입 매핑
BOND_INDEX_MAPPING = {
    "5년 국채선물 추종 지수": {
        "name": "5년 국채선물 추종 지수",
        "idxIndCd": "896",
        "idxCd2": "896",
    },
    "10년국채선물지수": {
        "name": "10년국채선물지수",
        "idxIndCd": "897",  # 실제 코드 확인 필요
        "idxCd2": "897",  # 실제 코드 확인 필요
    },
}


class KOSPI200OptionVolume:
    """코스피200 옵션 거래량 조회 클래스"""

    def __init__(self):
        self.base_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.session = requests.Session()  # 세션 객체 사용
        self.headers = OPTION_HEADERS  # 옵션 데이터용 헤더 사용
        # 세션 초기화 - 페이지 방문하여 쿠키 획득
        self._initialize_session()

    def _initialize_session(self):
        """세션 초기화 - 페이지 방문하여 쿠키 획득"""
        try:
            # 웹페이지 방문하여 세션 쿠키 획득
            init_url = "https://data.krx.co.kr/contents/MMC/ISIF/isif/MMCISIF013.cmd"
            self.session.get(init_url, headers=self.headers, timeout=10)
            print("세션 초기화 완료")
        except Exception as e:
            print(f"세션 초기화 중 오류: {e}")

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
        dict : Call과 Put 거래량 데이터를 포함하는 딕셔너리
        """

        # 옵션 타입 유효성 검사
        if option_type not in ["C", "P"]:
            raise ValueError(
                f"Invalid option_type: {option_type}. Must be 'C' (Call) or 'P' (Put)."
            )

        # 옵션용 페이로드 템플릿을 복사하고 날짜 및 옵션 타입 추가
        payload = OPTION_PAYLOAD_TEMPLATE.copy()
        payload["strtDd"] = start_date
        payload["endDd"] = end_date
        payload["isuOpt"] = option_type

        try:
            response = self.session.post(
                self.base_url, headers=self.headers, data=payload, timeout=10
            )
            response.raise_for_status()

            # 디버깅: 응답 내용 확인
            print(f"응답 상태 코드: {response.status_code}")
            print(f"응답 Content-Type: {response.headers.get('Content-Type', 'N/A')}")

            # 응답이 비어있는지 확인
            if not response.text:
                print("경고: 서버로부터 빈 응답을 받았습니다.")
                return None

            # JSON 파싱 시도
            try:
                data = response.json()
                return data
            except json.JSONDecodeError as e:
                print(f"JSON 파싱 오류: {e}")
                print("응답 내용 (처음 200자):")
                print(response.text[:200])

                # HTML 응답인 경우 파일로 저장
                if "text/html" in response.headers.get("Content-Type", ""):
                    error_file = "error_response.html"
                    with open(error_file, "w", encoding="utf-8") as f:
                        f.write(response.text)
                    print(f"\nHTML 응답이 '{error_file}' 파일로 저장되었습니다.")
                    print("API 요청 파라미터를 확인해주세요.")

                return None

        except requests.exceptions.RequestException as e:
            print(f"요청 중 오류 발생: {e}")
            return None

    def format_date(self, date_str):
        """날짜 형식 변환 (YYYYMMDD -> YYYY-MM-DD)"""
        try:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            return date_obj.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return date_str

    def parse_and_display(self, data):
        """데이터 파싱 및 출력"""
        if not data:
            print("데이터가 없습니다.")
            return None

        # "block1" 또는 "output" 키 처리
        if "block1" in data:
            df = pd.DataFrame(data["block1"])
        elif "output" in data:
            df = pd.DataFrame(data["output"])
        else:
            print("응답 데이터 형식이 예상과 다릅니다.")
            print("원본 데이터:", json.dumps(data, indent=2, ensure_ascii=False))
            return None

        # 컬럼명 한글화 (실제 응답 데이터 구조에 따라 조정 필요)
        print("\n" + "=" * 80)
        print("코스피200 옵션 거래량 조회 결과")
        print("=" * 80)

        if not df.empty:
            # 날짜 형식 변환 (2024/11/08 -> 2024-11-08 또는 20241108 -> 2024-11-08)
            if "TRD_DD" in df.columns:
                df["거래일"] = df["TRD_DD"].apply(
                    lambda x: x.replace("/", "-")
                    if "/" in str(x)
                    else self.format_date(str(x))
                )

            # 숫자 데이터 정리 (쉼표 제거 및 정수 변환)
            numeric_columns = ["A07", "A08", "A09", "A12", "AMT_OR_QTY"]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: int(str(x).replace(",", ""))
                        if isinstance(x, str)
                        else x
                    )

            # 컬럼명 매핑 (output 형식인 경우)
            if "A07" in df.columns:
                column_mapping = {
                    "A07": "기관합계",
                    "A08": "기타법인",
                    "A09": "개인",
                    "A12": "외국인합계",
                    "AMT_OR_QTY": "전체",
                }
                df = df.rename(columns=column_mapping)

            # 데이터 출력
            print(df.to_string(index=False))
            print("\n" + "=" * 80)

            # Call과 Put으로 구분된 데이터가 있는 경우
            if "CALL_VAL" in df.columns and "PUT_VAL" in df.columns:
                print("\n[통계 요약]")
                print(f"총 Call 거래량: {df['CALL_VAL'].sum():,.0f}")
                print(f"총 Put 거래량: {df['PUT_VAL'].sum():,.0f}")
                print(
                    f"Call/Put 비율: {df['CALL_VAL'].sum() / df['PUT_VAL'].sum():.2f}"
                )
            elif "콜옵션1" in df.columns and "풋옵션1" in df.columns:
                print("\n[통계 요약]")
                total_call = df["콜옵션1"].sum() + df["콜옵션2"].sum()
                total_put = df["풋옵션1"].sum() + df["풋옵션2"].sum()
                print(f"총 Call 거래량: {total_call:,.0f}")
                print(f"총 Put 거래량: {total_put:,.0f}")
                print(f"Call/Put 비율: {total_call / total_put:.2f}")
                print(f"총 거래량: {df['총거래량'].sum():,.0f}")

            return df
        else:
            print("조회된 데이터가 없습니다.")
            return None


class BondIndexData:
    """채권 지수 데이터 조회 클래스"""

    def __init__(self):
        self.base_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        self.session = requests.Session()  # 세션 객체 사용
        self.headers = BOND_HEADERS  # 채권 지수용 헤더 사용
        # 세션 초기화 - 페이지 방문하여 쿠키 획득
        self._initialize_session()

    def _initialize_session(self):
        """세션 초기화 - 페이지 방문하여 쿠키 획득"""
        try:
            # 웹페이지 방문하여 세션 쿠키 획득
            init_url = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010301"
            self.session.get(init_url, headers=self.headers, timeout=10)
            print("채권 지수 세션 초기화 완료")
        except Exception as e:
            print(f"채권 지수 세션 초기화 중 오류: {e}")

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

        # 지수 타입 유효성 검사
        if index_type not in BOND_INDEX_MAPPING:
            valid_types = ", ".join(BOND_INDEX_MAPPING.keys())
            raise ValueError(
                f"Invalid index_type: {index_type}. Must be one of: {valid_types}"
            )

        # 선택한 지수 정보 가져오기
        index_info = BOND_INDEX_MAPPING[index_type]

        # 채권 지수용 페이로드 템플릿을 복사하고 날짜 및 지수 타입 추가
        payload = BOND_PAYLOAD_TEMPLATE.copy()
        payload["strtDd"] = start_date
        payload["endDd"] = end_date
        payload["idxIndCd"] = index_info["idxIndCd"]
        payload["idxCd2"] = index_info["idxCd2"]
        payload["tboxidxCd_finder_drvetcidx0_1"] = index_info["name"]
        payload["codeNmidxCd_finder_drvetcidx0_1"] = index_info["name"]

        try:
            response = self.session.post(
                self.base_url, headers=self.headers, data=payload, timeout=10
            )
            response.raise_for_status()

            # 디버깅: 응답 내용 확인
            print(f"응답 상태 코드: {response.status_code}")
            print(f"응답 Content-Type: {response.headers.get('Content-Type', 'N/A')}")

            # 응답이 비어있는지 확인
            if not response.text:
                print("경고: 서버로부터 빈 응답을 받았습니다.")
                return None

            # JSON 파싱 시도
            try:
                data = response.json()
                return data
            except json.JSONDecodeError as e:
                print(f"JSON 파싱 오류: {e}")
                print("응답 내용 (처음 200자):")
                print(response.text[:200])

                # HTML 응답인 경우 파일로 저장
                if "text/html" in response.headers.get("Content-Type", ""):
                    error_file = "bond_error_response.html"
                    with open(error_file, "w", encoding="utf-8") as f:
                        f.write(response.text)
                    print(f"\nHTML 응답이 '{error_file}' 파일로 저장되었습니다.")
                    print("API 요청 파라미터를 확인해주세요.")

                return None

        except requests.exceptions.RequestException as e:
            print(f"요청 중 오류 발생: {e}")
            return None

    def format_date(self, date_str):
        """날짜 형식 변환 (YYYYMMDD -> YYYY-MM-DD)"""
        try:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            return date_obj.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return date_str

    def parse_and_display(self, data):
        """데이터 파싱 및 출력"""
        if not data:
            print("데이터가 없습니다.")
            return None

        # "block1" 또는 "output" 키 처리
        if "block1" in data:
            df = pd.DataFrame(data["block1"])
        elif "output" in data:
            df = pd.DataFrame(data["output"])
        else:
            print("응답 데이터 형식이 예상과 다릅니다.")
            print("원본 데이터:", json.dumps(data, indent=2, ensure_ascii=False))
            return None

        # 컬럼명 한글화 (실제 응답 데이터 구조에 따라 조정 필요)
        print("\n" + "=" * 80)
        print("채권 지수 데이터 조회 결과")
        print("=" * 80)

        if not df.empty:
            # 날짜 형식 변환
            if "TRD_DD" in df.columns:
                df["거래일"] = df["TRD_DD"].apply(
                    lambda x: x.replace("/", "-")
                    if "/" in str(x)
                    else self.format_date(str(x))
                )

            # 데이터 출력
            print(df.to_string(index=False))
            print("\n" + "=" * 80)

            return df
        else:
            print("조회된 데이터가 없습니다.")
            return None


def main():
    """메인 함수 - 옵션 데이터와 채권 지수 데이터 조회 예시"""
    print("=" * 80)
    print("KRX 파생상품 데이터 조회 프로그램")
    print("=" * 80)

    # 조회 기간 설정
    start_date = "20251103"
    end_date = "20251108"

    # ========== 1. 코스피200 Call 옵션 거래량 조회 ==========
    print("\n" + "=" * 80)
    print("[ 1-1. 코스피200 Call 옵션 거래량 조회 ]")
    print("=" * 80)
    print("사용 헤더: OPTION_HEADERS")
    print("사용 페이로드: OPTION_PAYLOAD_TEMPLATE (option_type='C')")

    option_volume = KOSPI200OptionVolume()
    print(
        f"\n조회 기간: {option_volume.format_date(start_date)} ~ {option_volume.format_date(end_date)}"
    )
    print("옵션 타입: Call (C)")
    print("데이터 조회 중...\n")

    # Call 옵션 데이터 조회
    call_result = option_volume.get_option_volume(start_date, end_date, option_type="C")

    # 데이터 파싱 및 출력
    call_df = option_volume.parse_and_display(call_result)

    # DataFrame을 CSV로 저장 (선택사항)
    if isinstance(call_df, pd.DataFrame) and not call_df.empty:
        filename = f"kospi200_call_option_{start_date}_{end_date}.csv"
        call_df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"\nCall 옵션 데이터가 '{filename}' 파일로 저장되었습니다.")

    # ========== 1-2. 코스피200 Put 옵션 거래량 조회 ==========
    print("\n" + "=" * 80)
    print("[ 1-2. 코스피200 Put 옵션 거래량 조회 ]")
    print("=" * 80)
    print("사용 헤더: OPTION_HEADERS")
    print("사용 페이로드: OPTION_PAYLOAD_TEMPLATE (option_type='P')")
    print(
        f"\n조회 기간: {option_volume.format_date(start_date)} ~ {option_volume.format_date(end_date)}"
    )
    print("옵션 타입: Put (P)")
    print("데이터 조회 중...\n")

    # Put 옵션 데이터 조회
    put_result = option_volume.get_option_volume(start_date, end_date, option_type="P")

    # 데이터 파싱 및 출력
    put_df = option_volume.parse_and_display(put_result)

    # DataFrame을 CSV로 저장 (선택사항)
    if isinstance(put_df, pd.DataFrame) and not put_df.empty:
        filename = f"kospi200_put_option_{start_date}_{end_date}.csv"
        put_df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"\nPut 옵션 데이터가 '{filename}' 파일로 저장되었습니다.")

    # ========== 2-1. 5년 국채선물 추종 지수 조회 ==========
    print("\n" + "=" * 80)
    print("[ 2-1. 5년 국채선물 추종 지수 조회 ]")
    print("=" * 80)
    print("사용 헤더: BOND_HEADERS")
    print("사용 페이로드: BOND_PAYLOAD_TEMPLATE (index_type='5년 국채선물 추종 지수')")

    bond_index = BondIndexData()
    print(
        f"\n조회 기간: {bond_index.format_date(start_date)} ~ {bond_index.format_date(end_date)}"
    )
    print("지수 타입: 5년 국채선물 추종 지수")
    print("데이터 조회 중...\n")

    # 5년 국채선물 지수 데이터 조회
    bond_5y_result = bond_index.get_bond_index(
        start_date, end_date, index_type="5년 국채선물 추종 지수"
    )

    # 데이터 파싱 및 출력
    bond_5y_df = bond_index.parse_and_display(bond_5y_result)

    # DataFrame을 CSV로 저장 (선택사항)
    if isinstance(bond_5y_df, pd.DataFrame) and not bond_5y_df.empty:
        filename = f"bond_5year_index_{start_date}_{end_date}.csv"
        bond_5y_df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"\n5년 국채선물 지수 데이터가 '{filename}' 파일로 저장되었습니다.")

    # ========== 2-2. 10년 국채선물 지수 조회 ==========
    print("\n" + "=" * 80)
    print("[ 2-2. 10년 국채선물 지수 조회 ]")
    print("=" * 80)
    print("사용 헤더: BOND_HEADERS")
    print("사용 페이로드: BOND_PAYLOAD_TEMPLATE (index_type='10년국채선물지수')")
    print(
        f"\n조회 기간: {bond_index.format_date(start_date)} ~ {bond_index.format_date(end_date)}"
    )
    print("지수 타입: 10년국채선물지수")
    print("데이터 조회 중...\n")

    # 10년 국채선물 지수 데이터 조회
    bond_10y_result = bond_index.get_bond_index(
        start_date, end_date, index_type="10년국채선물지수"
    )

    # 데이터 파싱 및 출력
    bond_10y_df = bond_index.parse_and_display(bond_10y_result)

    # DataFrame을 CSV로 저장 (선택사항)
    if isinstance(bond_10y_df, pd.DataFrame) and not bond_10y_df.empty:
        filename = f"bond_10year_index_{start_date}_{end_date}.csv"
        bond_10y_df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"\n10년 국채선물 지수 데이터가 '{filename}' 파일로 저장되었습니다.")

    # ========== 요약 ==========
    print("\n" + "=" * 80)
    print("[ 조회 완료 요약 ]")
    print("=" * 80)
    print(
        f"✓ Call 옵션 데이터: {'성공' if call_df is not None and not call_df.empty else '실패'}"
    )
    print(
        f"✓ Put 옵션 데이터: {'성공' if put_df is not None and not put_df.empty else '실패'}"
    )
    print(
        f"✓ 5년 국채 지수 데이터: {'성공' if bond_5y_df is not None and not bond_5y_df.empty else '실패'}"
    )
    print(
        f"✓ 10년 국채 지수 데이터: {'성공' if bond_10y_df is not None and not bond_10y_df.empty else '실패'}"
    )
    print("=" * 80)


if __name__ == "__main__":
    main()
