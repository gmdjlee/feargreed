"""KOSPI API 테스트 - 간단 버전"""
import sys
sys.path.insert(0, '/home/user/feargreed')

from main import IndexData

# IndexData 인스턴스 생성
idx = IndexData()

# KOSPI 데이터 조회
print("=" * 80)
print("KOSPI 데이터 조회 테스트")
print("=" * 80)

kospi_data = idx.get("20250303", "20250310", "KOSPI")

if kospi_data:
    print(f"\n✓ API 응답 받음")
    print(f"응답 키: {list(kospi_data.keys())}")

    if "output" in kospi_data:
        print(f"\noutput 키 존재: {len(kospi_data['output'])} 개 데이터")
        if kospi_data['output']:
            print(f"첫 번째 데이터:")
            for key, value in kospi_data['output'][0].items():
                print(f"  {key}: {value}")

    if "block1" in kospi_data:
        print(f"\nblock1 키 존재: {len(kospi_data['block1'])} 개 데이터")
        if kospi_data['block1']:
            print(f"첫 번째 데이터:")
            for key, value in kospi_data['block1'][0].items():
                print(f"  {key}: {value}")

    # 파싱 테스트
    print(f"\n{'='*80}")
    print("파싱 테스트")
    print("=" * 80)
    kospi_df = idx.parse(kospi_data)

    if kospi_df is not None and not kospi_df.empty:
        print(f"✓ 파싱 성공: {len(kospi_df)} 행")
        print(f"컬럼: {list(kospi_df.columns)}")
        print(f"\n데이터 샘플:")
        print(kospi_df.head().to_string(index=False))
    else:
        print("❌ 파싱 실패: 데이터프레임이 비어있음")
else:
    print("❌ API 응답이 None입니다")

print(f"\n{'='*80}")
print("KOSDAQ 데이터 조회 테스트")
print("=" * 80)

kosdaq_data = idx.get("20250303", "20250310", "KOSDAQ")

if kosdaq_data:
    print(f"\n✓ API 응답 받음")
    print(f"응답 키: {list(kosdaq_data.keys())}")

    if "output" in kosdaq_data:
        print(f"\noutput 키 존재: {len(kosdaq_data['output'])} 개 데이터")

    if "block1" in kosdaq_data:
        print(f"\nblock1 키 존재: {len(kosdaq_data['block1'])} 개 데이터")

    # 파싱 테스트
    kosdaq_df = idx.parse(kosdaq_data)

    if kosdaq_df is not None and not kosdaq_df.empty:
        print(f"✓ 파싱 성공: {len(kosdaq_df)} 행")
        print(f"컬럼: {list(kosdaq_df.columns)}")
    else:
        print("❌ 파싱 실패: 데이터프레임이 비어있음")
else:
    print("❌ API 응답이 None입니다")
