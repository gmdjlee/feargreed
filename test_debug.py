"""디버그 모드로 실행하여 5일 이동평균 계산 검증"""
import sys
sys.path.insert(0, '/home/user/feargreed')

from main import main

if __name__ == "__main__":
    print("=" * 80)
    print("디버그 모드 실행: 5일 이동평균 계산 검증")
    print("=" * 80)
    main(debug=True)
