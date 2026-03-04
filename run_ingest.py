#!/usr/bin/env python3
"""
수집 파이프라인 CLI

사용 예시:
  # 전체 1학년 학교문제 수집 (분류 없이)
  python run_ingest.py --source school --grade 1학년 --no-classify

  # 특정 연도 폴더만
  python run_ingest.py --source school --grade 1학년 --year 2023-09

  # 테스트 (최대 5개 파일만)
  python run_ingest.py --source school --grade 1학년 --max-files 5

  # 모의고사 수집
  python run_ingest.py --source mock

  # DB 통계 확인
  python run_ingest.py --stats
"""

import argparse
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path 에 추가
sys.path.insert(0, str(Path(__file__).parent))

from pipeline.config import DB_PATH, GEMINI_API_KEY, SCHOOL_DIR, MOCK_DIR
from pipeline.db import init_db
from pipeline.ingest import ingest_school_directory, ingest_mock_directory
from pipeline.search import get_stats


def main():
    parser = argparse.ArgumentParser(description="고1 영어 문제 수집 파이프라인")
    parser.add_argument("--source", choices=["school", "mock", "all"], default="school",
                        help="수집 소스: school(출판사 txt) | mock(모의고사 PDF) | all")
    parser.add_argument("--grade", default="1학년", help="학년 폴더명 (예: 1학년)")
    parser.add_argument("--year", default=None, help="특정 연도-월 폴더만 (예: 2023-09)")
    parser.add_argument("--no-classify", action="store_true",
                        help="sub_type 분류 안 함 (빠른 수집)")
    parser.add_argument("--max-files", type=int, default=None,
                        help="최대 처리 파일 수 (테스트용)")
    parser.add_argument("--api-key", default=GEMINI_API_KEY, help="Gemini API 키")
    parser.add_argument("--stats", action="store_true", help="DB 통계만 출력 후 종료")
    parser.add_argument("--init-db", action="store_true", help="DB 초기화만 수행")

    args = parser.parse_args()

    if args.init_db:
        init_db(DB_PATH)
        print("DB 초기화 완료")
        return

    if args.stats:
        init_db(DB_PATH)
        stats = get_stats()
        print("\n=== DB 통계 ===")
        print(f"  총 문항 수     : {stats['total_questions']:,}")
        print(f"  검수 필요      : {stats['needs_review']:,}")
        print(f"\n  세부 유형 분포:")
        for st, cnt in stats["sub_type_distribution"].items():
            print(f"    {st:<12} : {cnt:,}")
        return

    classify = not args.no_classify
    api_key = args.api_key

    if args.source in ("school", "all"):
        print(f"\n[학교문제 수집 시작] grade={args.grade}  year={args.year or '전체'}")
        ingest_school_directory(
            school_dir=SCHOOL_DIR,
            grade=args.grade,
            year_filter=args.year,
            classify=classify,
            max_files=args.max_files,
            api_key=api_key,
        )

    if args.source in ("mock", "all"):
        print("\n[모의고사 수집 시작]")
        ingest_mock_directory(
            mock_dir=MOCK_DIR,
            classify=classify,
            api_key=api_key,
        )


if __name__ == "__main__":
    main()
