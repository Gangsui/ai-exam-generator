#!/usr/bin/env python3
"""
교과서 본문 PDF → DB 인제스트 스크립트

사용법:
    python ingest_textbooks.py                    # 전체 인제스트
    python ingest_textbooks.py --subject 공통영어1  # 공통영어1만
    python ingest_textbooks.py --stats             # 통계 조회
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline.config import DB_PATH, TEXTBOOK_DIRS
from pipeline.db import get_connection, init_db, upsert_textbook
from pipeline.parsers.textbook_parser import (
    parse_textbook_filename,
    extract_english_text,
)


def ingest_textbooks(subject_filter=None):
    """교과서 본문 PDF를 파싱하여 DB에 저장."""
    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    total, ok, skip, err = 0, 0, 0, 0

    for subject_key, dir_path in TEXTBOOK_DIRS.items():
        if subject_filter and subject_key != subject_filter:
            continue
        if not dir_path.exists():
            print(f"[WARN] 디렉토리 없음: {dir_path}")
            continue

        pdfs = sorted(dir_path.glob("*.pdf"))
        for pdf_path in pdfs:
            total += 1
            meta = parse_textbook_filename(pdf_path.name)
            if not meta:
                print(f"  [SKIP] 파일명 파싱 실패: {pdf_path.name}")
                skip += 1
                continue

            try:
                result = extract_english_text(str(pdf_path))
                if not result["passage_text"]:
                    print(f"  [SKIP] 텍스트 없음: {pdf_path.name}")
                    skip += 1
                    continue

                data = {
                    "subject": meta["subject"],
                    "publisher": meta["publisher"],
                    "textbook_label": meta["textbook_label"],
                    "textbook_author": meta["textbook_author"],
                    "unit_no": meta["unit_no"],
                    "unit_title": result["unit_title"],
                    "semester_exam": meta["semester_exam"],
                    "passage_text": result["passage_text"],
                    "page_count": result["page_count"],
                    "file_path": str(pdf_path),
                }

                with conn:
                    tb_id = upsert_textbook(conn, data)

                print(
                    f"  [OK] {meta['subject']} {meta['textbook_label']} "
                    f"{meta['unit_no']}과 → {result['page_count']}페이지 "
                    f"({len(result['passage_text'])}자)"
                )
                ok += 1

            except Exception as e:
                print(f"  [ERROR] {pdf_path.name}: {e}")
                err += 1

    conn.close()
    print(f"\n[Textbook Ingest 완료] 총 {total}개 | 성공 {ok}개 | 건너뜀 {skip}개 | 오류 {err}개")


def show_stats():
    """교과서 본문 DB 통계 표시."""
    conn = get_connection(DB_PATH)
    
    # textbooks 테이블 존재 여부
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='textbooks'"
    ).fetchone()
    if not exists:
        print("textbooks 테이블이 없습니다. 먼저 인제스트를 실행하세요.")
        conn.close()
        return

    total = conn.execute("SELECT COUNT(*) FROM textbooks").fetchone()[0]
    print(f"\n=== 교과서 본문 통계 ===")
    print(f"총 본문: {total}개\n")

    # 과목별
    rows = conn.execute(
        "SELECT subject, COUNT(*) as cnt FROM textbooks GROUP BY subject ORDER BY subject"
    ).fetchall()
    for r in rows:
        print(f"  {r['subject']}: {r['cnt']}개")

    # 출판사별
    print()
    rows = conn.execute(
        "SELECT publisher, COUNT(*) as cnt FROM textbooks GROUP BY publisher ORDER BY publisher"
    ).fetchall()
    for r in rows:
        print(f"  {r['publisher']}: {r['cnt']}개")

    # 시험 기간별
    print()
    rows = conn.execute(
        "SELECT semester_exam, COUNT(*) as cnt FROM textbooks "
        "WHERE semester_exam IS NOT NULL GROUP BY semester_exam ORDER BY semester_exam"
    ).fetchall()
    for r in rows:
        print(f"  {r['semester_exam']}: {r['cnt']}개")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="교과서 본문 PDF → DB 인제스트")
    parser.add_argument("--subject", choices=["공통영어1", "공통영어2"],
                        help="특정 과목만 인제스트")
    parser.add_argument("--stats", action="store_true",
                        help="통계만 표시")
    args = parser.parse_args()

    if args.stats:
        show_stats()
    else:
        ingest_textbooks(subject_filter=args.subject)
        show_stats()


if __name__ == "__main__":
    main()
