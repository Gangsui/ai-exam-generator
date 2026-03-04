#!/usr/bin/env python3
"""
DB에 저장된 미분류 객관식 문항에 대해 sub_type 분류를 실행합니다.

사용 예시:
  # 규칙 기반만 (빠름, 무료)
  python classify_existing.py

  # Gemini로 '기타' 재분류 (API 키 필요)
  python classify_existing.py --gemini

  # 전체 재분류 (이미 분류된 것도 다시)
  python classify_existing.py --force

  # 최대 N개만 (테스트용)
  python classify_existing.py --limit 100
"""

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.config import DB_PATH, GEMINI_API_KEY
from pipeline.classifiers.subtype_classifier import classify_question, classify_questions_batch


def classify_existing(api_key: str = "", limit: int | None = None, batch_size: int = 500):
    import time
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 객관식 미분류 문항만 대상
    total_unclassified = conn.execute(
        "SELECT COUNT(*) FROM questions WHERE question_type='객관식' "
        "AND (sub_type_pred IS NULL OR sub_type_pred = '미분류')"
    ).fetchone()[0]

    print(f"미분류 객관식 문항: {total_unclassified:,}개")
    if limit:
        total_unclassified = min(total_unclassified, limit)
        print(f"처리 한도: {limit:,}개")

    processed = 0
    updated = 0
    gemini_used = 0
    gemini_failed = 0
    last_id = 0  # id 기반 페이지네이션 (OFFSET 대신)
    use_gemini = bool(api_key)

    while True:
        rows = conn.execute(
            """SELECT id, question_text, choices, passage_text, question_type
               FROM questions
               WHERE question_type='객관식'
                 AND (sub_type_pred IS NULL OR sub_type_pred = '미분류')
                 AND id > ?
               ORDER BY id
               LIMIT ?""",
            (last_id, batch_size),
        ).fetchall()

        if not rows:
            break

        last_id = rows[-1]["id"]

        cur = conn.cursor()
        for row in rows:
            q = {
                "question_text": row["question_text"],
                "choices": row["choices"],
                "passage_text": row["passage_text"],
                "question_type": row["question_type"],
            }
            result = classify_question(q, api_key=api_key)

            sub_type = result.get("sub_type_pred") or "기타"
            confidence = result.get("sub_type_confidence", 0.0)
            model = result.get("classifier_model", "rule_based")
            needs_review = result.get("needs_review", 0)

            actually_used_gemini = (model and "gemini" in model.lower()
                                    and sub_type != "기타")
            if actually_used_gemini:
                gemini_used += 1
            elif use_gemini and sub_type == "기타":
                gemini_failed += 1

            cur.execute(
                """UPDATE questions
                   SET sub_type_pred=?, sub_type_confidence=?,
                       classifier_model=?, classifier_version=?, needs_review=?
                   WHERE id=?""",
                (sub_type, confidence, model, result.get("classifier_version", ""),
                 needs_review, row["id"]),
            )
            updated += 1
            processed += 1

            if processed % 10 == 0 or processed == total_unclassified:
                conn.commit()
                if use_gemini:
                    print(f"  진행: {processed:,}/{total_unclassified:,}  |  Gemini 성공: {gemini_used}  |  기타유지: {gemini_failed}", end="\r")
                else:
                    print(f"  진행: {processed:,}/{total_unclassified:,}  |  업데이트: {updated:,}", end="\r")

            if limit and processed >= limit:
                break

        conn.commit()
        if limit and processed >= limit:
            break

    conn.close()
    print(f"\n완료: {updated:,}개 분류  (Gemini 호출: {gemini_used})")

    # 분류 결과 요약
    conn2 = sqlite3.connect(DB_PATH)
    rows2 = conn2.execute(
        "SELECT sub_type_pred, COUNT(*) cnt FROM questions GROUP BY sub_type_pred ORDER BY cnt DESC"
    ).fetchall()
    conn2.close()

    print("\n=== 세부 유형 분포 ===")
    for r in rows2:
        print(f"  {(r[0] or '미분류'):<14} : {r[1]:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="기존 DB 객관식 문항 sub_type 분류")
    parser.add_argument("--gemini", action="store_true",
                        help="'기타'로 분류된 문항을 Gemini로 재분류")
    parser.add_argument("--force", action="store_true",
                        help="이미 분류된 문항도 전체 재분류")
    parser.add_argument("--api-key", default="", help="Gemini API 키 (없으면 config.py 값 사용)")
    parser.add_argument("--limit", type=int, default=None, help="처리할 최대 문항 수")
    parser.add_argument("--batch-size", type=int, default=500, help="배치 크기")
    args = parser.parse_args()

    # --gemini 또는 --force 시 '기타' 항목도 재분류 대상에 포함
    api_key = args.api_key or (GEMINI_API_KEY if args.gemini else "")

    if args.force:
        # 기존 분류 결과 초기화
        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE questions SET sub_type_pred=NULL WHERE question_type='객관식'")
        conn.commit()
        conn.close()
        print("기존 분류 결과 초기화 완료")

    if args.gemini and not args.force:
        # '기타'도 재분류 대상에 포함
        conn = sqlite3.connect(DB_PATH)
        cnt = conn.execute(
            "UPDATE questions SET sub_type_pred=NULL "
            "WHERE question_type='객관식' AND sub_type_pred='기타'"
        ).rowcount
        conn.commit()
        conn.close()
        print(f"'기타' {cnt}개를 재분류 대상으로 전환")

    classify_existing(api_key=api_key, limit=args.limit, batch_size=args.batch_size)
