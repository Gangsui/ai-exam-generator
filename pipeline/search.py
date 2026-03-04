"""
문제 검색 모듈

DB에서 조건에 맞는 문제를 검색하고 다양한 필터링을 제공한다.
"""

import json
import random
from typing import List, Dict, Any, Optional

from pipeline.db import get_connection, search_questions
from pipeline.config import DB_PATH


def find_questions(
    textbook_label: Optional[str] = None,
    publisher: Optional[str] = None,
    unit_no: Optional[int] = None,
    sub_type: Optional[str] = None,
    sub_types: Optional[List[str]] = None,   # 여러 유형 동시 지정
    question_type: Optional[str] = None,
    exam_year: Optional[int] = None,
    source_type: Optional[str] = None,
    exclude_review: bool = True,
    limit: int = 100,
    randomize: bool = False,
    seed: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    조건에 맞는 문항 검색.

    Args:
        textbook_label: 교재 레이블 (부분 일치)
        publisher     : 출판사 (완전 일치)
        unit_no       : 단원 번호
        sub_type      : 단일 세부 유형
        sub_types     : 여러 세부 유형 (OR 조건)
        question_type : '객관식' | '서술형'
        exam_year     : 출제 연도
        source_type   : '출판사' | '모의고사'
        exclude_review: True 이면 needs_review=1 인 항목 제외
        limit         : 최대 반환 개수
        randomize     : 랜덤 샘플링
        seed          : 랜덤 시드

    Returns:
        문항 dict 리스트
    """
    conn = get_connection(DB_PATH)

    # 여러 sub_type 지원
    if sub_types and not sub_type:
        results = []
        for st in sub_types:
            r = search_questions(
                conn,
                textbook_label=textbook_label,
                publisher=publisher,
                unit_no=unit_no,
                sub_type=st,
                question_type=question_type,
                exam_year=exam_year,
                source_type=source_type,
                needs_review=False if exclude_review else None,
                limit=limit,
            )
            results.extend(r)
        # 중복 제거 (id 기준)
        seen = set()
        deduped = []
        for item in results:
            if item["id"] not in seen:
                seen.add(item["id"])
                deduped.append(item)
        results = deduped
    else:
        results = search_questions(
            conn,
            textbook_label=textbook_label,
            publisher=publisher,
            unit_no=unit_no,
            sub_type=sub_type,
            question_type=question_type,
            exam_year=exam_year,
            source_type=source_type,
            needs_review=False if exclude_review else None,
            limit=limit,
        )

    conn.close()

    if randomize:
        rng = random.Random(seed)
        rng.shuffle(results)

    return results[:limit]


def find_questions_by_plan(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    문제집 구성 계획(plan dict)에 따라 문항을 검색.

    plan 예시:
    {
        "textbook_label": "YBM(한상호)",
        "unit_no": 1,
        "sub_types": [{"type": "어법", "count": 3}, {"type": "주제", "count": 2}],
        "question_type": "객관식",
        "randomize": true,
        "seed": 42
    }
    """
    all_qs: List[Dict[str, Any]] = []
    for spec in plan.get("sub_types", []):
        qs = find_questions(
            textbook_label=plan.get("textbook_label"),
            publisher=plan.get("publisher"),
            unit_no=plan.get("unit_no"),
            sub_type=spec["type"],
            question_type=plan.get("question_type"),
            exam_year=plan.get("exam_year"),
            source_type=plan.get("source_type"),
            exclude_review=plan.get("exclude_review", True),
            limit=spec.get("count", 10) * 5,  # 풀에서 많이 뽑아 랜덤 선택
            randomize=plan.get("randomize", False),
            seed=plan.get("seed"),
        )
        all_qs.extend(qs[: spec.get("count", 10)])

    return all_qs


def get_stats(source_type: Optional[str] = None) -> Dict[str, Any]:
    """DB 통계 조회."""
    conn = get_connection(DB_PATH)
    params = []
    where = ""
    if source_type:
        where = "WHERE e.source_type = ?"
        params.append(source_type)

    total = conn.execute(
        f"SELECT COUNT(*) FROM questions q JOIN exams e ON q.exam_id=e.id {where}", params
    ).fetchone()[0]
    needs_review = conn.execute(
        f"SELECT COUNT(*) FROM questions q JOIN exams e ON q.exam_id=e.id {where} {'AND' if where else 'WHERE'} q.needs_review=1",
        params if where else [],
    ).fetchone()[0]

    sub_type_counts = conn.execute(
        f"""SELECT COALESCE(q.sub_type_final, q.sub_type_pred,'미분류') AS st, COUNT(*) AS cnt
            FROM questions q JOIN exams e ON q.exam_id=e.id {where}
            GROUP BY st ORDER BY cnt DESC""",
        params,
    ).fetchall()

    conn.close()
    return {
        "total_questions": total,
        "needs_review": needs_review,
        "sub_type_distribution": {row[0]: row[1] for row in sub_type_counts},
    }
