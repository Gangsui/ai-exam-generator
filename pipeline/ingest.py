"""
메인 수집/전처리/저장 파이프라인

처리 흐름:
  출판사 .txt → 파일명 파싱 → 본문/문항 파싱 → sub_type 분류 → DB 저장 → 검수 큐
  모의고사 PDF  → 파일명 파싱 → 문제/해설 PDF 매칭 → 파싱 → 정답 매핑 → DB 저장
"""

import sys
import traceback
from pathlib import Path
from typing import Optional, List, Tuple
import sqlite3

from pipeline.config import SCHOOL_DIR, MOCK_DIR, DB_PATH, GEMINI_API_KEY, GEMINI_MODEL, PARSER_VERSION, SUBTYPE_CONFIDENCE_THRESHOLD
from pipeline.db import get_connection, init_db, upsert_exam, insert_questions
from pipeline.parsers.filename_parser import parse_school_filename, parse_mock_filename
from pipeline.parsers.txt_parser import parse_txt_file
from pipeline.parsers.pdf_parser import (
    parse_problem_pdf,
    extract_answer_table_from_solution_pdf,
    validate_answer_table,
    apply_answer_table,
)
from pipeline.classifiers.subtype_classifier import classify_questions_batch
from pipeline.review_queue import add_batch_to_review_queue, save_parse_failure


# ─── 출판사 .txt 처리 ──────────────────────────────────────────────────────────

def ingest_school_file(
    txt_path: str,
    conn: sqlite3.Connection,
    classify: bool = True,
    api_key: str = GEMINI_API_KEY,
) -> Tuple[int, int]:
    """
    단일 출판사 .txt 파일 수집.
    반환: (저장된 문항 수, 검수 큐 추가 수)
    """
    p = Path(txt_path)

    # 1. 파일명 메타데이터 추출
    meta = parse_school_filename(p.name)
    if not meta:
        save_parse_failure(txt_path, "파일명 파싱 실패")
        return 0, 0

    # 부교재 / 모의고사 파일은 교재 문항이 아니므로 건너뜀
    _SKIP_LABELS = {"부교재", "모의고사"}
    if meta.get("publisher") in _SKIP_LABELS or meta.get("textbook_label") in _SKIP_LABELS:
        return 0, 0

    meta["file_name_raw"] = p.name
    meta["file_path"] = str(p.resolve())

    # 2. 본문/문항 파싱
    try:
        questions = parse_txt_file(txt_path, parser_version=PARSER_VERSION)
    except Exception as exc:
        save_parse_failure(txt_path, f"텍스트 파싱 오류: {exc}")
        return 0, 0

    if not questions:
        save_parse_failure(txt_path, "문항 추출 결과 없음")
        return 0, 0

    # 3. sub_type 분류
    if classify:
        questions = classify_questions_batch(
            questions,
            api_key=api_key,
            model_name=GEMINI_MODEL,
            confidence_threshold=SUBTYPE_CONFIDENCE_THRESHOLD,
        )

    # 4. DB 저장
    exam_id = upsert_exam(conn, meta)
    insert_questions(conn, exam_id, questions)

    # 5. 검수 큐
    review_items = [q for q in questions if q.get("needs_review")]
    if review_items:
        for item in review_items:
            item["exam_file"] = str(p.name)
            item["exam_id"] = exam_id
        add_batch_to_review_queue(review_items)

    return len(questions), len(review_items)


def ingest_school_directory(
    school_dir: Path = SCHOOL_DIR,
    grade: str = "1학년",
    year_filter: Optional[str] = None,
    classify: bool = True,
    max_files: Optional[int] = None,
    api_key: str = GEMINI_API_KEY,
    verbose: bool = True,
):
    """
    학교문제 디렉터리 전체(또는 특정 연도 폴더)를 순회하여 수집.

    Args:
        school_dir  : 학교문제 루트 경로
        grade       : '1학년' | '2학년' 등
        year_filter : '2023-09' 처럼 특정 폴더만 처리 (None 이면 전체)
        classify    : sub_type 분류 여부
        max_files   : 최대 처리 파일 수 (테스트용)
        api_key     : Gemini API 키
        verbose     : 진행상황 출력 여부
    """
    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    grade_dir = school_dir / grade

    # grade 서브폴더가 없으면 school_dir 자체를 flat 폴더로 사용
    if not grade_dir.exists():
        if list(school_dir.glob("*.txt")):
            print(f"[Ingest] flat 폴더로 처리: {school_dir}")
            grade_dir = school_dir
        else:
            print(f"[Ingest] 경로를 찾을 수 없음: {grade_dir}")
            return

    # flat 구조: grade_dir 안에 바로 .txt 파일이 있는 경우
    direct_txts = list(grade_dir.glob("*.txt"))
    if direct_txts:
        print(f"[Ingest] flat 폴더 감지: {len(direct_txts)}개 파일 직접 처리")
        year_dirs = [grade_dir]   # 단일 "가상 연도" 폴더로 처리
    # year_filter 적용
    elif year_filter:
        year_dirs = [grade_dir / year_filter]
    else:
        year_dirs = sorted(grade_dir.iterdir())

    total_files = 0
    total_questions = 0
    total_review = 0
    errors = 0

    for year_dir in year_dirs:
        if not year_dir.is_dir():
            continue
        txt_files = sorted(year_dir.glob("*.txt"))

        for txt_path in txt_files:
            if max_files and total_files >= max_files:
                break
            try:
                with conn:
                    n_q, n_r = ingest_school_file(
                        str(txt_path), conn, classify=classify, api_key=api_key
                    )
                total_questions += n_q
                total_review += n_r
                total_files += 1
                if verbose:
                    print(f"  [OK] {txt_path.name}  → {n_q}문항 (검수 {n_r}개)")
            except Exception as exc:
                errors += 1
                save_parse_failure(str(txt_path), str(exc))
                if verbose:
                    print(f"  [ERR] {txt_path.name}: {exc}")

        if max_files and total_files >= max_files:
            break

    conn.close()
    print(f"\n[Ingest 완료] 파일 {total_files}개 | 문항 {total_questions}개 | 검수 {total_review}개 | 오류 {errors}개")


# ─── 모의고사 PDF 처리 ─────────────────────────────────────────────────────────

def ingest_mock_exam(
    problem_pdf: str,
    solution_pdf: str,
    conn: sqlite3.Connection,
    classify: bool = True,
    api_key: str = GEMINI_API_KEY,
) -> Tuple[int, int]:
    """
    모의고사 문제지 PDF + 해설 PDF 쌍을 처리.
    반환: (저장된 문항 수, 검수 큐 추가 수)
    """
    p = Path(problem_pdf)

    # 1. 파일명 메타데이터
    meta = parse_mock_filename(p.name)
    if not meta:
        save_parse_failure(problem_pdf, "모의고사 파일명 파싱 실패")
        return 0, 0

    meta["file_name_raw"] = p.name
    meta["file_path"] = str(p.resolve())
    meta.pop("doc_type", None)  # 내부 키 제거

    # 2. 문제지 파싱
    try:
        questions = parse_problem_pdf(problem_pdf, parser_version=PARSER_VERSION)
    except Exception as exc:
        save_parse_failure(problem_pdf, f"문제지 PDF 파싱 오류: {exc}")
        return 0, 0

    # 3. 해설 정답표 추출
    try:
        answer_table = extract_answer_table_from_solution_pdf(
            solution_pdf, api_key=api_key
        )
        validation = validate_answer_table(answer_table)
        if not validation["valid"]:
            print(f"  [경고] 정답표 검증 실패: {validation}")
        questions = apply_answer_table(questions, answer_table)
    except Exception as exc:
        print(f"  [경고] 정답표 추출 실패: {exc}")

    if not questions:
        return 0, 0

    # 4. sub_type 분류
    if classify:
        questions = classify_questions_batch(
            questions,
            api_key=api_key,
            model_name=GEMINI_MODEL,
            confidence_threshold=SUBTYPE_CONFIDENCE_THRESHOLD,
        )

    # 5. DB 저장
    exam_id = upsert_exam(conn, meta)
    insert_questions(conn, exam_id, questions)

    review_items = [q for q in questions if q.get("needs_review")]
    if review_items:
        for item in review_items:
            item["exam_file"] = p.name
            item["exam_id"] = exam_id
        add_batch_to_review_queue(review_items)

    return len(questions), len(review_items)


def ingest_mock_directory(
    mock_dir: Path = MOCK_DIR,
    classify: bool = True,
    api_key: str = GEMINI_API_KEY,
    verbose: bool = True,
):
    """모의고사 PDF 디렉터리에서 문제/해설 PDF 쌍을 찾아 일괄 처리."""
    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    # 문제 PDF 찾기
    problem_pdfs = sorted(mock_dir.glob("*문제*.pdf"))
    total_files = 0
    total_q = 0

    for prob_pdf in problem_pdfs:
        # 매칭 해설 PDF 찾기 (같은 이름, '문제' → '해설' 치환)
        sol_name = prob_pdf.name.replace("문제", "해설")
        sol_pdf = mock_dir / sol_name
        if not sol_pdf.exists():
            print(f"  [스킵] 해설 PDF 없음: {sol_name}")
            continue

        try:
            with conn:
                n_q, n_r = ingest_mock_exam(
                    str(prob_pdf), str(sol_pdf), conn, classify=classify, api_key=api_key
                )
            total_q += n_q
            total_files += 1
            if verbose:
                print(f"  [OK] {prob_pdf.name}  → {n_q}문항 (검수 {n_r}개)")
        except Exception as exc:
            save_parse_failure(str(prob_pdf), str(exc))
            if verbose:
                print(f"  [ERR] {prob_pdf.name}: {exc}")

    conn.close()
    print(f"\n[모의고사 Ingest 완료] PDF 쌍 {total_files}개 | 문항 {total_q}개")
