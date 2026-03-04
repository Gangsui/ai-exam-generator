#!/usr/bin/env python3
"""
새 데이터 추가 CLI — 모의고사 PDF / 교과서 PDF를 DB에 자동 추가

사용법:
  # 모의고사 추가 (입력_모의고사/ 폴더에 PDF 넣고 실행)
  python add_data.py --mock

  # 교과서 추가 (입력_교과서/ 폴더에 PDF 넣고 실행)
  python add_data.py --textbook

  # 둘 다 한번에
  python add_data.py --all

  # 분류 없이 빠르게 (나중에 classify_existing.py 로 분류)
  python add_data.py --mock --no-classify

폴더 규칙:
  입력_모의고사/
    - "2024년-고1-6월-모의고사-영어-문제.pdf" + "...해설.pdf" 쌍으로 넣기
    - 파일명에서 연도/월/학년 자동 추출
    - 처리 완료 후 → 모의고사문제/ 폴더로 자동 이동

  입력_교과서/
    - "공통영어1_NE능률(민병천)_1과.pdf" 형식으로 넣기
    - 처리 완료 후 → 해당 학교교과서본문_공통영어X/ 폴더로 자동 이동
"""

import argparse
import shutil
import sqlite3
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline.config import (
    DB_PATH, GEMINI_API_KEY, GEMINI_MODEL, PARSER_VERSION,
    SUBTYPE_CONFIDENCE_THRESHOLD, INPUT_MOCK_DIR, INPUT_TEXTBOOK_DIR,
    MOCK_DIR, TEXTBOOK_DIRS, RAG_DIR,
)
from pipeline.db import init_db, get_connection
from pipeline.parsers.filename_parser import parse_mock_filename
from pipeline.parsers.pdf_parser import (
    parse_problem_pdf,
    extract_answer_table_from_solution_pdf,
    validate_answer_table,
    apply_answer_table,
)
from pipeline.classifiers.subtype_classifier import classify_questions_batch
from pipeline.db import upsert_exam, insert_questions
from pipeline.review_queue import add_batch_to_review_queue, save_parse_failure


# ── 모의고사 추가 ──────────────────────────────────────────────────────────────

def add_mock_exams(classify: bool = True, api_key: str = GEMINI_API_KEY):
    """입력_모의고사/ 폴더의 PDF 쌍을 DB에 추가하고 모의고사문제/ 로 이동."""
    input_dir = INPUT_MOCK_DIR
    archive_dir = MOCK_DIR

    if not input_dir.exists():
        input_dir.mkdir(parents=True, exist_ok=True)
        print(f"  📁 입력 폴더 생성됨: {input_dir}")
        print(f"  → 모의고사 PDF를 이 폴더에 넣고 다시 실행하세요.")
        return

    # macOS NFD 문제: glob이 한글을 못 찾으므로 NFC 정규화 후 필터
    problem_pdfs = sorted([
        f for f in input_dir.iterdir()
        if f.suffix.lower() == ".pdf"
        and "문제" in unicodedata.normalize("NFC", f.name)
    ])
    if not problem_pdfs:
        print(f"  📁 입력_모의고사/ 폴더에 문제 PDF가 없습니다.")
        print(f"  → 파일명 예시: 2024년-고1-6월-모의고사-영어-문제.pdf")
        return

    init_db(DB_PATH)
    conn = get_connection(DB_PATH)
    archive_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    for prob_pdf in problem_pdfs:
        prob_name_nfc = unicodedata.normalize("NFC", prob_pdf.name)
        # 매칭 해설/정답 PDF — 여러 이름 패턴 시도
        sol_pdf = None
        for replacement in ["해설", "정답"]:
            # NFC 변환 후 치환해서 후보 생성하고, 실제 파일 중 NFC 매칭
            target_nfc = prob_name_nfc.replace("문제", replacement)
            for f in input_dir.iterdir():
                if unicodedata.normalize("NFC", f.name) == target_nfc:
                    sol_pdf = f
                    break
            if sol_pdf:
                break
        if sol_pdf is None:
            # 같은 연도/학년 키워드를 포함하는 해설/정답 파일 탐색
            prob_stem_nfc = unicodedata.normalize("NFC", prob_pdf.stem)
            for f in input_dir.iterdir():
                if f == prob_pdf or not f.suffix.lower() == ".pdf":
                    continue
                f_name_nfc = unicodedata.normalize("NFC", f.name)
                if ("해설" in f_name_nfc or "정답" in f_name_nfc) and "문제" not in f_name_nfc:
                    # 연도가 같은지 확인
                    import re as _re
                    prob_year = _re.search(r"\d{4}", prob_stem_nfc)
                    f_year = _re.search(r"\d{4}", f.stem)
                    if prob_year and f_year and prob_year.group() == f_year.group():
                        # 학년(고1 등)도 확인
                        prob_grade = _re.search(r"고\d", prob_stem_nfc)
                        f_stem_nfc = unicodedata.normalize("NFC", f.stem)
                        f_grade = _re.search(r"고\d", f_stem_nfc)
                        if prob_grade and f_grade and prob_grade.group() == f_grade.group():
                            # 월도 확인 (있으면)
                            prob_month = _re.search(r"(\d{1,2})월", prob_stem_nfc)
                            f_month = _re.search(r"(\d{1,2})월", f_stem_nfc)
                            if prob_month and f_month:
                                if prob_month.group(1) == f_month.group(1):
                                    sol_pdf = f
                                    break
                            else:
                                sol_pdf = f
                                break

        if sol_pdf is None:
            print(f"  ⚠️  해설/정답 PDF 없음: {prob_pdf.name} (건너뜀)")
            continue

        print(f"\n  📄 처리 중: {prob_pdf.name}")

        # 1. 파일명 메타데이터
        meta = parse_mock_filename(prob_pdf.name)
        if not meta:
            print(f"  ❌ 파일명 파싱 실패. 예시: 2024년-고1-6월-모의고사-영어-문제.pdf")
            save_parse_failure(str(prob_pdf), "모의고사 파일명 파싱 실패")
            continue

        meta["file_name_raw"] = prob_pdf.name
        meta["file_path"] = str(prob_pdf.resolve())
        meta.pop("doc_type", None)

        # 2. 문제지 PDF 파싱
        try:
            questions = parse_problem_pdf(str(prob_pdf), parser_version=PARSER_VERSION)
        except Exception as exc:
            print(f"  ❌ PDF 파싱 오류: {exc}")
            save_parse_failure(str(prob_pdf), str(exc))
            continue

        # 3. 해설 정답표 추출
        try:
            answer_table = extract_answer_table_from_solution_pdf(
                str(sol_pdf), api_key=api_key, model_name=GEMINI_MODEL
            )
            validation = validate_answer_table(answer_table)
            if not validation["valid"]:
                print(f"  ⚠️  정답표 검증 실패: {validation}")
            questions = apply_answer_table(questions, answer_table)
        except Exception as exc:
            print(f"  ⚠️  정답표 추출 실패: {exc}")

        if not questions:
            print(f"  ⚠️  문항 없음 (건너뜀)")
            continue

        # 4. 세부 유형 분류
        if classify:
            questions = classify_questions_batch(
                questions,
                api_key=api_key,
                model_name=GEMINI_MODEL,
                confidence_threshold=SUBTYPE_CONFIDENCE_THRESHOLD,
            )

        # 5. DB 저장
        with conn:
            exam_id = upsert_exam(conn, meta)
            insert_questions(conn, exam_id, questions)

        # 6. 검수 큐
        review_items = [q for q in questions if q.get("needs_review")]
        if review_items:
            for item in review_items:
                item["exam_file"] = prob_pdf.name
                item["exam_id"] = exam_id
            add_batch_to_review_queue(review_items)

        print(f"  ✅ {len(questions)}문항 추가 (검수 {len(review_items)}개)")
        total += len(questions)

        # 7. 처리 완료 → 모의고사문제/ 로 이동
        shutil.move(str(prob_pdf), str(archive_dir / prob_pdf.name))
        shutil.move(str(sol_pdf), str(archive_dir / sol_pdf.name))
        print(f"  → 파일 이동 완료: 모의고사문제/")

    conn.close()
    print(f"\n  📊 모의고사 추가 완료: 총 {total}문항")


# ── 교과서 추가 ────────────────────────────────────────────────────────────────

def add_textbooks(api_key: str = GEMINI_API_KEY):
    """입력_교과서/ 폴더의 PDF를 DB에 추가하고 해당 교과서 폴더로 이동."""
    input_dir = INPUT_TEXTBOOK_DIR

    if not input_dir.exists():
        input_dir.mkdir(parents=True, exist_ok=True)
        print(f"  📁 입력 폴더 생성됨: {input_dir}")
        print(f"  → 교과서 PDF를 이 폴더에 넣고 다시 실행하세요.")
        return

    pdf_files = sorted(input_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"  📁 입력_교과서/ 폴더에 PDF가 없습니다.")
        print(f"  → 파일명 예시: 공통영어1_NE능률(민병천)_1과.pdf")
        return

    # 교과서 파서 import
    try:
        from pipeline.parsers.textbook_parser import parse_textbook_pdf
    except ImportError:
        print("  ❌ textbook_parser.py를 찾을 수 없습니다.")
        return

    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    total = 0
    for pdf_path in pdf_files:
        print(f"\n  📄 처리 중: {pdf_path.name}")

        # 파일명에서 메타 추출: 공통영어1_NE능률(민병천)_1과.pdf
        stem = pdf_path.stem
        parts = stem.split("_")

        if len(parts) < 3:
            print(f"  ❌ 파일명 형식 오류. 예시: 공통영어1_NE능률(민병천)_1과.pdf")
            continue

        subject = parts[0]       # 공통영어1 or 공통영어2
        textbook_label = parts[1]  # NE능률(민병천)
        unit_str = parts[2]      # 1과

        # 단원 번호 추출
        import re
        unit_match = re.search(r'(\d+)', unit_str)
        if not unit_match:
            print(f"  ❌ 단원 번호 추출 실패: {unit_str}")
            continue
        unit_no = int(unit_match.group(1))

        # 출판사 추출
        pub_match = re.match(r'^([^(]+)', textbook_label)
        publisher = pub_match.group(1) if pub_match else textbook_label

        # 시험기간 매핑
        if subject == "공통영어1":
            semester_exam = "1학기 중간" if unit_no <= 2 else "1학기 기말"
        elif subject == "공통영어2":
            semester_exam = "2학기 중간" if unit_no <= 2 else "2학기 기말"
        else:
            semester_exam = None

        # PDF 파싱
        try:
            result = parse_textbook_pdf(str(pdf_path))
            passage_text = result.get("passage_text", "")
            unit_title = result.get("unit_title", "")
        except Exception as exc:
            print(f"  ❌ PDF 파싱 오류: {exc}")
            continue

        if not passage_text or len(passage_text.strip()) < 50:
            print(f"  ⚠️  본문이 너무 짧음 ({len(passage_text.strip())}자)")
            continue

        # DB upsert
        with conn:
            conn.execute("""
                INSERT INTO textbooks (subject, publisher, textbook_label, unit_no,
                                       unit_title, semester_exam, passage_text)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subject, textbook_label, unit_no)
                DO UPDATE SET passage_text=excluded.passage_text,
                              unit_title=excluded.unit_title,
                              semester_exam=excluded.semester_exam,
                              publisher=excluded.publisher
            """, (subject, publisher, textbook_label, unit_no,
                  unit_title, semester_exam, passage_text))

        print(f"  ✅ {subject} / {textbook_label} / {unit_no}과 추가 ({len(passage_text)}자)")
        total += 1

        # 처리 완료 → 해당 교과서 폴더로 이동
        target_dir = TEXTBOOK_DIRS.get(subject)
        if target_dir:
            target_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(pdf_path), str(target_dir / pdf_path.name))
            print(f"  → 파일 이동 완료: {target_dir.name}/")
        else:
            # 과목이 공통영어1/2가 아닌 경우
            fallback = RAG_DIR / f"학교교과서본문_{subject}"
            fallback.mkdir(parents=True, exist_ok=True)
            shutil.move(str(pdf_path), str(fallback / pdf_path.name))
            print(f"  → 파일 이동 완료: {fallback.name}/")

    conn.close()
    print(f"\n  📊 교과서 추가 완료: 총 {total}개")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="새 데이터 추가 — 모의고사 PDF / 교과서 PDF → DB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python add_data.py --mock                    # 모의고사 추가
  python add_data.py --textbook                # 교과서 추가
  python add_data.py --all                     # 둘 다
  python add_data.py --mock --no-classify      # 분류 없이 빠르게

폴더에 파일 넣기:
  입력_모의고사/    ← 문제.pdf + 해설.pdf 쌍
  입력_교과서/      ← 공통영어1_출판사(저자)_N과.pdf
        """
    )

    parser.add_argument("--mock", action="store_true", help="모의고사 PDF 추가")
    parser.add_argument("--textbook", action="store_true", help="교과서 PDF 추가")
    parser.add_argument("--all", action="store_true", help="모의고사 + 교과서 모두")
    parser.add_argument("--no-classify", action="store_true",
                        help="세부 유형 분류 안 함 (나중에 classify_existing.py)")
    parser.add_argument("--api-key", default=GEMINI_API_KEY, help="Gemini API 키")

    args = parser.parse_args()

    if not any([args.mock, args.textbook, args.all]):
        parser.print_help()
        print("\n  ⚠️  --mock, --textbook, --all 중 하나를 선택하세요.")
        return

    classify = not args.no_classify

    if args.mock or args.all:
        print("\n" + "=" * 50)
        print("  📥 모의고사 추가")
        print("=" * 50)
        add_mock_exams(classify=classify, api_key=args.api_key)

    if args.textbook or args.all:
        print("\n" + "=" * 50)
        print("  📥 교과서 추가")
        print("=" * 50)
        add_textbooks(api_key=args.api_key)


if __name__ == "__main__":
    main()
