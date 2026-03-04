"""
SQLite 데이터베이스 스키마 및 CRUD 모듈

테이블:
  - exams    : 파일/시험 단위 메타데이터
  - questions: 지문/문항 단위 구조 데이터 + 분류/품질 관리 데이터
"""

import sqlite3
import json
from pathlib import Path
from typing import Optional, List, Dict, Any

from pipeline.config import DB_PATH


# ─── Schema ────────────────────────────────────────────────────────────────────

CREATE_EXAMS_TABLE = """
CREATE TABLE IF NOT EXISTS exams (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name_raw    TEXT NOT NULL,
    source_type      TEXT NOT NULL,          -- '출판사' | '모의고사'
    exam_year        INTEGER,
    exam_month       INTEGER,                -- 모의고사용 (3,6,9,11,10...)
    grade            TEXT,                   -- '고1'
    term             TEXT,                   -- '1학기 중간' | '1학기 기말' | NULL(모의고사)
    school_name      TEXT,
    region           TEXT,
    subject          TEXT,
    textbook_label   TEXT,                   -- 'YBM(한상호)'
    publisher        TEXT,                   -- 'YBM'
    textbook_author  TEXT,                   -- '한상호'
    file_path        TEXT UNIQUE NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_QUESTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS questions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    exam_id                 INTEGER NOT NULL REFERENCES exams(id),

    -- 지문/단원 정보
    unit_no                 INTEGER,
    unit_label              TEXT,            -- '1과'
    section_type            TEXT,            -- '본문' | '문법' | '대화문' | NULL
    passage_text            TEXT,

    -- 문항 정보
    question_no             INTEGER,
    question_text           TEXT,
    choices                 TEXT,            -- JSON 배열 문자열
    question_type           TEXT,            -- '객관식' | '서술형'
    answer                  TEXT,            -- 정수(1~5) 또는 문자열

    -- 원본 / 정제 텍스트
    raw_text                TEXT,
    clean_text              TEXT,

    -- 해설
    explanation             TEXT,

    -- 분류 (sub_type)
    sub_type_pred           TEXT,
    sub_type_confidence     REAL,
    sub_type_reason         TEXT,
    sub_type_final          TEXT,           -- 검수 후 확정 (NULL 이면 pred 사용)

    -- 품질 관리
    needs_review            INTEGER DEFAULT 0,   -- 0=OK 1=요검수
    quality_check_status    TEXT DEFAULT 'pending',
    classifier_model        TEXT,
    classifier_version      TEXT,
    parser_version          TEXT,

    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_q_exam_id    ON questions(exam_id)",
    "CREATE INDEX IF NOT EXISTS idx_q_sub_type   ON questions(sub_type_pred)",
    "CREATE INDEX IF NOT EXISTS idx_q_needs_rev  ON questions(needs_review)",
    "CREATE INDEX IF NOT EXISTS idx_e_textbook   ON exams(textbook_label)",
    "CREATE INDEX IF NOT EXISTS idx_e_year       ON exams(exam_year)",
]


# ─── Textbooks (교과서 본문) 테이블 ─────────────────────────────────────────────

CREATE_TEXTBOOKS_TABLE = """
CREATE TABLE IF NOT EXISTS textbooks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    subject          TEXT NOT NULL,          -- '공통영어1' | '공통영어2'
    publisher        TEXT NOT NULL,          -- 'YBM'
    textbook_label   TEXT NOT NULL,          -- 'YBM(박준언)'
    textbook_author  TEXT,                   -- '박준언'
    unit_no          INTEGER NOT NULL,       -- 1, 2, 3, 4
    unit_title       TEXT,                   -- 'English or Englishes?'
    semester_exam    TEXT,                   -- '1학기 중간' | '1학기 기말' | '2학기 중간' | '2학기 기말'
    passage_text     TEXT NOT NULL,          -- 영어 본문 전체
    page_count       INTEGER,               -- PDF 페이지 수
    file_path        TEXT,                   -- 원본 PDF 경로
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(subject, textbook_label, unit_no)
)
"""

CREATE_TEXTBOOK_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tb_publisher  ON textbooks(publisher)",
    "CREATE INDEX IF NOT EXISTS idx_tb_subject    ON textbooks(subject)",
    "CREATE INDEX IF NOT EXISTS idx_tb_unit       ON textbooks(unit_no)",
    "CREATE INDEX IF NOT EXISTS idx_tb_semester   ON textbooks(semester_exam)",
]


# ─── Connection helper ─────────────────────────────────────────────────────────

def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path = DB_PATH):
    """DB 초기화: 테이블 및 인덱스 생성"""
    conn = get_connection(db_path)
    with conn:
        conn.execute(CREATE_EXAMS_TABLE)
        conn.execute(CREATE_QUESTIONS_TABLE)
        for idx_sql in CREATE_INDEXES:
            conn.execute(idx_sql)
        conn.execute(CREATE_TEXTBOOKS_TABLE)
        for idx_sql in CREATE_TEXTBOOK_INDEXES:
            conn.execute(idx_sql)
    conn.close()
    print(f"[DB] 초기화 완료: {db_path}")


# ─── Exam CRUD ─────────────────────────────────────────────────────────────────

def upsert_exam(conn: sqlite3.Connection, meta: Dict[str, Any]) -> int:
    """파일 경로를 기준으로 exam 레코드를 삽입 또는 업데이트. exam_id 반환."""
    existing = conn.execute(
        "SELECT id FROM exams WHERE file_path = ?", (meta["file_path"],)
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE exams SET
                file_name_raw=:file_name_raw, source_type=:source_type,
                exam_year=:exam_year, exam_month=:exam_month,
                grade=:grade, term=:term,
                school_name=:school_name, region=:region,
                subject=:subject, textbook_label=:textbook_label,
                publisher=:publisher, textbook_author=:textbook_author,
                updated_at=CURRENT_TIMESTAMP
            WHERE file_path=:file_path
        """, meta)
        return existing["id"]
    else:
        cur = conn.execute("""
            INSERT INTO exams (
                file_name_raw, source_type, exam_year, exam_month,
                grade, term, school_name, region, subject,
                textbook_label, publisher, textbook_author, file_path
            ) VALUES (
                :file_name_raw, :source_type, :exam_year, :exam_month,
                :grade, :term, :school_name, :region, :subject,
                :textbook_label, :publisher, :textbook_author, :file_path
            )
        """, meta)
        return cur.lastrowid


def insert_questions(conn: sqlite3.Connection, exam_id: int, questions: List[Dict[str, Any]]):
    """문항 목록을 DB에 삽입 (기존 삭제 후 재삽입)."""
    conn.execute("DELETE FROM questions WHERE exam_id = ?", (exam_id,))
    for q in questions:
        q["exam_id"] = exam_id
        if isinstance(q.get("choices"), list):
            q["choices"] = json.dumps(q["choices"], ensure_ascii=False)
        conn.execute("""
            INSERT INTO questions (
                exam_id, unit_no, unit_label, section_type, passage_text,
                question_no, question_text, choices, question_type, answer,
                raw_text, clean_text, explanation,
                sub_type_pred, sub_type_confidence, sub_type_reason, sub_type_final,
                needs_review, quality_check_status,
                classifier_model, classifier_version, parser_version
            ) VALUES (
                :exam_id, :unit_no, :unit_label, :section_type, :passage_text,
                :question_no, :question_text, :choices, :question_type, :answer,
                :raw_text, :clean_text, :explanation,
                :sub_type_pred, :sub_type_confidence, :sub_type_reason, :sub_type_final,
                :needs_review, :quality_check_status,
                :classifier_model, :classifier_version, :parser_version
            )
        """, q)


# ─── Search ────────────────────────────────────────────────────────────────────

def search_questions(
    conn: sqlite3.Connection,
    textbook_label: Optional[str] = None,
    publisher: Optional[str] = None,
    unit_no: Optional[int] = None,
    sub_type: Optional[str] = None,
    question_type: Optional[str] = None,
    exam_year: Optional[int] = None,
    source_type: Optional[str] = None,
    needs_review: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[Dict]:
    """다중 조건 검색. 조건이 None 이면 해당 필드 필터링 안 함."""
    clauses = []
    params = []

    if textbook_label:
        clauses.append("e.textbook_label LIKE ?")
        params.append(f"%{textbook_label}%")
    if publisher:
        clauses.append("e.publisher = ?")
        params.append(publisher)
    if unit_no is not None:
        clauses.append("q.unit_no = ?")
        params.append(unit_no)
    if sub_type:
        clauses.append("COALESCE(q.sub_type_final, q.sub_type_pred) = ?")
        params.append(sub_type)
    if question_type:
        clauses.append("q.question_type = ?")
        params.append(question_type)
    if exam_year:
        clauses.append("e.exam_year = ?")
        params.append(exam_year)
    if source_type:
        clauses.append("e.source_type = ?")
        params.append(source_type)
    if needs_review is not None:
        clauses.append("q.needs_review = ?")
        params.append(1 if needs_review else 0)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.extend([limit, offset])

    sql = f"""
        SELECT q.*, e.textbook_label, e.publisher, e.exam_year, e.school_name,
               e.term, e.source_type
        FROM questions q
        JOIN exams e ON q.exam_id = e.id
        {where}
        ORDER BY q.id
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(sql, params).fetchall()
    results = []
    for row in rows:
        d = dict(row)
        if d.get("choices"):
            try:
                d["choices"] = json.loads(d["choices"])
            except Exception:
                pass
        results.append(d)
    return results


# ─── Textbook CRUD ─────────────────────────────────────────────────────────────

def upsert_textbook(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """교과서 본문 레코드를 삽입 또는 업데이트. textbook_id 반환."""
    existing = conn.execute(
        "SELECT id FROM textbooks WHERE subject=? AND textbook_label=? AND unit_no=?",
        (data["subject"], data["textbook_label"], data["unit_no"]),
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE textbooks SET
                publisher=:publisher, textbook_author=:textbook_author,
                unit_title=:unit_title, semester_exam=:semester_exam,
                passage_text=:passage_text, page_count=:page_count,
                file_path=:file_path
            WHERE subject=:subject AND textbook_label=:textbook_label AND unit_no=:unit_no
        """, data)
        return existing["id"]
    else:
        cur = conn.execute("""
            INSERT INTO textbooks (
                subject, publisher, textbook_label, textbook_author,
                unit_no, unit_title, semester_exam,
                passage_text, page_count, file_path
            ) VALUES (
                :subject, :publisher, :textbook_label, :textbook_author,
                :unit_no, :unit_title, :semester_exam,
                :passage_text, :page_count, :file_path
            )
        """, data)
        return cur.lastrowid
