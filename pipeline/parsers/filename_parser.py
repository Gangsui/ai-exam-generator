"""
파일명 메타데이터 파서

출판사 파일명 예시:

모의고사 파일명 예시:
  2025년-고1-10월-모의고사-영어-문제.pdf
  2025년-고1-10월-모의고사-영어-해설.pdf
"""

import re
import unicodedata
from pathlib import Path
from typing import Dict, Any, Optional


def _nfc(s: str) -> str:
    """macOS NFD 파일명을 NFC로 정규화."""
    return unicodedata.normalize('NFC', s)


# ① ~ ⑤  →  정수 변환 보조 딕셔너리
CIRCLE_TO_INT = {"①": 1, "②": 2, "③": 3, "④": 4, "⑤": 5}


# ─── 출판사 파일명 파서 ─────────────────────────────────────────────────────────

# (개정)YYYY년_고N_학기 시험종류_학교명_지역_영어_교재레이블(저자).ext
_SCHOOL_PATTERN = re.compile(
    r"(?:\(개정\))?"
    r"(?P<year>\d{4})년_"
    r"(?P<grade>고\d)_"
    r"(?P<term>[^_]+)_"          # 예: '1학기 중간', '2학기 기말'
    r"(?P<school>[^_]+)_"
    r"(?P<region>[^_]+)_"
    r"(?:(?P<subject>[^_]+)_)?"  # 예: '영어' — 일부 파일은 과목 필드 없음
    r"(?P<textbook_label>.+)"    # 예: YBM(한상호), NE능률(김성곤), 부교재
)

# '출판사(저자)' 분리 — 후행 마침표·공백·이중괄호(음악 등) 허용
_TEXTBOOK_DETAIL = re.compile(r"^(?P<publisher>[^(]+)\((?P<author>[^)]+)\).*$")


def parse_school_filename(filename: str) -> Dict[str, Any]:
    """
    출판사 .txt 파일명에서 메타데이터 추출.
    매칭 실패 시 빈 dict 반환.
    """
    stem = _nfc(Path(filename).stem)  # 확장자 제거 + NFC 정규화
    m = _SCHOOL_PATTERN.match(stem)
    if not m:
        return {}

    textbook_label = m.group("textbook_label").strip()
    publisher, author = _split_textbook_label(textbook_label)

    return {
        "source_type": "출판사",
        "exam_year": int(m.group("year")),
        "exam_month": None,
        "grade": m.group("grade"),
        "term": m.group("term").strip(),
        "school_name": m.group("school").strip(),
        "region": m.group("region").strip(),
        "subject": m.group("subject").strip() if m.group("subject") else "영어",
        "textbook_label": textbook_label,
        "publisher": publisher,
        "textbook_author": author,
    }


def _split_textbook_label(label: str):
    """'YBM(한상호)' → ('YBM', '한상호').  괄호 없으면 (label, None)."""
    m = _TEXTBOOK_DETAIL.match(label.strip())
    if m:
        return m.group("publisher").strip(), m.group("author").strip()
    return label.strip(), None


# ─── 모의고사 파일명 파서 ───────────────────────────────────────────────────────

# 지원 파일명 패턴 (구분자: - 또는 _):
#   2025년-고1-10월-모의고사-영어-문제.pdf    (연도-학년-월-기타-과목-문서)
#   2024년-6월-고1-모의고사-영어-문제.pdf     (연도-월-학년-기타-과목-문서)
#   2025년_3월_고1_영어_문제.pdf              (모의고사 단어 없이 4토큰)
#   2025년_6월_고1_모의고사_영어_문제.pdf     (_ 구분자)

_SEP = r"[-_]"  # 하이픈 또는 언더스코어

# 패턴 A: 연도-학년-월  (6토큰: 연-학-월-종류-과목-문서)
_MOCK_PATTERN_A = re.compile(
    r"(?P<year>\d{4})년" + _SEP +
    r"(?P<grade>고\d)" + _SEP +
    r"(?P<month>\d{1,2})월" + _SEP +
    r"[^-_]+" + _SEP +          # 모의고사 / 학력평가 등
    r"[^-_]+" + _SEP +          # 영어 / 국어 등
    r"(?P<doc_type>문제|해설|정답)",
    re.IGNORECASE,
)

# 패턴 B: 연도-월-학년  (6토큰)
_MOCK_PATTERN_B = re.compile(
    r"(?P<year>\d{4})년" + _SEP +
    r"(?P<month>\d{1,2})월" + _SEP +
    r"(?P<grade>고\d)" + _SEP +
    r"[^-_]+" + _SEP +          # 모의고사 / 학력평가 등
    r"[^-_]+" + _SEP +          # 영어 / 국어 등
    r"(?P<doc_type>문제|해설|정답)",
    re.IGNORECASE,
)

# 패턴 C: 연도-학년-월-과목-문서  (모의고사 단어 생략, 5토큰)
_MOCK_PATTERN_C = re.compile(
    r"(?P<year>\d{4})년" + _SEP +
    r"(?P<grade>고\d)" + _SEP +
    r"(?P<month>\d{1,2})월" + _SEP +
    r"[^-_]+" + _SEP +          # 영어 등 (과목만)
    r"(?P<doc_type>문제|해설|정답)",
    re.IGNORECASE,
)

# 패턴 D: 연도-월-학년-과목-문서  (모의고사 단어 생략, 5토큰)
_MOCK_PATTERN_D = re.compile(
    r"(?P<year>\d{4})년" + _SEP +
    r"(?P<month>\d{1,2})월" + _SEP +
    r"(?P<grade>고\d)" + _SEP +
    r"[^-_]+" + _SEP +          # 영어 등
    r"(?P<doc_type>문제|해설|정답)",
    re.IGNORECASE,
)

# 패턴 E: 연도-학년-과목-문서  (월 없이 4토큰 — 월은 0 처리)
_MOCK_PATTERN_E = re.compile(
    r"(?P<year>\d{4})년" + _SEP +
    r"(?P<grade>고\d)" + _SEP +
    r"[^-_]+" + _SEP +          # 영어 등
    r"(?P<doc_type>문제|해설|정답)",
    re.IGNORECASE,
)


def parse_mock_filename(filename: str) -> Dict[str, Any]:
    """모의고사 PDF 파일명에서 메타데이터 추출."""
    stem = _nfc(Path(filename).stem)  # NFC 정규화

    m = None
    for pat in (_MOCK_PATTERN_A, _MOCK_PATTERN_B,
                _MOCK_PATTERN_C, _MOCK_PATTERN_D, _MOCK_PATTERN_E):
        m = pat.search(stem)
        if m:
            break
    if not m:
        return {}

    return {
        "source_type": "모의고사",
        "exam_year": int(m.group("year")),
        "exam_month": int(m.group("month")),
        "grade": m.group("grade"),
        "term": None,
        "school_name": None,
        "region": None,
        "subject": "영어",
        "textbook_label": None,
        "publisher": None,
        "textbook_author": None,
        "doc_type": m.group("doc_type"),  # '문제' or '해설'
    }


# ─── 공통 헬퍼 ─────────────────────────────────────────────────────────────────

def parse_filename(file_path: str) -> Dict[str, Any]:
    """파일 경로를 받아 출판사/모의고사 자동 판별 후 메타데이터 반환."""
    p = Path(file_path)
    name = p.name
    if p.suffix.lower() == ".txt":
        return parse_school_filename(name)
    elif p.suffix.lower() == ".pdf":
        return parse_mock_filename(name)
    return {}
