"""
교과서 본문 PDF에서 영어 텍스트를 추출하는 파서.

PDF 레이아웃: 좌측 = 영어 원문, 우측 = 한국어 번역
→ 좌측 62% 영역만 crop 하여 영어 본문 추출
"""

import re
from pathlib import Path
from typing import Optional, Dict, Any

import pdfplumber


# ─── 파일명 파싱 ────────────────────────────────────────────────────────────────

_FILENAME_RE = re.compile(
    r"\((?:2022)?개정\)"
    r"(\d{4})년"                           # (1) 연도
    r"_(공통영어[12])"                      # (2) 과목
    r"_([^_]+)\(([^)]+)\)"                 # (3) 출판사, (4) 저자
    r"_(\d+)과_본문"                        # (5) 과번호
    r"(?:_\([^)]*\))?"                     # 선택적 수정일
    r"\.pdf$"
)

_FILENAME_SPECIAL_RE = re.compile(
    r"\((?:2022)?개정\)"
    r"(\d{4})년"
    r"_(공통영어[12])"
    r"_([^_]+)\(([^)]+)\)"
    r"_(Special\s*Lesson)_본문"
    r"(?:_\([^)]*\))?"
    r"\.pdf$"
)

# 과번호 → 시험 기간 매핑
UNIT_TO_EXAM = {
    "공통영어1": {1: "1학기 중간", 2: "1학기 중간", 3: "1학기 기말", 4: "1학기 기말"},
    "공통영어2": {1: "2학기 중간", 2: "2학기 중간", 3: "2학기 기말", 4: "2학기 기말"},
}


def parse_textbook_filename(filename: str) -> Optional[Dict[str, Any]]:
    """
    교과서 본문 PDF 파일명에서 메타데이터 추출.
    
    반환: {subject, publisher, textbook_label, textbook_author, unit_no, semester_exam}
    """
    name = Path(filename).name
    
    m = _FILENAME_RE.search(name)
    if m:
        subject = m.group(2)
        publisher = m.group(3)
        author = m.group(4)
        unit_no = int(m.group(5))
        semester_exam = UNIT_TO_EXAM.get(subject, {}).get(unit_no)
        return {
            "subject": subject,
            "publisher": publisher,
            "textbook_label": f"{publisher}({author})",
            "textbook_author": author,
            "unit_no": unit_no,
            "semester_exam": semester_exam,
        }
    
    # Special Lesson
    m = _FILENAME_SPECIAL_RE.search(name)
    if m:
        subject = m.group(2)
        publisher = m.group(3)
        author = m.group(4)
        return {
            "subject": subject,
            "publisher": publisher,
            "textbook_label": f"{publisher}({author})",
            "textbook_author": author,
            "unit_no": 0,  # Special Lesson
            "semester_exam": None,
        }
    
    return None


# ─── 헤더/푸터 필터 ─────────────────────────────────────────────────────────────

_HEADER_RE = re.compile(
    r"^(2022\s*개정|교과서\s*본문|공통영어|YBM|NE|천재|비상|미래|동아|지학)"
)
_PAGE_NUM_RE = re.compile(r"^-?\s*\d+\s*-?$")
_SOLO_DIGIT_RE = re.compile(r"^\d{1,2}$")


def _clean_page_text(text: str) -> str:
    """헤더·푸터·페이지 번호 등 불필요한 줄 제거"""
    lines = text.split("\n")
    clean = []
    for line in lines:
        s = line.strip()
        if not s:
            continue
        if _HEADER_RE.match(s):
            continue
        if _PAGE_NUM_RE.match(s):
            continue
        if _SOLO_DIGIT_RE.match(s):
            continue
        clean.append(s)
    return "\n".join(clean)


# ─── 영어 본문 추출 ─────────────────────────────────────────────────────────────

def extract_english_text(pdf_path: str) -> Dict[str, Any]:
    """
    교과서 본문 PDF에서 영어 텍스트만 추출.
    
    반환: {
        "passage_text": str,      # 영어 본문 전체
        "unit_title": str | None, # 첫 줄이 제목이면 추출
        "page_count": int,        # PDF 페이지 수
    }
    """
    pdf = pdfplumber.open(pdf_path)
    pages_text = []
    
    for page in pdf.pages:
        # 좌측 62% 영역만 잘라냄 (영어 칼럼)
        eng_crop = page.crop((0, 0, page.width * 0.62, page.height))
        text = eng_crop.extract_text()
        if text:
            cleaned = _clean_page_text(text)
            if cleaned:
                pages_text.append(cleaned)
    
    page_count = len(pdf.pages)
    pdf.close()
    
    full_text = "\n".join(pages_text).strip()
    
    # 첫 줄을 제목으로 추출 (보통 "English or Englishes?" 같은 형태)
    unit_title = None
    if full_text:
        first_line = full_text.split("\n")[0].strip()
        # 제목 줄: 짧고 영문으로 시작
        if len(first_line) < 80 and first_line[0].isupper():
            unit_title = first_line
    
    return {
        "passage_text": full_text,
        "unit_title": unit_title,
        "page_count": page_count,
    }
