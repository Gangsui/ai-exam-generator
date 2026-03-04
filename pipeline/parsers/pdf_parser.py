"""
모의고사 PDF 파서

처리 전략:
  - 문제지 PDF: 각 페이지에서 텍스트 추출 → 문항번호/질문/선지 구조화
  - 해설 PDF:   첫 페이지만 LLM(Gemini)으로 정답표 추출 → {번호: 정답} dict

의존 패키지:
  pip install pdfplumber google-generativeai
"""

import json
import re
import base64
import io
from pathlib import Path
from typing import Dict, Any, List, Optional

# ─── 선택적 import (패키지 없을 때 graceful 처리) ─────────────────────────────
try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False


# ─── 정규표현식 ─────────────────────────────────────────────────────────────────

CIRCLE_MAP = {"①": 1, "②": 2, "③": 3, "④": 4, "⑤": 5}
CIRCLE_RE = re.compile(r"[①②③④⑤]")
CHOICE_START_RE = re.compile(r"^([①②③④⑤])\s*(.*)")

# "18. 글의 목적으로 ..."  또는  "18) 글의 목적으로 ..."  또는  "18.글의 목적으로 ..."
Q_NUM_LINE_RE = re.compile(r"^(\d{1,2})[.)]\s*(.+)")

# 문항번호만 있는 줄: "36." "37." 등 (지시문이 상위 섹션 헤더에 있음)
Q_NUM_ONLY_RE = re.compile(r"^(\d{1,2})[.)]\s*$")

# 섹션 헤더: "[31~34]", "[38~39]", "[41~42]" 등
SECTION_HEADER_RE = re.compile(r"^\[(\d{1,2})~(\d{1,2})\]\s*(.*)")

# 정답표 줄: "1 ② 2 ④ 3 ① ..."  or  "번호 정답" 형태
ANSWER_TABLE_RE = re.compile(r"(\d{1,2})\s+([①②③④⑤]|\d)")

# PDF 페이지 꼬리말 패턴: "8 영어" / "3 영역 3" / "8 4 영어" / "4 영역" / "8" 등
# 숫자(들) + 영어|영역 조합, 또는 단독 페이지 번호(1~2자리 숫자)
FOOTER_RE = re.compile(r"^[\d\s]*(영어|영역)[\d\s]*$|^\d{1,2}$")

# 확인 사항 등 시험지 끝 안내문
CONFIRM_RE = re.compile(r"^※\s*확인\s*사항")

# 요약문완성 선지 테이블 헤더 쓰레기: "(A) (B) (A) (B)" 등
SUMMARY_TABLE_HEADER_RE = re.compile(r'^\(A\)\s*\(B\)(\s*\(A\)\s*\(B\))?\s*$')

# 지문 내 원문자 마커를 사용하는 문제 유형 (= 별도 선지 줄이 없는 유형)
# - 어법: "밑줄 친 부분 중, 어법상 틀린 것"
# - 어휘: "밑줄 친 부분 중, 문맥상 낱말의 쓰임"
# - 무관문장: "전체 흐름과 관계 없는 문장"
# - 문장삽입: "들어가기에 가장 적절한 곳"
# - 도표 일치/불일치: 지문에 ①~⑤ 마커가 있을 수 있음
INLINE_MARKER_PATTERNS = [
    re.compile(r"밑줄\s*친\s*부분\s*중.*(?:어법|틀린)"),
    re.compile(r"밑줄\s*친\s*부분\s*중.*(?:낱말|쓰임)"),
    re.compile(r"흐름과\s*관계\s*없는\s*문장"),
    re.compile(r"들어가기에\s*가장\s*적절한\s*(?:곳|것)"),
]

def _is_inline_marker_question(question_text: str) -> bool:
    """지문 내 ①~⑤ 마커를 사용하는 문제인지 판별."""
    for pat in INLINE_MARKER_PATTERNS:
        if pat.search(question_text):
            return True
    return False


def _is_real_choice_line(stripped: str, q: Optional[Dict] = None) -> bool:
    """줄이 진짜 선지인지 판별. 지문 내 ①~⑤ 마커와 구별.

    인라인 마커 문제(어법/어휘/무관문장/문장삽입)에서는
    ①~⑤로 시작하는 줄도 모두 지문의 일부로 처리.
    """
    cm = CHOICE_START_RE.match(stripped)
    if not cm:
        return False

    # 현재 문제가 인라인 마커 유형이면 → 모든 ①~⑤는 지문
    if q and _is_inline_marker_question(q.get("question_text", "")):
        return False

    remainder = cm.group(2).strip()
    # 선지 뒤의 내용이 80자 이상이면 → 지문의 일부일 확률 높음
    if len(remainder) > 80:
        return False
    return True


def _is_question_text_complete(text: str) -> bool:
    """질문 지시문이 완성되었는지 판별.
    
    한국어 지시문이 ?, 것은?, 시오. 등으로 끝나면 완성.
    영어로 시작하면 즉시 완성으로 취급 (빈칸추론 등이 바로 지문을 시작).
    빈 문자열이면 완성(섹션 지시문이 이미 별도 처리됨).
    """
    if not text:
        return True
    text = text.rstrip()
    # 점수 표시 제거: [3점]
    text = re.sub(r'\s*\[\d점\]\s*$', '', text).rstrip()
    # 한국어가 포함되어 있고 질문 형태 종결어미로 끝나면 완성
    if text.endswith(('?', '것은?', '시오.', '시오', '하시오.')):
        return True
    # 줄 끝이 한국어 종결어미 패턴
    if re.search(r'(?:것은|하시오|인가|세요|할까|는지)\s*[?.]?\s*$', text):
        return True
    # 영어만으로 이루어진 줄이면 → 지문 시작이므로 완성 취급
    korean_chars = sum(1 for c in text if '\uac00' <= c <= '\ud7a3')
    if korean_chars == 0:
        return True
    return False


# ─── 문제지 PDF 파싱 ───────────────────────────────────────────────────────────

def parse_problem_pdf(pdf_path: str, parser_version: str = "1.0.0") -> List[Dict[str, Any]]:
    """
    모의고사 문제지 PDF 에서 문항 리스트 추출.
    듣기(1~17번)를 건너뛰고 읽기 파트(18번~)부터 처리한다.
    """
    if not PDFPLUMBER_AVAILABLE:
        raise ImportError("pdfplumber 가 설치되지 않았습니다. pip install pdfplumber 를 실행하세요.")

    full_lines: List[str] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            w = page.width
            # 2단 컬럼 레이아웃: 좌열 → 우열 순서로 추출
            for col_bbox in [
                (0, 0, w * 0.5, page.height),       # 좌열
                (w * 0.5, 0, w, page.height),        # 우열
            ]:
                col = page.crop(col_bbox)
                text = col.extract_text(x_tolerance=3, y_tolerance=3)
                if text:
                    full_lines.extend(text.splitlines())

    return _parse_mock_lines(full_lines, parser_version)


def _parse_mock_lines(lines: List[str], parser_version: str) -> List[Dict[str, Any]]:
    """추출된 텍스트 줄 목록을 문항 단위로 파싱.
    
    개선 사항 (v2):
      - 섹션 헤더 [a~b] 처리: 지시문 + 공유 지문 전파
      - 어법/어휘 문제의 지문 내 ①~⑤ 마커를 선지로 오인하지 않음
      - 빈칸추론 등 본문이 question_text에 직접 시작되는 경우 처리
      - [41~42], [43~45] 등 장문 독해 공유 지문 처리
    """
    questions: List[Dict[str, Any]] = []
    current_q: Optional[Dict[str, Any]] = None
    passage_buffer: List[str] = []
    in_choices = False

    # 섹션 헤더 정보
    section_instruction: Optional[str] = None   # "[31~34] 빈칸에 들어갈 ..."
    section_range: Optional[tuple] = None        # (31, 34)
    section_instruction_pending = False           # 섹션 지시문 이어짐 여부
    shared_passage_buffer: List[str] = []         # 장문 독해 공유 지문
    shared_passage_text: Optional[str] = None     # 완성된 공유 지문
    shared_passage_range: Optional[tuple] = None  # (41, 42) 또는 (43, 45)

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # 페이지 꼬리말 스킵 ("8 영어", "3 영역 3" 등)
        if FOOTER_RE.match(stripped):
            continue

        # ※ 확인 사항 (시험지 끝) 스킵
        if CONFIRM_RE.match(stripped):
            # 이후 모든 줄 무시
            if current_q:
                _finalize_question(current_q, passage_buffer, parser_version)
                questions.append(current_q)
                current_q = None
            break

        # ── 섹션 헤더 감지: [31~34], [38~39], [41~42] 등 ──────────────
        m_sec = SECTION_HEADER_RE.match(stripped)
        if m_sec:
            # 이전 문항 저장
            if current_q:
                _finalize_question(current_q, passage_buffer, parser_version)
                questions.append(current_q)
                current_q = None
                passage_buffer = []
                in_choices = False

            sec_start = int(m_sec.group(1))
            sec_end = int(m_sec.group(2))
            sec_text = m_sec.group(3).strip()
            section_range = (sec_start, sec_end)
            section_instruction = sec_text if sec_text else None
            # 지시문이 줄 끝에서 잘렸을 수 있음 (다음 줄이 이어짐)
            # 마침표나 물음표로 끝나지 않으면 이어짐으로 판단
            if section_instruction and not section_instruction.endswith(('.', '?', '。')):
                section_instruction_pending = True
            else:
                section_instruction_pending = False

            # 장문 독해 섹션인지 판단 (지문을 먼저 읽고 → 뒤에 문제가 나옴)
            # [41~42], [43~45] 패턴: "다음 글을 읽고, 물음에 답하시오"
            if section_instruction and "읽고" in section_instruction:
                shared_passage_buffer = []
                shared_passage_text = None
                shared_passage_range = section_range
            else:
                shared_passage_range = None
                shared_passage_text = None
                shared_passage_buffer = []

            continue

        # ── 섹션 지시문 이어짐 처리 ───────────────────────────────────
        if section_instruction_pending and current_q is None:
            # 문항번호나 다른 헤더가 아닌 줄이면 지시문 이어붙이기
            m_q_check = Q_NUM_LINE_RE.match(stripped) or Q_NUM_ONLY_RE.match(stripped)
            m_sec_check = SECTION_HEADER_RE.match(stripped)
            if not m_q_check and not m_sec_check:
                section_instruction = (section_instruction or "") + " " + stripped
                if stripped.endswith(('.', '?', '。')):
                    section_instruction_pending = False
                    # 이제 장문 독해 여부 재판단
                    if section_instruction and "읽고" in section_instruction:
                        shared_passage_buffer = []
                        shared_passage_text = None
                        shared_passage_range = section_range
                continue

        # ── 섹션 헤더 이후, 아직 문항번호 나오기 전 → 공유 지문 수집 ──
        if shared_passage_range and current_q is None:
            m_q_check = Q_NUM_LINE_RE.match(stripped) or Q_NUM_ONLY_RE.match(stripped)
            if not m_q_check and not _is_real_choice_line(stripped):
                shared_passage_buffer.append(stripped)
                continue

        # ── 문항번호 줄 감지 ───────────────────────────────────────────
        m_q = Q_NUM_LINE_RE.match(stripped)
        m_q_only = Q_NUM_ONLY_RE.match(stripped) if not m_q else None

        if m_q or m_q_only:
            q_no = int((m_q or m_q_only).group(1))
            # 듣기 파트(1~17) 스킵
            if q_no <= 17:
                current_q = None
                passage_buffer = []
                in_choices = False
                continue

            # 이전 문항 저장
            if current_q:
                _finalize_question(current_q, passage_buffer, parser_version)
                questions.append(current_q)

            # 공유 지문 확정 (장문 독해 첫 문제 도달 시)
            if shared_passage_range and shared_passage_buffer:
                if shared_passage_range[0] <= q_no <= shared_passage_range[1]:
                    if shared_passage_text is None:
                        shared_passage_text = "\n".join(shared_passage_buffer).strip()
                        shared_passage_buffer = []

            # question_text 결정
            if m_q:
                raw_q_text = m_q.group(2).strip()
            else:
                raw_q_text = ""

            # 섹션 범위 내의 문항이면 섹션 지시문을 question_text에 사용
            if section_range and section_range[0] <= q_no <= section_range[1]:
                if not raw_q_text and section_instruction:
                    # "36." 처럼 번호만 있는 경우 → 섹션 지시문 사용
                    question_text = section_instruction
                    passage_buffer = []
                elif raw_q_text and section_instruction:
                    # 장문 독해 (공유 지문 있음): 개별 문항의 실제 질문 지시문
                    if shared_passage_text and shared_passage_range and shared_passage_range[0] <= q_no <= shared_passage_range[1]:
                        question_text = raw_q_text
                        passage_buffer = []
                    else:
                        # [31~34] 빈칸추론 등: raw_q_text는 본문 시작
                        question_text = section_instruction
                        passage_buffer = [raw_q_text]
                else:
                    question_text = raw_q_text
                    passage_buffer = []
            else:
                question_text = raw_q_text
                passage_buffer = []
                # 섹션 범위 벗어남 → 섹션 리셋
                if section_range and q_no > section_range[1]:
                    section_range = None
                    section_instruction = None

            current_q = {
                "question_no": q_no,
                "question_text": question_text,
                "choices": ["", "", "", "", ""],
                "question_type": "객관식",
                "answer": None,
                "raw_text": stripped,
                "unit_no": None,
                "unit_label": None,
                "section_type": None,
                "_q_text_pending": not _is_question_text_complete(question_text),
            }

            # 장문 독해 공유 지문 적용
            if shared_passage_range and shared_passage_range[0] <= q_no <= shared_passage_range[1]:
                if shared_passage_text:
                    current_q["_shared_passage"] = shared_passage_text

            in_choices = False
            continue

        if current_q is None:
            continue

        # ── 질문 지시문 이어짐 처리 ───────────────────────────────────
        if current_q.get("_q_text_pending"):
            # 문항번호, 선지, 섹션 헤더가 아닌 줄 → 지시문 계속
            m_q_check = Q_NUM_LINE_RE.match(stripped) or Q_NUM_ONLY_RE.match(stripped)
            m_sec_check = SECTION_HEADER_RE.match(stripped)
            if not m_q_check and not m_sec_check and not _is_real_choice_line(stripped, current_q):
                current_q["question_text"] += " " + stripped
                current_q["raw_text"] += "\n" + stripped
                if _is_question_text_complete(current_q["question_text"]):
                    current_q["_q_text_pending"] = False
                continue

        # ── 선지 줄 감지 (진짜 선지인지 판별) ─────────────────────────
        if _is_real_choice_line(stripped, current_q):
            cm = CHOICE_START_RE.match(stripped)
            in_choices = True
            idx = CIRCLE_MAP[cm.group(1)] - 1
            remainder = cm.group(2).strip()
            # 한 줄에 선지 2+ 합쳐진 경우 (2열 레이아웃)
            parts = re.split(r"\s+(?=[①②③④⑤])", remainder)
            current_q["choices"][idx] = parts[0].strip()
            for part in parts[1:]:
                m2 = CHOICE_START_RE.match(part)
                if m2:
                    idx2 = CIRCLE_MAP[m2.group(1)] - 1
                    current_q["choices"][idx2] = m2.group(2).strip()
            current_q["raw_text"] += "\n" + stripped
            continue

        # ── 지문 내 원문자 (어법/어휘 밑줄 마커) → 지문으로 처리 ──────
        if not in_choices:
            passage_buffer.append(stripped)
        else:
            # 선지 이어지는 줄 — BUT 다른 섹션 헤더가 나오면 중단
            m_sec2 = SECTION_HEADER_RE.match(stripped)
            if m_sec2:
                # 이전 문항 저장하고 섹션 헤더 처리로 되감기
                # (다음 루프에서 처리되도록 - 여기서는 수동 처리)
                _finalize_question(current_q, passage_buffer, parser_version)
                questions.append(current_q)
                current_q = None
                passage_buffer = []
                in_choices = False

                sec_start = int(m_sec2.group(1))
                sec_end = int(m_sec2.group(2))
                sec_text = m_sec2.group(3).strip()
                section_range = (sec_start, sec_end)
                section_instruction = sec_text if sec_text else None

                if section_instruction and "읽고" in section_instruction:
                    shared_passage_buffer = []
                    shared_passage_text = None
                    shared_passage_range = section_range
                else:
                    shared_passage_range = None
                    shared_passage_text = None
                    shared_passage_buffer = []
                continue

            last_filled = max((i for i, c in enumerate(current_q["choices"]) if c), default=-1)
            if last_filled >= 0:
                current_q["choices"][last_filled] += " " + stripped

        current_q["raw_text"] += "\n" + stripped

    # 마지막 문항
    if current_q:
        _finalize_question(current_q, passage_buffer, parser_version)
        questions.append(current_q)

    return questions


def _extract_summary_from_passage(q: Dict, passage_lines: List[str]):
    """요약문완성 문제에서 지문 끝의 요약문((A)/(B) 포함)을 question_text로 이동."""
    # 테이블 헤더 쓰레기 제거: "(A) (B) (A) (B)"
    while passage_lines and SUMMARY_TABLE_HEADER_RE.match(passage_lines[-1]):
        passage_lines.pop()
    if not passage_lines:
        return

    # 뒤에서부터 (A) 또는 (B)를 포함한 줄 범위 찾기
    i = len(passage_lines) - 1
    while i >= 0 and re.search(r'\([AB]\)', passage_lines[i]):
        i -= 1
    summary_start = i + 1  # (A)/(B)를 포함한 첫 줄

    if summary_start >= len(passage_lines):
        return  # 요약문 없음

    # 바로 앞 줄이 문장 중간이면 요약문 시작에 포함
    if summary_start > 0:
        prev = passage_lines[summary_start - 1].rstrip()
        if prev and prev[-1] not in '.?!。':
            summary_start -= 1

    summary_lines = passage_lines[summary_start:]
    del passage_lines[summary_start:]

    summary_text = "\n".join(summary_lines).strip()
    if summary_text:
        q["question_text"] += "\n\n" + summary_text


def _finalize_question(q: Dict[str, Any], passage_lines: List[str], parser_version: str):
    """문항 dict 완성 (지문 설정, 기본값 채우기)."""
    # 내부 상태 키 제거
    q.pop("_q_text_pending", None)

    # 요약문완성: 지문 끝의 요약문을 question_text로 이동
    if "요약" in q.get("question_text", ""):
        _extract_summary_from_passage(q, passage_lines)

    passage = "\n".join(passage_lines).strip() or None

    # 장문 독해 공유 지문 처리: _shared_passage가 있으면 우선 사용
    shared = q.pop("_shared_passage", None)
    if shared:
        if passage:
            q["passage_text"] = shared + "\n\n" + passage
        else:
            q["passage_text"] = shared
    else:
        q["passage_text"] = passage

    # 인라인 마커 문제 (어법/어휘/무관문장/문장삽입) → 선지를 ①~⑤로 자동 설정
    if _is_inline_marker_question(q.get("question_text", "")):
        if not any(q["choices"]):
            q["choices"] = ["①", "②", "③", "④", "⑤"]

    # 도표 문제에서 지문 내 ①~⑤가 있고 선지가 비어있으면 → 자동 설정
    qt = q.get("question_text", "")
    if ("도표" in qt or "일치하지 않는" in qt) and not any(q["choices"]):
        full_text = (q.get("passage_text", "") or "") + " " + qt
        marker_count = sum(1 for m in ["①","②","③","④","⑤"] if m in full_text)
        if marker_count >= 3:
            q["choices"] = ["①", "②", "③", "④", "⑤"]

    q["clean_text"] = q["question_text"]
    q["explanation"] = None
    q["needs_review"] = 1 if not any(q["choices"]) else 0
    q["quality_check_status"] = "needs_review" if q["needs_review"] else "pending"
    q["sub_type_pred"] = None
    q["sub_type_confidence"] = None
    q["sub_type_reason"] = None
    q["sub_type_final"] = None
    q["classifier_model"] = None
    q["classifier_version"] = None
    q["parser_version"] = parser_version


# ─── 해설 PDF 첫 페이지 정답표 추출 (LLM 기반) ────────────────────────────────

def extract_answer_table_from_solution_pdf(
    pdf_path: str,
    api_key: str = "",
    model_name: str = "gemini-2.5-flash",
) -> Dict[int, int]:
    """
    해설 PDF 첫 페이지를 LLM 에 전달하여 {문항번호: 정답(1~5)} 딕셔너리 반환.
    LLM 실패 시 regex fallback.
    """
    if not PDFPLUMBER_AVAILABLE:
        raise ImportError("pdfplumber 가 설치되지 않았습니다.")

    # ── 첫 페이지 텍스트 추출 ──────────────────────────────────────────────
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        text = first_page.extract_text() or ""

    # regex fallback 먼저 시도
    fallback = _extract_answer_table_by_regex(text)
    if len(fallback) >= 10:  # 어느정도 추출됐으면 사용
        return fallback

    # LLM 시도
    if api_key and GENAI_AVAILABLE:
        llm_result = _extract_answer_table_by_llm(text, api_key, model_name)
        if llm_result:
            return llm_result

    return fallback


def _extract_answer_table_by_regex(text: str) -> Dict[int, int]:
    """정규식으로 '번호 정답' 패턴 추출."""
    result = {}
    for m in ANSWER_TABLE_RE.finditer(text):
        q_no = int(m.group(1))
        raw_ans = m.group(2)
        if raw_ans in CIRCLE_MAP:
            result[q_no] = CIRCLE_MAP[raw_ans]
        else:
            try:
                ans = int(raw_ans)
                if 1 <= ans <= 5:
                    result[q_no] = ans
            except ValueError:
                pass
    return result


def _extract_answer_table_by_llm(
    text: str,
    api_key: str,
    model_name: str,
) -> Optional[Dict[int, int]]:
    """Gemini 에 정답표 텍스트를 전달하여 JSON 추출."""
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)
        prompt = f"""아래는 고1 영어 모의고사 해설집의 정답표입니다.
각 문항번호와 정답(1~5 정수)을 파싱하여 순수 JSON 딕셔너리로만 출력하세요.
예시 출력: {{"1": 2, "2": 4, "3": 1}}

정답표:
{text}

JSON 만 출력 (설명 없이):"""

        response = model.generate_content(prompt)
        raw = response.text.strip()

        # JSON 코드블록 제거
        raw = re.sub(r"```(?:json)?", "", raw).replace("```", "").strip()
        data = json.loads(raw)
        return {int(k): int(v) for k, v in data.items() if 1 <= int(v) <= 5}
    except Exception as exc:
        print(f"[PDF Parser] LLM 정답표 추출 실패: {exc}")
        return None


def validate_answer_table(answer_table: Dict[int, int]) -> Dict[str, Any]:
    """
    정답표 검증.
    반환: { "valid": bool, "missing": [...], "out_of_range": [...] }
    """
    issues: Dict[str, Any] = {"valid": True, "missing": [], "out_of_range": [], "duplicate": []}

    if not answer_table:
        issues["valid"] = False
        return issues

    found_nums = sorted(answer_table.keys())
    expected = list(range(min(found_nums), max(found_nums) + 1))
    missing = [n for n in expected if n not in answer_table]
    out_of_range = [n for n, v in answer_table.items() if not (1 <= v <= 5)]

    if missing:
        issues["missing"] = missing
        issues["valid"] = False
    if out_of_range:
        issues["out_of_range"] = out_of_range
        issues["valid"] = False

    return issues


def apply_answer_table(questions: List[Dict[str, Any]], answer_table: Dict[int, int]) -> List[Dict[str, Any]]:
    """문제지 문항 리스트에 정답 매핑."""
    for q in questions:
        q_no = q.get("question_no")
        if q_no and q_no in answer_table:
            q["answer"] = str(answer_table[q_no])
            q["needs_review"] = 0
        else:
            q["needs_review"] = 1
    return questions
