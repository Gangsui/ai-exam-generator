"""
출판사 .txt 학교 시험 파일 파서

처리 순서:
  1. 파일 읽기 / 인코딩 처리
  2. 단원 블록 분리 ([1과 본문] 태그 기준)
  3. 문항 블록 단위 분리
  4. 질문, 선지, 정답 추출
  5. needs_review 플래그 부여
"""

import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple


# ─── 정규표현식 ─────────────────────────────────────────────────────────────────

# 동그라미 숫자
CIRCLE = "①②③④⑤"
CIRCLE_RE = re.compile(r"[①②③④⑤]")
CIRCLE_MAP = {"①": 1, "②": 2, "③": 3, "④": 4, "⑤": 5}

# 줄 끝에 정답 동그라미가 붙은 패턴:   "...은? ②"  또는  "...은? ② "
ANSWER_AT_END_RE = re.compile(r"([①②③④⑤])\s*$")

# 선지 시작 패턴: ① some text  또는  ①some text
CHOICE_START_RE = re.compile(r"^([①②③④⑤])\s*(.*)")

# 단원 태그 패턴: [1과 본문], [2과 대화문], [3과 문법], [2과] 등
UNIT_TAG_RE = re.compile(
    r"\[(?P<unit_no>\d+)과\s*(?P<section>[^\]]*)\.?\]"
)

# [N과] 단독 또는 [N과 section] 형식 (줄 전체가 이 태그인 경우)
UNIT_COURSE_TAG_RE = re.compile(r"^\s*\[(\d+)과\s*([^\]]*)\]\s*$")

# 어떤 대괄호 태그든 (standalone 태그줄)
ANY_BRACKET_TAG_RE = re.compile(r"^\s*\[[^\]]+\]\s*$")

# 제외 태그: 참고서/부교재 또는 모의고사 출처 → 해당 문항 제외
# - 참고서: 올림포스, 수능특강, 능률보카, 리딩파워 등
# - 모의고사 출처: [25년 6월 24번], [24년 9월 31번] 등 (XX년 YY월 ZZ번 패턴)
# - 학교 프린트/외부지문: [학교 프린트], [외부지문] 등 (단원 정보 없음)
EXCLUDE_TAG_RE = re.compile(
    r"올림포스|수능특강|수능연계교재|능률보카|리딩파워|파사주|워드마스터"
    r"|Wonder|Special\s*Lesson|기출\d+강|Analysis"
    r"|학교\s*프린트|학교프린트|외부지문|범위\s*외|범위외"
    r"|학교\s*유인물"
    r"|\d{2}년\s*\d+월\s*(?:\d+[-~]?\d*번|모의고사)"  # 모의고사 출처 태그
)
TEXTBOOK_TAG_RE = EXCLUDE_TAG_RE  # 하위 호환 별칭

# 본문 안내 구문 + 단원 태그가 있는 줄
PASSAGE_INTRO_RE = re.compile(
    r"다음\s+글을\s+읽고.*?\[(?P<unit_no>\d+)과\s*(?P<section>[^\]]*)\.?\]",
    re.DOTALL,
)

# 참고서/워드마스터 태그 (단원 정보 아닌 것)
OTHER_TAG_RE = re.compile(r"^\s*\[[^\]]+\]\s*$")

# 숫자로 시작하는 문항번호 줄: "15. 다음" 또는 "15) 다음"  -- 일부 파일에 있을 수 있음
Q_NUM_RE = re.compile(r"^(\d{1,2})[.)]\s+")

# ── 주관식/서술형 전용 패턴 ──────────────────────────────────────────────────────

# 주관식 지시문: "쓰시오", "작성하시오", "서술하시오", "완성하시오" 등이 줄 어딘가에 포함
# ($앵커 없음 — 뒤에 인라인 답 "(가) word"가 붙어올 수 있음)
SUBJ_INSTR_RE = re.compile(
    r"쓰시오|작성하시오|서술하시오|완성하시오"
    r"|고치시오|적으시오|채우시오|바꾸시오|답하시오"
    r"|영작하시오|배열하시오|재배열하시오|기술하시오"
    r"|해석하시오|설명하시오|수정하시오|변형하시오"
    r"|요약하시오|작문하시오|논술하시오"
)

# 답 명시줄: (가) raising  /  (A) If I had studied...  /  (1) ⓒ, ...  /  ⓐ believe
# 키: 한글/영문 1글자, 숫자(1~9), 또는 동그라미소문자(ⓐ~ⓩ, 괄호 없음)
SHOWN_ANSWER_RE = re.compile(
    r"^\s*(?:\(([가-힣A-Za-z\d]+)\)|([ⓐ-ⓩ]))\s+([^_:{}]+?)\s*$"
)

# 답란 슬롯: (가): ____  /  (A): ____  /  (1): ____
SLOT_RE = re.compile(
    r"^\s*(?:\(([가-힣A-Za-z\d]+)\)|([ⓐ-ⓩ]))\s*[:：]?\s*_{2,}"
)

# 단순 정답란: "정답: ___"  /  "답: ___"
PLAIN_ANSWER_SLOT_RE = re.compile(r"^(?:정답|답)\s*[:：]?\s*_{2,}")


# ─── 인코딩 감지 헬퍼 ──────────────────────────────────────────────────────────

def read_txt(path: str) -> str:
    """UTF-8 → CP949 순서로 시도해서 파일 내용 반환."""
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            return Path(path).read_text(encoding=enc)
        except (UnicodeDecodeError, LookupError):
            continue
    # 마지막 수단: errors='replace'
    return Path(path).read_text(encoding="utf-8", errors="replace")


# ─── 단원(블록) 분리 ────────────────────────────────────────────────────────────

def _split_into_unit_blocks(lines: List[str]) -> List[Dict[str, Any]]:
    """
    [n과 section] 태그가 나오는 줄을 기준으로 단원 블록을 분리한다.
    각 블록은 { unit_no, unit_label, section_type, lines } 를 가진다.
    태그가 없는 줄들은 unit_no=None 블록으로 모은다.
    """
    blocks = []
    current_block = {"unit_no": None, "unit_label": None, "section_type": None, "lines": []}

    for line in lines:
        # 단원 태그가 있는 줄인지 확인
        m_intro = PASSAGE_INTRO_RE.search(line)
        m_tag = UNIT_TAG_RE.search(line)

        if m_intro or m_tag:
            m = m_intro or m_tag
            unit_no = int(m.group("unit_no"))
            section = m.group("section").strip()
            # 이전 블록의 마지막 비어있지 않은 줄이 문제줄/서술형 지시문이면
            # 그 줄을 새 블록으로 이동한다.
            # ("다음 글...은? ②" 바로 다음 줄에 [4과] 태그가 오는 패턴 처리)
            last_nonempty = ""
            last_nonempty_idx = -1
            for pi, pl in enumerate(reversed(current_block["lines"])):
                if pl.strip():
                    last_nonempty = pl.strip()
                    last_nonempty_idx = len(current_block["lines"]) - 1 - pi
                    break
            is_after_mcq  = bool(ANSWER_AT_END_RE.search(last_nonempty))
            is_after_subj = (bool(SUBJ_INSTR_RE.search(last_nonempty))
                             and not bool(CIRCLE_RE.search(last_nonempty))
                             and not last_nonempty.startswith(("·", "•", "※")))
            if last_nonempty_idx >= 0 and (is_after_mcq or is_after_subj):
                # 문제줄을 현 블록에서 빼서 새 블록의 첫 줄로 이동
                moved_line = current_block["lines"].pop(last_nonempty_idx)
                if current_block["lines"] or current_block["unit_no"] is not None:
                    blocks.append(current_block)
                current_block = {
                    "unit_no": unit_no,
                    "unit_label": f"{unit_no}과",
                    "section_type": section,
                    "lines": [moved_line, line],
                }
            else:
                # 일반적인 새 블록 시작
                if current_block["lines"] or current_block["unit_no"] is not None:
                    blocks.append(current_block)
                current_block = {
                    "unit_no": unit_no,
                    "unit_label": f"{unit_no}과",
                    "section_type": section,
                    "lines": [line],
                }
        else:
            current_block["lines"].append(line)

    if current_block["lines"] or current_block["unit_no"] is not None:
        blocks.append(current_block)

    return blocks


# ─── 문항 블록 분리 ─────────────────────────────────────────────────────────────

def _split_into_question_blocks(lines: List[str]) -> List[List[str]]:
    """
    줄 끝에 정답 동그라미가 있는 줄을 기점으로 문항 블록을 나눈다.
    반환: 각 블록이 [ 질문줄, choice줄들, ... ] 형태의 리스트
    """
    # "다음 글을 읽고 물음에 답하시오." 같은 공유지문 안내 → 새 passage 시작
    _PASSAGE_INTRO_SPLIT = re.compile(
        r"다음.*(?:물음에\s*답|질문에\s*답)"
    )

    blocks = []
    current = []
    passage_buffer = []  # 질문 전 지문 행들

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current:
                current.append(line)
            else:
                passage_buffer.append(line)
            continue

        # 줄 끝 정답 확인
        # 단, "① ①  ② ②  ③ ③  ④ ④  ⑤ ⑤" 처럼 ①로 시작하는 선지 확인 줄은
        # 새 문항이 아니라 현재 블록의 선지이므로 제외
        is_choice_summary = bool(CIRCLE_RE.match(stripped))
        if ANSWER_AT_END_RE.search(stripped) and not is_choice_summary:
            # passage_buffer 를 먼저 닫음
            if current:
                blocks.append(current)
            # 새 문항 블록: 앞에 쌓인 passage 포함
            current = list(passage_buffer) + [line]
            passage_buffer = []
        elif current:
            # 공유지문 안내 줄 → 현재 블록 닫고 새 passage 시작
            if _PASSAGE_INTRO_SPLIT.search(stripped):
                blocks.append(current)
                current = []
                passage_buffer = [line]
            else:
                current.append(line)
        else:
            passage_buffer.append(line)

    if current:
        blocks.append(current)

    return blocks


# ─── 단일 문항 블록 파싱 ───────────────────────────────────────────────────────

def _parse_question_block(
    block: List[str],
    q_no: int,
    parent_unit_no: Optional[int] = None,
    parent_section: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    하나의 문항 블록 (passage + question + choices) 을 파싱.
    """
    if not block:
        return None

    # ── 1. 질문줄 및 정답 추출 ──────────────────────────────────────────────
    q_line_idx = None
    for i, line in enumerate(block):
        if ANSWER_AT_END_RE.search(line.strip()):
            q_line_idx = i
            break

    if q_line_idx is None:
        return None

    q_line = block[q_line_idx].strip()
    answer_m = ANSWER_AT_END_RE.search(q_line)
    answer_circle = answer_m.group(1)
    answer = CIRCLE_MAP[answer_circle]
    question_text = ANSWER_AT_END_RE.sub("", q_line).strip()

    # ── 2. 지문(passage) 추출: q_line 이전 행들 ──────────────────────────────
    passage_lines = []
    _has_exclude_tag_in_passage = False
    for line in block[:q_line_idx]:
        s = line.strip()
        if not s:
            continue
        if OTHER_TAG_RE.match(s):
            continue  # 참고서 태그 스킵
        # 단원 태그 있는 줄은 스킵 (이미 블록 분리 시 처리됨)
        if UNIT_TAG_RE.search(s) or PASSAGE_INTRO_RE.search(s):
            continue
        # 인라인 모의고사/참고서 태그 감지: "다음 글을 읽고... [25년 6월 41~42번]"
        if EXCLUDE_TAG_RE.search(s):
            _has_exclude_tag_in_passage = True
        passage_lines.append(s)
    if _has_exclude_tag_in_passage:
        return None
    passage_text = "\n".join(passage_lines).strip() or None

    # ── 3. q_line 이후: 태그 감지 + 지문 추출 + 선지 추출 ──────────────────────
    after_lines = block[q_line_idx + 1:]

    # 첫 번째 비어있지 않은 줄 = 태그
    tag_idx = None
    detected_unit_no: Optional[int] = None
    detected_section: str = ""
    for ai, aline in enumerate(after_lines):
        astripped = aline.strip()
        if not astripped:
            continue
        m_unit = UNIT_COURSE_TAG_RE.match(astripped)
        if m_unit:
            detected_unit_no = int(m_unit.group(1))
            detected_section = m_unit.group(2).strip()
            tag_idx = ai
        elif ANY_BRACKET_TAG_RE.match(astripped):
            # 참고서 태그([올림포스...]) → 해당 문항 제외
            if TEXTBOOK_TAG_RE.search(astripped):
                return None
            # 모의고사 출처 태그([25년 6월 24번] 등) → tag_idx 잡고 계속 수집
            # detected_unit_no는 None → 아래에서 parent 상속 또는 0(단원 미지정)으로 처리
            tag_idx = ai
            detected_section = astripped.strip("[] ")  # 출처 태그를 section_type으로 저장
        else:
            # 태그 없이 바로 선지/지문 → parent 없으면 제외
            if parent_unit_no is None:
                return None
        break

    # unit_no 결정: 인라인 태그 우선, 없으면 부모 블록 상속
    if detected_unit_no is None:
        if parent_unit_no is None:
            # 단원 정보 없지만 태그는 있었음(모의고사/외부지문) → unit_no=0으로 허용
            if tag_idx is not None:
                detected_unit_no = 0
            else:
                return None
        else:
            detected_unit_no = parent_unit_no
            detected_section = detected_section or parent_section or ""

    # 태그 다음부터 첫 선지(①) 이전까지 = 지문
    post_tag_lines = after_lines[tag_idx + 1:] if tag_idx is not None else after_lines
    passage_lines = []
    first_choice_idx_in_post = None
    for pi, pline in enumerate(post_tag_lines):
        pstripped = pline.strip()
        if not pstripped:
            continue
        if CHOICE_START_RE.match(pstripped):
            first_choice_idx_in_post = pi
            break
        if _is_subj_instruction(pstripped):
            # 다음 주관식 문항 시작 → 지문 없이 선지도 없음
            break
        if ANSWER_AT_END_RE.search(pstripped):
            # 다음 객관식 문항 시작 → 종료
            break
        if not OTHER_TAG_RE.match(pstripped):
            # 인라인 EXCLUDE 태그 감지
            if EXCLUDE_TAG_RE.search(pstripped):
                return None
            passage_lines.append(pstripped)

    # 지문 업데이트: pre-q_line 지문이 없을 때만 post-tag 내용으로 설정
    # (pre-q_line 지문이 있으면 "(A) (B)" 같은 선지 헤더가 덮어쓰는 문제 방지)
    if passage_lines and not passage_text:
        passage_text = "\n".join(passage_lines).strip() or None

    # 선지 추출
    choices: List[str] = ["", "", "", "", ""]  # 인덱스 0→① ... 4→⑤
    choice_src = post_tag_lines if first_choice_idx_in_post is None else post_tag_lines[first_choice_idx_in_post:]
    in_choice: Optional[int] = None  # 현재 선지 인덱스

    for line in choice_src:
        stripped = line.strip()
        if not stripped:
            continue
        # 다음 문항 시작 신호 → 선지 수집 종료
        if _is_subj_instruction(stripped):
            break
        if ANSWER_AT_END_RE.search(stripped) and not CIRCLE_RE.match(stripped):
            break
        cm = CHOICE_START_RE.match(stripped)
        if cm:
            in_choice = CIRCLE_MAP[cm.group(1)] - 1  # 0-based
            choices[in_choice] = cm.group(2).strip()
        elif in_choice is not None:
            # 선지 내용이 여러 줄에 걸친 경우
            choices[in_choice] = (choices[in_choice] + " " + stripped).strip()

    # ── 4. 인접 선지가 한 줄에 붙어있는 경우 재분리 ─────────────────────────
    # 예: "① option1② option2③ option3"
    choices = _expand_inline_choices(choices)

    # ── 5. 유효성 ────────────────────────────────────────────────────────────
    filled_choices = [c for c in choices if c]
    needs_review = False
    if not question_text:
        needs_review = True
    if filled_choices and len(filled_choices) < 3:
        needs_review = True

    raw_text = "\n".join(line.rstrip() for line in block)

    return {
        "question_no": q_no,
        "question_text": question_text,
        "choices": choices if any(choices) else [],
        "question_type": "객관식",
        "answer": str(answer),
        "passage_text": passage_text,
        "raw_text": raw_text,
        "clean_text": question_text,
        "explanation": None,
        "needs_review": 1 if needs_review else 0,
        "quality_check_status": "needs_review" if needs_review else "pending",
        # 단원 정보 (태그에서 추출)
        "unit_no": detected_unit_no if detected_unit_no != 0 else None,
        "unit_label": f"{detected_unit_no}과" if detected_unit_no and detected_unit_no != 0 else None,
        "section_type": detected_section or None,
        # 이하 분류 필드는 classifier 에서 채움
        "sub_type_pred": None,
        "sub_type_confidence": None,
        "sub_type_reason": None,
        "sub_type_final": None,
        "classifier_model": None,
        "classifier_version": None,
    }


def _expand_inline_choices(choices: List[str]) -> List[str]:
    """
    '① text1② text2' 처럼 하나의 선지 문자열에 여러 선지가 합쳐진 경우 분리.
    """
    result = []
    for idx, text in enumerate(choices):
        if not text:
            result.append(text)
            continue
        # 선지 안에 다른 동그라미 번호가 있으면 분리
        parts = re.split(r"([①②③④⑤])", text)
        if len(parts) > 1:
            # parts = ['', '①', 'optA', '②', 'optB', ...]  -> 재구성 필요
            # 현재 선지(idx+1)의 시작 부분만 추출
            result.append(parts[0].strip())
            # 나머지는 이후 choices 에 병합
            for i in range(1, len(parts) - 1, 2):
                circle = parts[i]
                val = parts[i + 1].strip() if i + 1 < len(parts) else ""
                cnum = CIRCLE_MAP.get(circle, None)
                if cnum is not None and cnum - 1 < len(choices):
                    if not choices[cnum - 1]:
                        choices[cnum - 1] = val
        else:
            result.append(text)

    # 길이 맞추기
    while len(result) < 5:
        result.append("")
    return result[:5]


# ─── 주관식 / 서술형 문항 추출 ─────────────────────────────────────────────────


# passage intro 패턴 ("\ub2e4\uc74c \uae00\uc744 \uc77d\uace0 \ubb3c\uc74c\uc5d0 \ub2f5\ud558\uc2dc\uc624" \ub4f1) \u2014 \uc8fc\uad00\uc2dd \uc544\ub2d8
_PASSAGE_INTRO_SUBJ_EXCLUDE = re.compile(
    r"\ub2e4\uc74c.*(?:\ubb3c\uc74c|\uc9c8\ubb38).*\ub2f5\ud558\uc2dc\uc624"
)


def _is_subj_instruction(s: str) -> bool:
    """
    \uc8fc\uad00\uc2dd \uc9c0\uc2dc\ubb38 \uc5ec\ubd80 \ud310\uc815.
    - '\uc4f0\uc2dc\uc624|\uc791\uc131\ud558\uc2dc\uc624|\uc11c\uc220\ud558\uc2dc\uc624' \ub4f1 \ud3ec\ud568
    - \ub05d\uc5d0 \u2460\u2461\u2462\u2463\u2464 \uc5c6\uc74c (= \uac1d\uad00\uc2dd\uc774 \uc544\ub2d8)
    - \u00b7\u2022\u203b- \uc2dc\uc791 \uc904\uc740 \uc81c\uc678 (\uc870\uac74/\uc3d0\uc810 \uc904)
    - '\ub2e4\uc74c \uae00\uc744 \uc77d\uace0 \ubb3c\uc74c\uc5d0 \ub2f5\ud558\uc2dc\uc624' \uac19\uc740 passage intro\ub294 \uc81c\uc678
    """
    if not s:
        return False
    if CIRCLE_RE.search(s):
        return False
    if s.startswith(("\u00b7", "\u2022", "\u203b", "-")):
        return False
    if not SUBJ_INSTR_RE.search(s):
        return False
    # passage intro\ub294 \uc8fc\uad00\uc2dd \uc9c0\uc2dc\ubb38\uc774 \uc544\ub2d8
    if _PASSAGE_INTRO_SUBJ_EXCLUDE.search(s):
        return False
    return True


def _parse_subjective_block(
    lines: List[str],
    start: int,
    parent_unit_no: Optional[int] = None,
    parent_section: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    `lines[start]`을 시작점으로 주관식 문항 하나 추출.

    반환: (question_dict | None, next_line_idx)

    지원 패턴
    ---------
    패턴 A  (인라인 괄호 답): "...쓰시오. (가) raising  (나) investors"
    패턴 A2 (인라인 bare 답): "...쓰시오. dedication"  /  "...쓰시오. hadn't taken"
    패턴 B  (다음줄 괄호 답): "...쓰시오.\\n(가) raising\\n(나) investors"
    패턴 B2 (다음줄 bare 답): "...쓰시오.\\ndedication"
    패턴 C  (조건・빈칸)     : "...쓰시오.\\n<조건>\\n...\\n(가): ___"
    패턴 D  (단순 서술)      : "...쓰시오.\\n정답: ___"
    """
    n = len(lines)
    instr_raw = lines[start].strip()

    # ── 0. "윗글" 참조 주관식: 상위 passage 에 EXCLUDE 태그가 있으면 제외
    if re.match(r"(윗글|위\s*글|위의\s*글)", instr_raw):
        for back_i in range(start - 1, max(start - 80, -1), -1):
            bline = lines[back_i].strip()
            if not bline:
                continue
            if '다음' in bline and ('읽고' in bline or '물음' in bline):
                if EXCLUDE_TAG_RE.search(bline):
                    return None, start + 1
                break
            if UNIT_COURSE_TAG_RE.match(bline):
                break

    # ① 지시문과 인라인 답 분리
    # 마지막 '쓰시오/작성하시오/서술하시오' 이후에 (가) word 가 있으면 인라인 답
    instr_text = instr_raw
    inline_answers: Dict[str, str] = {}

    # 마지막 지시어(쓰시오 등) 이후에 오는 인라인 답 추출
    # (단, ...) 같은 긴 조건이 있어도 건너뜀 — 영문 단어만 답으로 인정
    instr_kwd_iter = list(re.finditer(r"(쓰시오|작성하시오|서술하시오|완성하시오|고치시오|적으시오|채우시오|바꾸시오|답하시오|영작하시오|배열하시오|재배열하시오|기술하시오|해석하시오|설명하시오|수정하시오|변형하시오|요약하시오|작문하시오|논술하시오)\.?", instr_raw))
    if instr_kwd_iter:
        after_kwd = instr_raw[instr_kwd_iter[-1].end():]
        # (가) EnglishWord / (A) word 패턴 추출 (값이 영문 단어)
        ans_hits = list(re.finditer(
            r"\(([가-힣A-Za-z\d]+)\)\s+([a-zA-Z][a-zA-Z'\-\s,]*[a-zA-Z.!?])",
            after_kwd,
        ))
        # ⓐ word 패턴 (동그라미 소문자, 괄호 없음)
        if not ans_hits:
            ans_hits = list(re.finditer(
                r"([ⓐ-ⓩ])\s+([a-zA-Z][a-zA-Z'\-\s]*[a-zA-Z])",
                after_kwd,
            ))
        # (1) any-content 패턴 (숫자 키 + 자유 형식 값, 예: "(1) ⓒ, sentence...")
        if not ans_hits:
            ans_hits = list(re.finditer(
                r"\((\d+)\)\s+(.*?)(?=\s*\(\d+\)\s|$)",
                after_kwd,
            ))
        if ans_hits:
            first_abs = instr_kwd_iter[-1].end() + ans_hits[0].start()
            instr_text = instr_raw[:first_abs].rstrip()
            for am in ans_hits:
                inline_answers[am.group(1)] = am.group(2).strip()
        else:
            # 패턴 A2: 괄호 없는 인라인 bare 답
            # 조건: tail이 또 다른 지시문을 포함하지 않아야 함
            # 조건 접속어('단,') / 한국어 지시 이어짐 제외
            _BARE_SKIP = re.compile(
                r"(쓰시오|작성하시오|서술하시오|완성하시오"
                r"|고치시오|적으시오|채우시오|바꾸시오|답하시오"
                r"|영작하시오|배열하시오|재배열하시오|기술하시오"
                r"|해석하시오|설명하시오|수정하시오|변형하시오"
                r"|요약하시오|작문하시오|논술하시오|"
                r"답안은|반드시|고려하여|표시하고)"
            )
            _BARE_SKIP_START = ("단,", "단 ", "필요", "우리말", "주어진 ", "조건에")
            tail = after_kwd.strip()
            if (tail
                and not _BARE_SKIP.search(tail)
                and not tail.startswith(_BARE_SKIP_START)):
                instr_text = instr_raw[:instr_kwd_iter[-1].end()].rstrip()
                inline_answers["_bare"] = tail

    # ② 태그 확인 (지시문 다음 줄에서 [N과] 태그를 탐색)
    #    답 명시줄이 태그보다 먼저 올 수 있으므로 답줄·빈줄을 건너뛰며 탐색
    i = start + 1
    subj_unit_no: Optional[int] = None
    subj_section: str = ""
    for look_i in range(i, n):  # 블록 끝까지 탐색 (빈줄·답줄 skip → 즉시 break)
        ls = lines[look_i].strip()
        if not ls:
            continue
        # 답 명시줄은 건너뛰고 계속 탐색
        if SHOWN_ANSWER_RE.match(ls):
            continue
        # "요약문: ..." 같은 설명줄도 건너뜀
        if ls.startswith("요약문"):
            continue
        m_unit = UNIT_COURSE_TAG_RE.match(ls)
        if m_unit:
            subj_unit_no = int(m_unit.group(1))
            subj_section = m_unit.group(2).strip()
        elif ANY_BRACKET_TAG_RE.match(ls):
            # 참고서/모의고사 태그 → 제외
            if TEXTBOOK_TAG_RE.search(ls):
                return None, i
            # 기타 태그 → parent 없으면 제외, 있으면 상속
            if parent_unit_no is None:
                return None, i
        break  # 태그 또는 일반 콘텐츠 줄에서 탐색 종료

    # unit_no 결정
    if subj_unit_no is None:
        if parent_unit_no is None:
            return None, i
        subj_unit_no = parent_unit_no
        subj_section = parent_section or ""

    answers: Dict[str, str] = dict(inline_answers)
    body_lines: List[str] = []
    blank_run = 0
    phase = "answers"  # 'answers' → 'body' → 'slots'

    while i < n:
        raw = lines[i]
        s = raw.strip()

        if not s:
            blank_run += 1
            if blank_run >= 4:   # 4줄 연속 공백 → 다음 문항
                break
            body_lines.append(raw)
            i += 1
            continue
        blank_run = 0

        # 단원/참고서 태그 줄 → body에 포함하지 않음
        if UNIT_COURSE_TAG_RE.match(s) or ANY_BRACKET_TAG_RE.match(s):
            # EXCLUDE 태그 발견 시 해당 문항 전체 제외
            if ANY_BRACKET_TAG_RE.match(s) and TEXTBOOK_TAG_RE.search(s):
                return None, i
            i += 1
            continue

        # 다음 객관식 문항 시작 → 블록 종료
        if ANSWER_AT_END_RE.search(s):
            break
        # 다음 주관식 지시문 (answers 단계 이후에만)
        if phase != "answers" and _is_subj_instruction(s):
            break

        # 답 명시줄: (가) word / (1) ⓒ, ... / ⓐ believe
        if phase == "answers":
            am = SHOWN_ANSWER_RE.match(s)
            if am:
                val = am.group(3).strip()
                # 값에 지시어가 포함되면 하위 질문 → 답이 아니라 body
                if SUBJ_INSTR_RE.search(val):
                    phase = "body"
                    body_lines.append(raw)
                    i += 1
                    continue
                key = am.group(1) or am.group(2)   # 그룹1: 괄호키, 그룹2: ⓐ키
                answers[key] = val
                i += 1
                continue
            # 패턴 B2: 괄호 없이 단어/짧은 구만 있는 줄 (영어 또는 영어+한국어 혼합)
            # 조건: 지시문 키워드 없음, 동그라미 없음, 슬롯 아님, 짧은 줄(≤80자)
            if (not CIRCLE_RE.search(s)
                and not SUBJ_INSTR_RE.search(s)
                and not SLOT_RE.match(s)
                and not PLAIN_ANSWER_SLOT_RE.match(s)
                and not s.startswith(("<", "[", "□", "※"))
                and len(s) <= 80
                and "_bare" not in answers):  # 이미 인라인 bare 있으면 스킵
                # 한국어만으로 구성된 경우 body로 처리 (순한국어 서술 답은 분리 불가)
                eng_ratio = len(re.findall(r"[a-zA-Z0-9'\-→]", s)) / max(len(s), 1)
                if eng_ratio >= 0.3 or re.match(r"^[ⓐ-ⓩ①-⑤].*→", s):
                    answers["_bare"] = s
                    i += 1
                    continue
            phase = "body"

        # 답란 슬롯: (가): ____ / (1): ____ / ⓐ: ____
        if SLOT_RE.match(s):
            lm = re.match(r"^\s*(?:\(([가-힣A-Za-z\d]+)\)|([ⓐ-ⓩ]))", s)
            if lm:
                k = lm.group(1) or lm.group(2)
                if k not in answers:
                    pass  # 학생이 채울 칸 → 답에 추가하지 않음
            phase = "slots"
            i += 1
            continue

        # "정답: ___" 형식
        if PLAIN_ANSWER_SLOT_RE.match(s):
            phase = "slots"
            i += 1
            continue

        # 슬롯 단계: 슬롯 라인만 소비
        if phase == "slots":
            if not SLOT_RE.match(s) and not PLAIN_ANSWER_SLOT_RE.match(s):
                # 슬롯 이후 다른 내용 → 적당히 포함
                body_lines.append(raw)
            i += 1
            continue

        body_lines.append(raw)
        i += 1

    if len(instr_text.strip()) < 5:
        return None, i

    # body에 인라인 EXCLUDE 태그 포함 여부 확인
    # → "다음 글을 읽고... [25년 6월 41~42번]" 같은 모의고사 지문을 참조하는 서술형 제외
    for bl in body_lines:
        bs = bl.strip()
        if bs and EXCLUDE_TAG_RE.search(bs):
            return None, i

    # 답 문자열 조합
    # - (가)(나)(다) 키 있는 경우: "(가): raising, (나): investors"
    # - bare 답만 있는 경우: "dedication"  /  "hadn't taken"
    # - 혼합은 없으므로 분기
    if answers:
        if "_bare" in answers and len(answers) == 1:
            answer_str = answers["_bare"]
        else:
            keyed = {k: v for k, v in answers.items() if k != "_bare"}
            answer_str = ", ".join(
                f"({k}): {v}" for k, v in sorted(keyed.items())
            ) if keyed else answers.get("_bare")
    else:
        answer_str = None

    passage = "\n".join(l.rstrip() for l in body_lines if l.strip()).strip() or None
    raw_text = "\n".join(l.rstrip() for l in lines[start:i])

    # "윗글" 참조 주관식: body는 문제 보충 자료이며 진짜 passage는 공유지문에서 상속받아야 함
    # body 내용을 question_text에 합치고 passage를 None으로 설정
    if re.match(r"(윗글|위\s*글|위의\s*글)", instr_text) and passage:
        instr_text = instr_text + "\n" + passage
        passage = None

    return {
        "question_no": None,
        "question_text": instr_text,
        "choices": [],
        "question_type": "주관식",
        "answer": answer_str,
        "passage_text": passage,
        "raw_text": raw_text,
        "clean_text": instr_text,
        "explanation": None,
        "needs_review": 1,
        "quality_check_status": "needs_review",
        "sub_type_pred": None,
        "sub_type_confidence": None,
        "sub_type_reason": None,
        "sub_type_final": None,
        "classifier_model": None,
        "classifier_version": None,
        "unit_no": subj_unit_no,
        "unit_label": f"{subj_unit_no}과" if subj_unit_no else None,
        "section_type": subj_section or None,
    }, i


def _extract_subjective_questions(
    lines: List[str],
    parent_unit_no: Optional[int] = None,
    parent_section: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    라인 리스트에서 주관식/서술형 문항을 모두 추출한다.

    객관식 문항은 `_split_into_question_blocks` 가 수집하므로,
    여기서는 '주관식 지시문(①②③④⑤ 없음) + 답 명시 또는 답란 슬롯' 패턴만 처리.
    """
    # "다음 글을 읽고 물음에 답하시오." 같은 passage intro 패턴
    _PASSAGE_INTRO_RE = re.compile(r"다음.*(?:물음에\s*답|질문에\s*답)")

    results: List[Dict[str, Any]] = []
    i = 0
    n = len(lines)

    while i < n:
        s = lines[i].strip()
        if _is_subj_instruction(s):
            # ── 상위 passage 블록 탐색 ──
            # "다음 글을 읽고 물음에 답하시오" 뒤의 지문을 찾아 할당
            enclosing_passage = None
            for back_i in range(i - 1, max(i - 80, -1), -1):
                bline = lines[back_i].strip()
                if not bline:
                    continue
                # 이전 주관식 지시문이나 객관식 선지를 만나면 중단
                if _is_subj_instruction(bline) or CIRCLE_RE.search(bline):
                    break
                # passage intro 발견 → intro 다음줄 ~ 현재줄 사이가 passage
                if _PASSAGE_INTRO_RE.search(bline):
                    p_lines = []
                    for pi in range(back_i + 1, i):
                        pl = lines[pi].rstrip()
                        if pl.strip():
                            p_lines.append(pl)
                    enclosing_passage = "\n".join(p_lines).strip() or None
                    break

            q, next_i = _parse_subjective_block(
                lines, i,
                parent_unit_no=parent_unit_no,
                parent_section=parent_section,
            )
            if q:
                # passage 가 없고 상위 블록에서 찾은 passage 가 있으면 할당
                if not q.get("passage_text") and enclosing_passage:
                    q["passage_text"] = enclosing_passage
                results.append(q)
            i = max(i + 1, next_i)
        else:
            i += 1

    return results


# ─── 메인 파싱 함수 ────────────────────────────────────────────────────────────

def parse_txt_file(file_path: str, parser_version: str = "1.0.0") -> List[Dict[str, Any]]:
    """
    출판사 .txt 파일 하나를 파싱하여 문항 리스트 반환.
    각 문항 dict 에는 단원 정보(unit_no, unit_label, section_type) 포함.
    """
    text = read_txt(file_path)
    lines = text.splitlines()

    all_questions: List[Dict[str, Any]] = []
    q_counter = 1

    # 단원 블록으로 분리
    unit_blocks = _split_into_unit_blocks(lines)

    for block in unit_blocks:
        parent_unit_no  = block["unit_no"]
        parent_section  = block["section_type"]

        # 해당 블록 내 문항 블록 분리
        q_blocks = _split_into_question_blocks(block["lines"])

        _exclude_refs = False   # EXCLUDE 태그 블록 뒤 "윗글" 참조 제외 플래그
        for qb in q_blocks:
            q = _parse_question_block(
                qb, q_counter,
                parent_unit_no=parent_unit_no,
                parent_section=parent_section,
            )
            if q:  # None 이면 수집 불가 → 제외
                qt = q.get("question_text", "")
                is_ref = bool(re.match(r"(윗글|위\s*글|위의\s*글)", qt))
                if _exclude_refs and is_ref:
                    continue  # "윗글" 이 제외된 모의고사 지문 참조 → 제외
                if not is_ref:
                    _exclude_refs = False
                q["parser_version"] = parser_version
                all_questions.append(q)
                q_counter += 1
            else:
                # 제외된 블록에 EXCLUDE 태그가 있으면 후속 "윗글" 문항도 제외
                if any(EXCLUDE_TAG_RE.search(l) for l in qb):
                    _exclude_refs = True

        # 서술형 문항 추가 추출
        for sq in _extract_subjective_questions(
            block["lines"],
            parent_unit_no=parent_unit_no,
            parent_section=parent_section,
        ):
            # unit_no 확보된 주관식만 수집
            if sq.get("unit_no") is not None:
                sq["parser_version"] = parser_version
                sq["question_no"] = q_counter
                all_questions.append(sq)
                q_counter += 1

    # ── 공유 지문 전파 ──────────────────────────────────────────────────────────
    # "윗글의..." 처럼 직전 지문을 참조하는 문항에 passage_text 가 없으면
    # 직전 문항의 passage_text 를 그대로 상속한다.
    REFERS_PREV_RE = re.compile(r"^(윗글|위\s*글|위의\s*글|위\s*대화)")
    # "밑줄 친"을 참조하지만 "다음"으로 시작하지 않는 하위 질문도 상속 대상
    _HAS_MILCHUL = re.compile(r"밑줄\s*친")
    last_passage: Optional[str] = None
    last_unit_no = None
    for q in all_questions:
        qt = q.get("question_text", "")
        if q.get("passage_text"):
            last_passage = q["passage_text"]
            last_unit_no = q.get("unit_no")
        elif (last_passage
              and q.get("unit_no") == last_unit_no
              and (REFERS_PREV_RE.match(qt)
                   or (_HAS_MILCHUL.search(qt) and not qt.startswith("다음")))):
            q["passage_text"] = last_passage

    return all_questions
