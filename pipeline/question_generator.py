"""
문제 생성 모듈 — Gemini로 새 문제 생성 (2단계 방식)

2단계 생성 파이프라인:
  Stage 1 (분석 에이전트): 본문 + 기출 예시 → 출제 전략 분석
  Stage 2 (생성):          본문 + 출제 전략 → 실제 문제 생성

입력: 교과서 본문(textbooks) 또는 모의고사 지문(questions) + 기출 예시 → LLM → 새 문제
"""

import json
import re
import sqlite3
import time
from typing import Dict, Any, List, Optional

try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False

from pipeline.config import DB_PATH, GEMINI_API_KEY, GEMINI_MODEL
from pipeline.classifiers.subtype_classifier import SUB_TYPES


# ── 프롬프트 ──────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT_OBJECTIVE = """당신은 대한민국 수능·모의고사 영어 영역 출제위원급 전문가입니다.
고등학교 1학년 영어 내신 시험 문제를 출제합니다.

## 출제 원칙
1. **교과서 본문 기반**: 반드시 [교과서 본문]에 등장하는 내용·어휘·문법을 소재로 사용합니다.
   교과서에 없는 내용을 임의로 만들어내지 마세요.
2. **기출 스타일 모방**: [기출 예시]의 문체·형식·선지 구성·지시문 표현을 최대한 따릅니다.
3. **난이도 적정성**: 고등학교 1학년 내신 수준 (중상)으로, 지나치게 쉽거나 대학 수준이 되지 않도록 합니다.
4. **객관식 5지선다**: 정답은 반드시 1개이며, 오답 4개는 학생이 혼동할 수 있을 만큼 그럴듯하되 명확히 틀려야 합니다.
5. **지문 활용**: 지문이 필요한 유형은 교과서 본문에서 적절히 발췌·재구성하여 포함하세요. 원문을 그대로 복사하지 말고, 출제 의도에 맞게 일부 수정(밑줄, 빈칸, 어순 변경 등)하세요.
   - **밑줄 표시**: 밑줄이 필요한 부분은 반드시 `<u>밑줄 칠 부분</u>` HTML 태그로 표시하세요.
6. **정답 근거 명확**: 정답의 근거가 지문/교과서 본문에서 반드시 확인 가능해야 합니다.

## 유형별 출제 가이드 ({sub_type})
- **빈칸추론**: 지문 핵심 내용을 담은 문장에서 중요 어구를 빈칸 처리. 선지는 의미적으로 유사하지만 문맥상 적절한 것은 하나뿐이어야 함.
- **어법**: `<u>밑줄 친 부분</u>` 중 어법상 틀린/맞는 것 고르기. 관계대명사·분사·시제·수일치·병렬구조 등을 활용.
- **어휘**: `<u>밑줄 친 단어</u>`의 문맥적 의미와 맞는/맞지 않는 단어를 고르기.
- **주제/요지/제목/목적**: 지문 전체의 중심 내용을 파악하는 문제. 선지는 한글/영어 모두 가능.
- **내용일치/불일치**: 지문 내용과 일치/불일치하는 선지 고르기.
- **순서배열**: 주어진 문장 뒤에 이어질 글의 순서를 (A)-(B)-(C)로 배열.
- **문장삽입**: 글의 흐름상 주어진 문장이 들어가기에 가장 적절한 위치를 고르기.
- **연결어**: 빈칸에 들어갈 적절한 연결어(접속사/부사) 고르기.
- **요약문완성**: 지문을 요약한 문장의 빈칸에 적절한 어구 고르기.
- **함축의미추론/지칭추론**: 밑줄 친 표현의 함축 의미 또는 지칭 대상을 추론.
- **무관문장**: 글의 전체 흐름과 관계 없는 문장 고르기.

## 출력 형식 (JSON 배열만 출력)
```json
[
  {{
    "question_no": 1,
    "sub_type": "{sub_type}",
    "question_text": "다음 글의 빈칸에 들어갈 말로 가장 적절한 것은?",
    "passage": "지문 텍스트 (교과서 본문 기반 재구성)",
    "choices": ["① 선지1", "② 선지2", "③ 선지3", "④ 선지4", "⑤ 선지5"],
    "answer": 3,
    "translation": "지문 전체의 자연스러운 한국어 해석",
    "explanation": "정답의 근거를 구체적으로 설명 (문법 규칙, 문맥적 논리, 오답 이유 등)"
  }}
]
```
**위 JSON 형식만 출력하세요.** 인사말, 부연설명 없이 순수 JSON 배열만 출력합니다.

{count_instruction}
"""

_SYSTEM_PROMPT_SUBJECTIVE = """당신은 대한민국 고등학교 영어 내신 출제위원급 전문가입니다.
고등학교 1학년 영어 내신 시험의 서술형/주관식 문제를 출제합니다.

## 출제 원칙
1. **교과서 본문 기반**: 반드시 [교과서 본문]에 등장하는 문장·어휘·문법을 소재로 사용합니다.
   교과서에 없는 내용을 절대 사용하지 마세요.
2. **기출 스타일 참고 (복사 금지)**: [기출 예시]의 **출제 형식·패턴·문체**를 참고하되, 예시의 내용(지문/답/해설)을 그대로 복사하지 마세요.
   반드시 **제공된 교과서 본문**에서 새로운 문제를 만드세요.
3. **모범답안 검증**: 모범 답안은 교과서 본문에서 직접 확인 가능해야 합니다.
4. **채점 가능성**: 답안이 명확하고, 채점 기준이 모호하지 않아야 합니다.
5. **지시문 명확성**: 문제 지시문(question_text)만 읽고도 무엇을 해야 하는지 명확해야 합니다. 지문 속 전문 용어('dot', 'gap' 등)를 지시문에 그대로 쓰지 말고, "다음 글의 빈칸에 들어갈 말을 쓰시오" 같이 구체적으로 작성하세요.
- **빈칸 채우기**: 교과서 본문의 핵심 문장에서 일부를 빈칸으로 만들고 올바른 단어/구문을 쓰도록 함
- **단어 변형 (어형 변환)**: 주어진 단어를 문맥에 맞는 형태로 변환 (동사 시제, 분사, 명사 등)
- **영작 (조건 영작)**: 주어진 조건(단어, 구문)을 사용하여 영어 문장 완성
- **문장 완성**: 교과서 본문을 바탕으로 불완전한 문장을 완성
- **배열**: 주어진 단어들을 올바른 어순으로 배열하여 문장 완성
- **요약/서술**: 지문 내용을 우리말 또는 영어로 요약
- **우리말 해석**: 밑줄 친 영어 문장을 우리말로 해석. **지문에서 밑줄은 반드시 `<u>해당 문장</u>` HTML 태그로 표시하세요.**
- **영어 설명**: 교과서 본문 내용에 대한 질문에 영어로 간략히 답변

## ⚠️ 밑줄 표시 규칙
- 지문(passage)에서 밑줄이 필요한 부분은 반드시 `<u>밑줄 칠 부분</u>` HTML 태그를 사용하세요.
- 예: `The key to success is <u>believing in yourself and never giving up</u>.`
- "밑줄 친 부분을 해석하시오" 문제에서 passage에 `<u>` 태그가 없으면 학생이 어디를 해석해야 하는지 알 수 없습니다.

## 출력 형식 (JSON 배열만 출력)
```json
[
  {{
    "question_no": 1,
    "sub_type": "서술형",
    "question_text": "다음 글의 빈칸 (A)~(C)에 들어갈 알맞은 말을 본문에서 찾아 쓰시오.",
    "passage": "지문 텍스트 (교과서 본문 기반)",
    "answer": "(A) discovery  (B) communicate  (C) imagination",
    "translation": "지문 전체의 자연스러운 한국어 해석",
    "explanation": "정답의 근거를 구체적으로 설명 (문법 규칙, 문맥적 논리 등)"
  }}
]
```
**위 JSON 형식만 출력하세요.** 인사말, 부연설명 없이 순수 JSON 배열만 출력합니다.

{count_instruction}
"""

# 모의고사 기반 서술형 프롬프트
_SYSTEM_PROMPT_MOCK_SUBJECTIVE = """당신은 대한민국 고등학교 영어 내신 출제위원급 전문가입니다.
아래 [모의고사 지문]을 활용하여 서술형/주관식 문제를 출제합니다.

## 출제 원칙
1. **모의고사 지문 기반**: 제공된 모의고사 지문의 내용을 활용하여 문제를 만들세요.
2. **기출 스타일 참고 (복사 금지)**: [기출 예시]의 **출제 형식·패턴·문체**만 참고하세요.
   예시의 지문/답/해설 내용을 그대로 복사하지 말고, 제공된 모의고사 지문에서 새 문제를 만드세요.
3. **지문 필수 포함**: 문제에 반드시 지문(passage)을 포함하세요. "다음 글에 따르면"이라고 한다면 지문이 있어야 합니다.
4. **모범답안 검증**: 답안은 지문에서 직접 확인 가능해야 합니다.
5. **채점 가능성**: 답안이 명확하고, 채점 기준이 모호하지 않아야 합니다.
6. **지시문 명확성**: 문제 지시문(question_text)만 읽고도 무엇을 해야 하는지 명확해야 합니다. 지문 속 전문 용어를 지시문에 그대로 쓰지 말고, 구체적으로 작성하세요.

## 서술형 유형 가이드
다양한 유형으로 출제하세요:
- **빈칸 채우기**: 지문의 핵심 문장에서 일부를 빈칸으로 만들고 올바른 단어/구문을 쓰도록 함
- **단어 변형 (어형 변환)**: 주어진 단어를 문맥에 맞는 형태로 변환
- **영작/문장 완성**: 조건을 사용하여 영어 문장 완성
- **배열**: 단어들을 올바른 어순으로 배열
- **요약/서술**: 지문 내용을 요약
- **우리말 해석**: 밑줄 친 영어 문장을 해석. **지문에서 밑줄은 반드시 `<u>해당 문장</u>` HTML 태그로 표시하세요.**
- **영어 설명**: 지문 내용에 대한 질문에 영어로 답변

## ⚠️ 밑줄 표시 규칙
- 지문(passage)에서 밑줄이 필요한 부분은 반드시 `<u>밑줄 칠 부분</u>` HTML 태그를 사용하세요.
- "밑줄 친 부분을 해석하시오" 문제에서 passage에 `<u>` 태그가 없으면 불합격입니다.

## 출력 형식 (JSON 배열만 출력)
```json
[
  {{
    "question_no": 1,
    "sub_type": "서술형",
    "question_text": "다음 글의 빈칸에 들어갈 알맞은 말을 쓰시오.",
    "passage": "지문 텍스트 (모의고사 지문 기반)",
    "answer": "모범 답안 (반드시 하나의 문자열로, JSON 객체가 아닌 텍스트)",
    "translation": "지문 전체의 자연스러운 한국어 해석",
    "explanation": "정답의 근거를 구체적으로 설명 (문법 규칙, 문맥적 논리 등)"
  }}
]
```
**위 JSON 형식만 출력하세요.** 인사말, 부연설명 없이 순수 JSON 배열만 출력합니다.

{count_instruction}
"""
# ── 모의고사 기반 프롬프트 ────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT_MOCK = """당신은 대한민국 수능·모의고사 영어 영역 출제위원급 전문가입니다.
아래 [모의고사 지문]은 원래 다른 유형의 문제로 출제된 지문입니다.
이 지문을 활용하여 **{target_type}** 유형의 새로운 객관식(5지선다) 문제를 만드세요.

## 핵심 규칙
1. **지문 변환**: 원래 지문을 **{target_type}** 유형에 맞게 재구성하세요.
   - 원문의 내용·어휘·문법은 유지하되, 출제 의도에 맞게 밑줄, 빈칸, 어순 변경, 단락 나누기 등을 적용하세요.
   - **밑줄 표시**: 밑줄이 필요한 부분은 반드시 `<u>밑줄 칠 부분</u>` HTML 태그로 표시하세요.
   - 원문을 그대로 복사하지 말고, 출제에 필요한 형태로 변형하세요.
2. **유형 변환 예시**:
   - 원래 빈칸추론 지문 → 어법 문제: 지문 내 문법 요소에 `<u>...</u>` 태그로 밑줄을 그어 어법 판단 문제로
   - 원래 주제 지문 → 빈칸추론: 핵심 어구를 빈칸 처리하여 추론 문제로
   - 원래 어휘 지문 → 순서배열: 단락을 나누어 순서 배열 문제로
3. **난이도**: 고등학교 1학년 모의고사 수준 (중상).
4. **오답 매력도**: 오답 4개는 학생이 혼동할 수 있을 만큼 그럴듯하되 명확히 틀려야 합니다.
5. **정답 근거 명확**: 정답의 근거가 지문에서 반드시 확인 가능해야 합니다.
6. **[{target_type} 예시]를 참고**: 해당 유형의 출제 스타일·지시문·선지 구성을 반드시 따르세요.

## 유형별 변환 가이드 ({target_type})
- **빈칸추론**: 지문 핵심 내용을 담은 문장에서 중요 어구를 빈칸 처리.
- **어법**: 지문 내 문법 요소에 `<u>...</u>` HTML 태그로 밑줄을 그어 어법 판단.
- **어휘**: 지문 내 단어에 `<u>...</u>` HTML 태그로 밑줄을 그어 문맥적 의미 판단.
- **주제/요지/제목/목적**: 지문 전체의 중심 내용 파악.
- **내용일치/불일치**: 지문 내용과 일치/불일치하는 선지.
- **순서배열**: 단락을 (A)-(B)-(C)로 나누어 순서 배열.
- **문장삽입**: 주어진 문장이 들어갈 위치 고르기.
- **연결어**: 빈칸에 적절한 연결어 고르기.
- **요약문완성**: 지문 요약문의 빈칸에 적절한 어구.
- **무관문장**: 글의 흐름과 관계 없는 문장 고르기.

## 출력 형식 (JSON 배열만 출력)
```json
[
  {{
    "question_no": 1,
    "sub_type": "{target_type}",
    "question_text": "문제 지시문",
    "passage": "변환된 지문 (원래 지문 기반 재구성)",
    "choices": ["① 선지1", "② 선지2", "③ 선지3", "④ 선지4", "⑤ 선지5"],
    "answer": 3,
    "translation": "지문 전체의 자연스러운 한국어 해석",
    "explanation": "정답의 근거를 구체적으로 설명 (문법 규칙, 문맥적 논리, 오답 이유 등)"
  }}
]
```
**위 JSON 형식만 출력하세요.** 인사말, 부연설명 없이 순수 JSON 배열만 출력합니다.

{count_instruction}
"""

# ── 유틸리티 ──────────────────────────────────────────────────────────────────

def _get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_textbook_passage(
    conn: sqlite3.Connection,
    publisher: Optional[str] = None,
    textbook_label: Optional[str] = None,
    unit_no: Optional[int] = None,
    subject: Optional[str] = None,
    semester_exam: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """교과서 본문을 가져온다. 조건이 없으면 랜덤 1개."""
    clauses, params = [], []
    if publisher:
        clauses.append("publisher=?"); params.append(publisher)
    if textbook_label:
        clauses.append("textbook_label=?"); params.append(textbook_label)
    if unit_no is not None:
        clauses.append("unit_no=?"); params.append(unit_no)
    if subject:
        clauses.append("subject=?"); params.append(subject)
    if semester_exam:
        clauses.append("semester_exam=?"); params.append(semester_exam)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    row = conn.execute(
        f"SELECT * FROM textbooks {where} ORDER BY RANDOM() LIMIT 1", params
    ).fetchone()

    if not row:
        return None
    return dict(row)


def _fetch_mock_passage(
    conn: sqlite3.Connection,
    exclude_sub_type: str,
    exam_year: Optional[int] = None,
    exam_month: Optional[int] = None,
    count: int = 1,
    required_question_nos: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """모의고사 다른 유형의 지문을 가져온다.
    
    exclude_sub_type으로 지정된 유형을 제외하고
    지문이 있는 모의고사 문항을 랜덤으로 가져온다.
    required_question_nos가 주어지면 해당 번호를 우선 포함한다.
    """
    clauses = [
        "e.source_type='모의고사'",
        "q.passage_text IS NOT NULL",
        "q.passage_text != ''",
        "LENGTH(q.passage_text) > 50",
    ]
    params: list = []

    if exclude_sub_type:
        clauses.append("(q.sub_type_pred IS NULL OR q.sub_type_pred != ?)")
        params.append(exclude_sub_type)
    if exam_year is not None:
        clauses.append("e.exam_year=?"); params.append(exam_year)
    if exam_month is not None:
        clauses.append("e.exam_month=?"); params.append(exam_month)

    # required_question_nos가 있으면 해당 번호의 지문을 우선 가져오기
    if required_question_nos:
        placeholders = ",".join("?" for _ in required_question_nos)
        req_clauses = [c for c in clauses if "sub_type_pred" not in c]  # exclude_sub_type 필터 제거 (필수 번호는 유형 무관)
        req_where = " AND ".join(req_clauses)
        req_params = [p for i, p in enumerate(params) if i != 0 or not exclude_sub_type]
        if exclude_sub_type:
            req_params = params[1:]  # exclude_sub_type 파라미터 제외
        else:
            req_params = list(params)
        req_rows = conn.execute(f"""
            SELECT q.question_no, q.question_text, q.passage_text,
                   q.sub_type_pred, q.choices, q.answer,
                   e.exam_year, e.exam_month, e.grade
            FROM questions q JOIN exams e ON q.exam_id=e.id
            WHERE {req_where} AND q.question_no IN ({placeholders})
            ORDER BY q.question_no
        """, req_params + list(required_question_nos)).fetchall()
        required = [dict(r) for r in req_rows]
        required_nos_set = {r["question_no"] for r in required}

        # 나머지 랜덤 지문 (required 제외)
        remaining_count = max(count - len(required), 0)
        if remaining_count > 0:
            where = " AND ".join(clauses)
            if required_nos_set:
                excl_placeholders = ",".join("?" for _ in required_nos_set)
                where += f" AND q.question_no NOT IN ({excl_placeholders})"
                extra_params = list(required_nos_set)
            else:
                extra_params = []
            extra_rows = conn.execute(f"""
                SELECT q.question_no, q.question_text, q.passage_text,
                       q.sub_type_pred, q.choices, q.answer,
                       e.exam_year, e.exam_month, e.grade
                FROM questions q JOIN exams e ON q.exam_id=e.id
                WHERE {where}
                ORDER BY RANDOM()
                LIMIT ?
            """, params + extra_params + [remaining_count]).fetchall()
            return required + [dict(r) for r in extra_rows]
        return required

    where = " AND ".join(clauses)
    rows = conn.execute(f"""
        SELECT q.question_no, q.question_text, q.passage_text,
               q.sub_type_pred, q.choices, q.answer,
               e.exam_year, e.exam_month, e.grade
        FROM questions q JOIN exams e ON q.exam_id=e.id
        WHERE {where}
        ORDER BY RANDOM()
        LIMIT ?
    """, params + [count]).fetchall()

    return [dict(r) for r in rows]


def _fetch_example_questions(
    conn: sqlite3.Connection,
    sub_type: Optional[str] = None,
    source_type: Optional[str] = None,
    question_type: str = "객관식",
    publisher: Optional[str] = None,
    unit_no: Optional[int] = None,
    semester_exam: Optional[str] = None,
    exam_year: Optional[int] = None,
    exam_month: Optional[int] = None,
    max_examples: int = 15,
) -> List[Dict[str, Any]]:
    """기출 예시 문항을 가져온다."""
    clauses = ["q.question_type=?"]
    params: list = [question_type]

    if sub_type:
        clauses.append("q.sub_type_pred=?"); params.append(sub_type)
    if source_type:
        clauses.append("e.source_type=?"); params.append(source_type)
    if publisher:
        clauses.append("e.publisher=?"); params.append(publisher)
    if unit_no is not None:
        clauses.append("q.unit_no=?"); params.append(unit_no)
    if semester_exam:
        clauses.append("e.term=?"); params.append(semester_exam)
    if exam_year is not None:
        clauses.append("e.exam_year=?"); params.append(exam_year)
    if exam_month is not None:
        clauses.append("e.exam_month=?"); params.append(exam_month)

    where = " AND ".join(clauses)
    rows = conn.execute(f"""
        SELECT q.question_text, q.passage_text, q.choices, q.answer,
               q.sub_type_pred, q.unit_no,
               e.publisher, e.source_type, e.school_name
        FROM questions q JOIN exams e ON q.exam_id=e.id
        WHERE {where}
        ORDER BY RANDOM()
        LIMIT ?
    """, params + [max_examples]).fetchall()

    return [dict(r) for r in rows]


def _format_example(ex: Dict[str, Any], idx: int) -> str:
    """기출 예시를 텍스트로 포맷."""
    lines = [f"### 예시 {idx}"]
    if ex.get("question_text"):
        lines.append(f"문제: {ex['question_text']}")
    if ex.get("passage_text"):
        passage = ex["passage_text"]
        if len(passage) > 2000:
            passage = passage[:2000] + "..."
        lines.append(f"지문: {passage}")
    if ex.get("choices"):
        choices = ex["choices"]
        if isinstance(choices, str):
            try:
                choices = json.loads(choices)
            except Exception:
                choices = []
        if choices:
            lines.append("선지:")
            for i, c in enumerate(choices):
                if c:
                    lines.append(f"  {i+1}. {c}")
    if ex.get("answer"):
        lines.append(f"정답: {ex['answer']}")
    return "\n".join(lines)


def _filter_valid_examples(examples: List[Dict[str, Any]], question_type: str) -> List[Dict[str, Any]]:
    """기출 예시의 품질을 검증하여 불량 데이터를 걸러낸다."""
    valid = []
    for ex in examples:
        qt = ex.get("question_text") or ""
        # 1) 문제 텍스트가 너무 짧으면 불량
        if len(qt.strip()) < 5:
            continue
        # 2) 객관식이면: 선지 확인
        if question_type == "객관식":
            choices = ex.get("choices") or []
            if isinstance(choices, str):
                try:
                    choices = json.loads(choices)
                except Exception:
                    choices = []
            # 선지가 3개 미만이면 불량
            real_choices = [c for c in choices if c and len(str(c).strip()) > 0]
            if len(real_choices) < 3:
                continue
        # 3) 정답 없으면 제외
        if not ex.get("answer"):
            continue
        # 4) 말도안되는 깨진 텍스트 검출 (비정상 문자 비율)
        total_len = len(qt)
        if total_len > 0:
            weird_chars = sum(1 for c in qt if ord(c) > 0xFFFF or c in '\x00\x01\x02')
            if weird_chars / total_len > 0.1:
                continue
        valid.append(ex)
    return valid


def _validate_generated(questions: List[Dict[str, Any]], textbook_passage: str,
                        question_type: str) -> List[Dict[str, Any]]:
    """생성된 문제를 검증하여 불량 문항을 제거한다."""
    valid = []
    # 교과서 본문에서 단어 집합 (검증용)
    tb_words = set()
    if textbook_passage:
        for w in re.findall(r'[a-zA-Z]{3,}', textbook_passage.lower()):
            tb_words.add(w)

    for q in questions:
        # 1) 문제 텍스트 존재 확인
        if not q.get("question_text") or len(q["question_text"].strip()) < 5:
            continue
        # 2) 정답 확인
        answer = q.get("answer_text") or q.get("answer")
        if not answer:
            continue
        if question_type == "객관식":
            # 객관식: answer_text(문자열)가 있으면 OK (선지는 나중에 생성됨)
            if isinstance(answer, str) and len(answer.strip()) >= 1:
                q["answer_text"] = answer.strip()
            elif isinstance(answer, int):
                # 혹시 answer가 정수로 온 경우 (레거시 호환)
                choices = q.get("choices", [])
                if choices and 1 <= answer <= len(choices):
                    q["answer_text"] = re.sub(r"^[①②③④⑤]\s*", "", choices[answer - 1]).strip()
                else:
                    continue
            else:
                continue
        else:
            # 주관식 정답은 비어있지 않아야
            if isinstance(answer, str) and len(answer.strip()) < 1:
                continue

        # 3) 교과서 본문 기반 확인 (지문이 있을 때)
        passage = q.get("passage", "")
        if passage and tb_words:
            passage_words = set(re.findall(r'[a-zA-Z]{3,}', passage.lower()))
            if passage_words:
                overlap = passage_words & tb_words
                overlap_ratio = len(overlap) / len(passage_words)
                # 교과서 본문과 20% 미만 겹치면 본문 기반이 아님
                if overlap_ratio < 0.2:
                    continue

        valid.append(q)
    return valid


def _normalize_answer(answer: Any) -> Any:
    """answer 필드가 dict/list일 경우 문자열로 변환한다.

    LLM이 서술형 답을 JSON 객체로 반환하면 JavaScript에서
    [object Object]로 표시되는 문제를 방지한다.

    예: {"요약": "...", "예시1": "..."} → "요약: ...\n예시1: ..."
    """
    if isinstance(answer, dict):
        parts = []
        for k, v in answer.items():
            parts.append(f"{k}: {v}")
        return "\n".join(parts)
    if isinstance(answer, list):
        return "\n".join(str(item) for item in answer)
    return answer


def _parse_llm_response(raw_text: str) -> List[Dict[str, Any]]:
    """LLM 응답에서 JSON 배열을 추출."""
    text = raw_text.strip()
    # ```json ... ``` 블록 제거
    text = re.sub(r"```(?:json)?\s*", "", text).strip()
    # 닫는 ``` 제거
    text = re.sub(r"```\s*$", "", text).strip()

    parsed: List[Dict[str, Any]] = []

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            data = [data]
        if isinstance(data, list):
            parsed = data
    except json.JSONDecodeError:
        pass

    if not parsed:
        # JSON 배열 찾기 (가장 바깥 [ ... ])
        match = re.search(r"\[\s*\{.*\}\s*\]", text, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group())
            except json.JSONDecodeError:
                pass

    if not parsed:
        # JSON 객체 하나만 있는 경우
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                obj = json.loads(match.group())
                if isinstance(obj, dict):
                    parsed = [obj]
            except json.JSONDecodeError:
                pass

    # answer 필드 정규화: dict/list → 문자열
    for q in parsed:
        if "answer" in q:
            q["answer"] = _normalize_answer(q["answer"])

    return parsed


# ── 검수 에이전트 ────────────────────────────────────────────────────────────

_REVIEW_SYSTEM_PROMPT = """당신은 대한민국 고등학교 영어 시험 문제 검수 전문가입니다.
AI가 생성한 문제를 아래 기준으로 꼼꼼히 검수하세요.

## 검수 기준

### 1. 정답 정확성 (가장 중요 — 이 항목이 불합격이면 반드시 fail)
- **객관식**: 정답 번호에 해당하는 선지만 유일하게 올바른가? 다른 선지도 정답이 될 수 있으면 불합격.
- **서술형**: 모범답안이 지문에서 확인 가능한가? 다른 답이 정답이 될 수 없는가?
- 정답이 지문 내용과 실제로 일치하는지 반드시 확인하세요.

### 2. 지문-문제 일관성
- 빈칸추론: 지문에 빈칸( ______ )이 실제로 있는가?
- 어법/어휘: 밑줄 친 부분이 `<u>...</u>` 태그로 표시되어 있는가?
- 순서배열: (A)(B)(C)로 나뉘어 있는가?
- 문장삽입: 삽입 위치 표시가 있는가?
- 지시문에서 "빈칸"이라고 했으면 지문에 빈칸이 반드시 있어야 합니다.
- 지시문에서 "밑줄 친"이라고 했으면 지문에 `<u>...</u>` 태그가 반드시 있어야 합니다.

### 3. 선지 품질 (객관식만 — 매우 엄격하게 검수)
이 항목은 시험 문제의 변별력과 공정성을 좌우하는 핵심 기준입니다.

#### 3-1. 기본 요건
- 선지가 정확히 5개인가?
- 정답이 유일한가? (2개 이상 답이 가능하면 즉시 불합격)
- 선지 번호(①②③④⑤)가 올바르게 표기되어 있는가?

#### 3-2. 오답 매력도 (가장 중요한 선지 품질 기준)
- **각 오답이 '그럴듯하게 틀린' 답인가?** 고1 수준의 학생이 고민할 만한 오답이어야 합니다.
- 오답이 지문의 내용과 전혀 무관한 단어/표현이면 불합격. (예: 지문이 환경에 관한 글인데 오답에 "음악 장르"가 등장)
- 오답이 너무 뻔하게 틀려서 소거법 없이도 바로 탈락시킬 수 있으면 불합격.
- **이상적인 오답**: 지문 내용과 관련되지만, 맥락·논리·문법적으로 정답이 될 수 없는 선지.

#### 3-3. 선지 간 형식적 균형
- **길이 균형**: 정답 선지만 유난히 길거나 짧지 않은가? 모든 선지의 단어 수가 비슷해야 합니다.
- **문법적 병렬성**: 모든 선지가 동일한 품사/형태인가? (예: 모두 명사구, 모두 동사원형 등)
- **정답 돋보임 방지**: 정답만 구체적이고 나머지가 추상적이면 불합격. 정답이 "눈에 띄게" 다르면 안 됩니다.

#### 3-4. 의미 변별력
- 선지 간 의미가 충분히 구별되는가? 유사한 의미의 선지가 2개 이상이면 혼동 가능 → 불합격.
- 반대로, 모든 선지가 완전히 다른 방향이어서 정답이 너무 명확한 것도 좋지 않음.
- **최적**: 2~3개 선지가 지문 주제와 관련되어 고민이 필요하고, 정밀하게 문맥을 분석해야 정답을 구분할 수 있는 수준.

#### 3-5. 정답 유일성 증명 (필수)
- 정답이 아닌 4개 선지 각각에 대해, "왜 이것은 오답인가"를 실제로 검증하세요.
- **하나라도 논리적으로 정답이 될 수 있으면 즉시 불합격입니다.**

### 4. 지시문 명확성
- 지시문만 읽고 무엇을 해야 하는지 명확한가?
- 지문 속 용어를 모호하게 사용하고 있지 않은가?

### 5. 해설 품질
- 정답의 근거를 구체적으로 설명하는가?
- "지문을 보면 알 수 있다" 같은 막연한 설명은 불합격.

### 6. 지문 길이 및 품질
- 지문이 너무 길지 않은가? (약 15문장, 250단어 이내 권장)
- 지문이 원본에 없는 새로운 영어 내용을 포함하고 있지 않은가?

### 7. 문제 간 중복
- 여러 문제가 완전히 같은 지문을 사용하고 있는가?
- 같은 형태/패턴의 문제가 반복되는가?

### 8. 서술형 적절성 (서술형만)
- **밑줄/빈칸 일치**: 지시문에서 "밑줄 친 부분"이라고 했으면 passage에 `<u>...</u>` 태그가 있는가? "빈칸"이라고 했으면 ______ 이 있는가?
- **답안 도출 가능성**: 지문만 읽고 모범답안을 도출할 수 있는가? 지문에 단서가 없으면 불합격.
- **답안 명확성**: 모범답안이 하나로 특정되는가? "어떤 표현이든 맞을 수 있는" 문제는 불합격.
- **채점 기준 명확성**: 부분 점수가 가능한 문제인가? 답안이 너무 길거나 주관적인 판단이 필요한 문제는 감점.
- **난이도 적절성**: 고1 수준의 학생이 풀 수 있는 수준인가? 너무 쉽거나 너무 어렵지 않은가?
- **어형 변환**: 괄호 안 단어를 변환하는 문제라면, 변환 근거가 문법적으로 명확한가? (단순 암기가 아닌 문법 적용이어야 함)
- **조건 영작**: 조건이 명확하고, 조건을 모두 충족하는 답이 유일한가?

## verdict 기준
- **pass**: 위 기준을 모두 충족. score 7 이상.
- **fail**: 위 기준 중 하나라도 심각하게 위반. 특히 아래 항목은 반드시 fail:
  - 정답 정확성 불합격
  - 선지 품질(3-2, 3-5) 불합격 — 오답이 뻔하거나, 정답이 유일하지 않으면 반드시 fail
  - 서술형에서 밑줄/빈칸이 지시문과 불일치

## 출력 형식 (JSON만)
```json
{
  "reviews": [
    {
      "question_no": 1,
      "verdict": "pass",
      "score": 9,
      "issues": [],
      "fix_suggestion": ""
    },
    {
      "question_no": 2,
      "verdict": "fail",
      "score": 4,
      "issues": ["정답이 불명확: 선지 2번과 4번 모두 문맥상 가능", "오답 ①⑤가 지문 주제와 무관하여 매력도 부족", "해설이 막연함"],
      "fix_suggestion": "빈칸 위치를 핵심 논지 문장으로 바꾸고, 오답을 지문 주제와 관련된 표현으로 교체하여 매력도를 높이세요"
    }
  ],
  "overall_comment": "전체적인 검수 의견"
}
```
**JSON만 출력하세요.**"""

_FIX_SYSTEM_PROMPT = """당신은 대한민국 고등학교 영어 시험 출제위원급 전문가입니다.
아래에 검수에서 불합격된 문제와 검수 피드백, 원본 지문이 주어집니다.
검수 피드백을 반영하여 문제를 수정하세요.

## 핵심 규칙
1. 검수에서 지적된 문제점을 반드시 해결하세요.
2. 지문은 원본에서 발췌하되, 문제에 필요한 부분만 사용하세요 (전체 복사 금지).
3. 정답이 명확하고 유일해야 합니다.
4. 지시문은 그것만 읽고도 무엇을 해야 하는지 알 수 있어야 합니다.
5. 원본 지문의 영어 내용을 그대로 사용하세요. 새로운 영어 내용을 만들지 마세요.
6. [해석](translation)도 반드시 포함하세요.

## 선지 수정 가이드 (객관식)
- 오답 매력도가 부족하다는 피드백을 받았으면, 오답을 지문 주제와 관련된 그럴듯한 표현으로 교체하세요.
- 정답 유일성이 의심되면, 빈칸/밑줄 위치를 변경하거나 선지를 재구성하여 정답이 명확히 하나가 되게 하세요.
- 선지 길이가 불균형하면 모든 선지의 길이를 비슷하게 맞추세요.

## 밑줄 표시 규칙
- 지문에서 밑줄이 필요한 부분은 반드시 `<u>밑줄 칠 부분</u>` HTML 태그로 표시하세요.
- "밑줄 친 부분"이라고 지시문에 썼으면 passage에 `<u>` 태그가 반드시 있어야 합니다.

## 서술형 수정 가이드
- 답안이 모호하다는 피드백을 받았으면, 답안이 유일하게 특정되도록 문제를 재설계하세요.
- 채점 기준이 불명확하다면, 조건을 더 구체적으로 제시하거나 답안 형식을 단순화하세요.
"""


# ── 오답 개선 에이전트 ────────────────────────────────────────────────────────

_DISTRACTOR_CREATE_PROMPT = """당신은 대한민국 수능·모의고사 영어 영역 출제위원급 전문가입니다.
AI가 생성한 객관식 문제에 대해 **매력적인 5지선다 선지를 새로 만드는** 것이 당신의 역할입니다.

각 문제에 대해 정답(answer_text)과 지문(passage), 문제 지시문(question_text), 유형(sub_type)이 주어집니다.
당신은 정답을 포함한 5개 선지(①~⑤)를 만들어야 합니다.

## 핵심 원칙
1. **answer_text를 반드시 선지 중 하나로 포함하세요.** 정답 위치(①~⑤ 중 어디에 넣을지)는 무작위로 정하세요.
2. **오답 4개는 매력적이지만 확실히 틀린** 것이어야 합니다.
3. **정답 텍스트를 변경하지 마세요.** answer_text를 그대로 선지에 넣으세요.

## 좋은 오답의 조건
1. **주제 관련성**: 오답은 지문의 주제·소재와 관련된 단어/표현이어야 합니다. 지문이 "환경"에 관한 글이면 오답도 환경 관련 어휘여야 합니다.
2. **매력도**: 고1 수준의 학생이 고민할 만큼 그럴듯해야 합니다. 지문을 대충 읽으면 정답으로 착각할 수 있는 수준.
3. **명확한 오답**: 그럴듯하지만 지문을 정확히 이해하면 확실히 틀린 답이어야 합니다.
4. **형식적 균형**: 모든 선지(정답 포함)의 길이·품사·형태가 유사해야 합니다. 정답만 길거나 짧으면 안 됩니다.
5. **의미 변별**: 선지 간 의미가 충분히 구별되어야 합니다. 유사한 뜻의 선지가 2개 이상이면 안 됩니다.
6. **자연스러운 영어**: 문법적으로 자연스러운 영어여야 합니다. 부자연스러운 표현은 즉시 소거됩니다.

## 유형별 선지 생성 전략
- **빈칸추론**: 빈칸 전후 문맥과 의미적으로 관련되지만, 글 전체 논지에는 맞지 않는 표현. 모든 선지가 빈칸에 문법적으로 들어갈 수 있어야 함.
- **어법**: 정답과 형태가 유사하지만 문법적으로 다른 선택지 (예: 능동/수동, 분사/동명사, 관계대명사 종류)
- **어휘**: 정답 단어와 철자나 발음이 유사하거나, 같은 의미장에 속하지만 문맥상 부적절한 단어
- **주제/요지/제목**: 글의 부분적 내용과 관련되지만 전체 주제와는 다른 선지. 한국어 또는 영어로 자연스러운 문장.
- **내용일치/불일치**: 글의 다른 부분에서 언급된 사실과 미묘하게 다른 선지
- **순서배열**: 가능한 순서 조합 중 5개 (정답 포함). 예: ① (A)-(B)-(C) ② (A)-(C)-(B) ...
- **문장삽입**: 가능한 삽입 위치 5개 중 정답 포함
- **지칭추론/함축의미추론**: 지문에서 관련된 다른 의미나 대상을 오답으로 활용

## 출력 형식 (JSON)
```json
{
  "distractors": [
    {
      "question_no": 1,
      "choices": ["① ...", "② ...", "③ ...", "④ ...", "⑤ ..."],
      "answer": 3,
      "reasoning": "정답을 3번에 배치. 오답 근거 간략 설명"
    }
  ]
}
```
- choices 배열은 반드시 5개, ①~⑤ 접두사 포함
- answer는 정답이 위치한 번호 (1~5 정수)
- **JSON만 출력하세요.**"""


def _generate_distractors(
    questions: List[Dict[str, Any]],
    passage_text: str,
    api_key: str,
    model_name: str,
) -> List[Dict[str, Any]]:
    """객관식 문제에 대해 매력적인 5지선다 선지를 생성한다.

    Stage 2에서 정답(answer_text)만 생성된 문제에 대해,
    정답을 포함한 5개 선지를 만들고 answer(정수)를 설정한다.
    실패 시 answer_text를 ③에 넣은 기본 선지를 만든다.
    """
    if not questions:
        return questions

    # ── 어법/어휘 문제: 선지는 항상 ①②③④⑤ (LLM 호출 불필요) ──
    _ABCDE_POS = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5}
    _CIRCLE_NUM = {'①': 1, '②': 2, '③': 3, '④': 4, '⑤': 5}
    for q in questions:
        if q.get("choices") or not q.get("answer_text"):
            continue
        sub = q.get("sub_type", "")
        if sub not in ("어법", "어휘"):
            continue
        at = q.get("answer_text", "").strip()
        ans_no = None
        # Pattern 1: (A)~(E)
        m = re.match(r'\(([A-E])\)', at)
        if m:
            ans_no = _ABCDE_POS[m.group(1)]
        # Pattern 2: (1)~(5)
        if not ans_no:
            m = re.match(r'\((\d)\)', at)
            if m and 1 <= int(m.group(1)) <= 5:
                ans_no = int(m.group(1))
        # Pattern 3: ①~⑤
        if not ans_no:
            for sym, num in _CIRCLE_NUM.items():
                if at.startswith(sym):
                    ans_no = num
                    break
        # Pattern 4: "3번" or "3."
        if not ans_no:
            m = re.match(r'(\d)\s*[번.]', at)
            if m and 1 <= int(m.group(1)) <= 5:
                ans_no = int(m.group(1))
        # Pattern 5: bare digit at start
        if not ans_no:
            m = re.match(r'^(\d)\s', at)
            if m and 1 <= int(m.group(1)) <= 5:
                ans_no = int(m.group(1))
        # Fallback: default to 3
        if not ans_no:
            ans_no = 3
        q["choices"] = ["①", "②", "③", "④", "⑤"]
        q["answer"] = ans_no
        q["distractor_created"] = True

    # 객관식만 필터 (answer_text가 있고 choices가 아직 없는 문제)
    mc_questions = [q for q in questions if q.get("answer_text") and not q.get("choices")]
    if not mc_questions:
        return questions

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    # 문제 정보 전달
    q_info = []
    for q in mc_questions:
        q_info.append({
            "question_no": q.get("question_no"),
            "sub_type": q.get("sub_type"),
            "question_text": q.get("question_text"),
            "passage": q.get("passage", "")[:1500],
            "answer_text": q.get("answer_text"),
        })

    user_prompt = f"""## 선지 생성 대상 문제
{json.dumps(q_info, ensure_ascii=False, indent=2)}

---

## 원본 지문
{passage_text[:5000]}

---

위 {len(mc_questions)}개 객관식 문제에 대해 각각 5지선다 선지를 만들어 주세요.
- **answer_text를 반드시 선지 중 하나로 포함**하고, 그 위치를 answer에 기록하세요.
- 정답 위치는 문제마다 다르게 (무작위로) 배치하세요. 모든 문제가 같은 번호에 정답이면 안 됩니다.
- 오답 4개는 매력적이지만 확실히 틀려야 합니다."""

    try:
        response = model.generate_content(
            [_DISTRACTOR_CREATE_PROMPT, user_prompt],
            generation_config=genai.types.GenerationConfig(
                temperature=0.7,
                max_output_tokens=8192,
            ),
        )
        text = response.text.strip()
        text = re.sub(r"```(?:json)?\s*", "", text).strip()
        text = re.sub(r"```\s*$", "", text).strip()
        result = json.loads(text)

        distractors_list = result.get("distractors", [])
        dist_map = {item["question_no"]: item for item in distractors_list}

        # 생성된 선지 적용
        for q in questions:
            qno = q.get("question_no")
            if qno in dist_map and q.get("answer_text") and not q.get("choices"):
                dist = dist_map[qno]
                new_choices = dist.get("choices", [])
                new_answer = dist.get("answer")

                if new_choices and len(new_choices) == 5 and isinstance(new_answer, int) and 1 <= new_answer <= 5:
                    # 정답 선지가 answer_text를 포함하는지 확인 (안전장치)
                    answer_choice = new_choices[new_answer - 1]
                    answer_clean = re.sub(r"^[①②③④⑤]\s*", "", answer_choice).strip()
                    original_answer = q["answer_text"].strip()

                    # 정답이 선지에 올바르게 포함되었으면 적용
                    if answer_clean == original_answer or original_answer in answer_clean:
                        q["choices"] = new_choices
                        q["answer"] = new_answer
                        q["distractor_created"] = True
                    else:
                        # 정답이 변경됨 → 폴백
                        _apply_fallback_choices(q)
                else:
                    _apply_fallback_choices(q)

        # 선지가 아직 없는 문제에 대해 폴백 적용
        for q in questions:
            if q.get("answer_text") and not q.get("choices"):
                _apply_fallback_choices(q)

        return questions
    except Exception:
        # 실패 시 모든 MC 문제에 폴백 적용
        for q in questions:
            if q.get("answer_text") and not q.get("choices"):
                _apply_fallback_choices(q)
        return questions


def _apply_fallback_choices(q: Dict[str, Any]) -> None:
    """선지 생성 실패 시 answer_text를 ③에 넣은 기본 선지를 만든다."""
    answer_text = q.get("answer_text", "정답")
    q["choices"] = [
        f"① (오답 자동생성 실패)",
        f"② (오답 자동생성 실패)",
        f"③ {answer_text}",
        f"④ (오답 자동생성 실패)",
        f"⑤ (오답 자동생성 실패)",
    ]
    q["answer"] = 3
    q["distractor_created"] = False


# ── 학생 풀이 시뮬레이션 에이전트 ──────────────────────────────────────────────

_STUDENT_SIM_PROMPT = """당신은 대한민국 고등학교 1학년 학생입니다.
영어 시험 문제를 풀어야 합니다. **정답은 모릅니다.**

## 풀이 규칙
1. 각 문제를 지문과 선지(또는 지시문)만 보고 직접 풀어보세요.
2. 정답을 고른 후, 확신도(1~10)를 매기세요.
3. 다른 답도 가능한지 반드시 검토하세요.
4. 풀이 과정을 간단히 적어주세요.

## 주의사항
- **정답이 맞다고 가정하지 마세요.** 진짜 학생처럼 예리하게 판단하세요.
- 선지 중 2개 이상이 답이 될 수 있으면 반드시 지적하세요.
- 문제가 모호해서 풀 수 없다면 그 이유를 설명하세요.
- 빈칸이 있다고 했는데 지문에 없으면 지적하세요.
- 밑줄이 있다고 했는데 지문에 없으면 지적하세요.
- 서술형은 답을 직접 쓰고, 다른 답도 가능한지 평가하세요.

## 출력 형식 (JSON)
```json
{
  "solutions": [
    {
      "question_no": 1,
      "my_answer": "3",
      "confidence": 9,
      "reasoning": "풀이 과정 설명",
      "alternative_possible": false,
      "alternative_answers": [],
      "issues": []
    },
    {
      "question_no": 2,
      "my_answer": "2",
      "confidence": 5,
      "reasoning": "풀이 과정 설명",
      "alternative_possible": true,
      "alternative_answers": ["4번도 문맥상 가능"],
      "issues": ["선지 2번과 4번이 의미적으로 유사하여 구분이 어려움"]
    }
  ],
  "overall_difficulty": "적절 / 쉬움 / 어려움",
  "overall_comment": "전체적인 체감 난이도와 문제 품질에 대한 의견"
}
```
**JSON만 출력하세요.**"""


def _simulate_student(
    questions: List[Dict[str, Any]],
    question_type: str,
    api_key: str,
    model_name: str,
) -> Optional[Dict[str, Any]]:
    """학생 관점에서 문제를 풀어보고, 정답 불일치·모호성을 탐지한다.

    Returns:
        {"solutions": [...], "overall_difficulty": "...", "overall_comment": "..."} or None
    """
    if not questions:
        return None

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    # 정답을 제거한 문제를 전달 — 학생은 정답을 모르는 상태
    questions_for_student = []
    for q in questions:
        student_q = {
            "question_no": q.get("question_no"),
            "question_text": q.get("question_text"),
            "passage": q.get("passage", ""),
        }
        if q.get("choices"):
            student_q["choices"] = q["choices"]
        # answer, explanation, translation 은 의도적으로 제외
        questions_for_student.append(student_q)

    user_prompt = f"""## 시험 문제 ({question_type})
{json.dumps(questions_for_student, ensure_ascii=False, indent=2)}

---

위 {len(questions)}개 문제를 풀어보세요. 정답은 주어지지 않습니다.
각 문제를 직접 풀고, 확신도를 매기고, 다른 답이 가능한지 검토하세요."""

    try:
        response = model.generate_content(
            [_STUDENT_SIM_PROMPT, user_prompt],
            generation_config=genai.types.GenerationConfig(
                temperature=0.3,
                max_output_tokens=8192,
            ),
        )
        text = response.text.strip()
        text = re.sub(r"```(?:json)?\s*", "", text).strip()
        text = re.sub(r"```\s*$", "", text).strip()
        return json.loads(text)
    except Exception:
        return None


def _check_simulation_results(
    questions: List[Dict[str, Any]],
    sim_result: Dict[str, Any],
    question_type: str,
) -> List[Dict[str, Any]]:
    """시뮬레이션 결과를 문제에 반영하고, 불일치 문제를 표시한다.

    각 문제에 simulation_* 필드를 추가한다.
    """
    if not sim_result or "solutions" not in sim_result:
        return questions

    sol_map = {s.get("question_no"): s for s in sim_result.get("solutions", [])}

    for q in questions:
        qno = q.get("question_no")
        sol = sol_map.get(qno)
        if not sol:
            continue

        q["sim_confidence"] = sol.get("confidence", 0)
        q["sim_issues"] = sol.get("issues", [])
        q["sim_alternative"] = sol.get("alternative_possible", False)

        # 정답 불일치 검사
        my_answer = str(sol.get("my_answer", "")).strip()
        intended_answer = str(q.get("answer", "")).strip()

        if question_type == "객관식":
            # 번호 비교
            my_num = re.sub(r"[^0-9]", "", my_answer)
            intended_num = re.sub(r"[^0-9]", "", intended_answer)
            if my_num and intended_num and my_num != intended_num:
                q["sim_mismatch"] = True
                q["sim_student_answer"] = my_answer
                q["sim_reasoning"] = sol.get("reasoning", "")
            else:
                q["sim_mismatch"] = False
        else:
            # 서술형 — 학생 답안 기록 (정확한 매칭은 어려우니 기록만)
            q["sim_student_answer"] = my_answer
            q["sim_mismatch"] = False  # 서술형은 텍스트 비교가 부정확

    return questions


def _review_questions(
    questions: List[Dict[str, Any]],
    passage_text: str,
    question_type: str,
    api_key: str,
    model_name: str,
) -> Optional[Dict[str, Any]]:
    """생성된 문제를 검수하여 합격/불합격을 판정한다.

    Returns:
        {"reviews": [...], "overall_comment": "..."} or None
    """
    if not questions:
        return None
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    questions_json = json.dumps(questions, ensure_ascii=False, indent=2)

    # 시뮬레이션 결과가 문제에 첨부되어 있으면 검수 시 함께 전달
    sim_warnings = []
    for q in questions:
        qno = q.get("question_no", 0)
        if q.get("sim_mismatch"):
            sim_warnings.append(
                f"⚠️ {qno}번: 학생 시뮬레이션 정답 불일치! "
                f"의도된 정답={q.get('answer')}, 학생 답={q.get('sim_student_answer')}. "
                f"이유: {q.get('sim_reasoning', '(없음)')}"
            )
        if q.get("sim_alternative"):
            sim_warnings.append(
                f"⚠️ {qno}번: 학생이 복수 정답 가능성을 지적. issues={q.get('sim_issues', [])}"
            )

    sim_section = ""
    if sim_warnings:
        sim_section = "\n\n## ⚠️ 학생 시뮬레이션 결과 (반드시 고려할 것)\n" + "\n".join(sim_warnings)
        sim_section += "\n\n위 시뮬레이션에서 정답 불일치 또는 복수 정답 가능성이 발견된 문제는 특별히 엄격하게 검수하세요."

    user_prompt = f"""## 검수 대상 문제들
{questions_json}

---

## 원본 지문
{passage_text[:6000]}
{sim_section}

---

위 {len(questions)}개 문제를 검수하세요. question_type: {question_type}
각 문제의 정답 정확성, 지문 가공, 선지 품질(오답 매력도·정답 유일성), 지시문 명확성, 해설 품질을 확인하세요.
특히 객관식은 선지 품질을 엄격히, 서술형은 밑줄/빈칸 일치와 답안 명확성을 집중 검수하세요."""

    try:
        response = model.generate_content(
            [_REVIEW_SYSTEM_PROMPT, user_prompt],
            generation_config=genai.types.GenerationConfig(
                temperature=0.2,
                max_output_tokens=8192,
            ),
        )
        text = response.text.strip()
        text = re.sub(r"```(?:json)?\s*", "", text).strip()
        text = re.sub(r"```\s*$", "", text).strip()
        return json.loads(text)
    except Exception:
        return None


def _fix_failed_questions(
    failed_with_reviews: List[tuple],
    passage_text: str,
    target_type: str,
    question_type: str,
    api_key: str,
    model_name: str,
    source_label: str = "본문",
) -> List[Dict[str, Any]]:
    """불합격된 문제를 검수 피드백 반영하여 재생성한다."""
    if not failed_with_reviews:
        return []
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    fix_items = []
    for q, review in failed_with_reviews:
        fix_items.append({
            "원래_문제": q,
            "검수_결과": {
                "불합격_사유": review.get("issues", []),
                "개선_제안": review.get("fix_suggestion", ""),
                "점수": review.get("score", 0),
            }
        })

    if question_type == "객관식":
        output_hint = """각 문제를 아래 JSON 형식으로 출력하세요:
[
  {
    "question_no": 1,
    "sub_type": "유형",
    "question_text": "수정된 지시문",
    "passage": "수정된 지문 (필요한 부분만 발췌, 전체 복사 금지)",
    "choices": ["① ...", "② ...", "③ ...", "④ ...", "⑤ ..."],
    "answer": 정답번호,
    "translation": "수정된 해석",
    "explanation": "수정된 해설"
  }
]"""
    else:
        output_hint = """각 문제를 아래 JSON 형식으로 출력하세요:
[
  {
    "question_no": 1,
    "sub_type": "서술형",
    "question_text": "수정된 지시문",
    "passage": "수정된 지문 (필요한 부분만 발췌, 전체 복사 금지)",
    "answer": "수정된 모범답안 (하나의 문자열)",
    "translation": "수정된 해석",
    "explanation": "수정된 해설"
  }
]"""

    user_prompt = f"""## 불합격된 문제 및 검수 피드백
{json.dumps(fix_items, ensure_ascii=False, indent=2)}

---

## 원본 {source_label}
{passage_text[:6000]}

---

위 문제들의 검수 피드백을 반영하여 수정하세요.
- 불합격 사유를 반드시 해결하세요.
- 원본 {source_label}의 영어 내용을 그대로 활용하세요.
- 지문은 원본에서 필요한 1~2문단만 발췌하세요.

{output_hint}
**JSON 배열만 출력하세요.**"""

    try:
        response = model.generate_content(
            [_FIX_SYSTEM_PROMPT, user_prompt],
            generation_config=genai.types.GenerationConfig(
                temperature=0.5,
                max_output_tokens=16384,
            ),
        )
        fixed = _parse_llm_response(response.text)
        return fixed if fixed else []
    except Exception:
        return []


def _review_and_retry(
    questions: List[Dict[str, Any]],
    passage_text: str,
    target_type: str,
    question_type: str,
    api_key: str,
    model_name: str,
    source_label: str = "본문",
    max_retries: int = 1,
) -> tuple:
    """검수하고 불합격 문제를 재생성한다.

    Returns:
        (final_questions, review_summary_dict)
    """
    if not questions:
        return questions, None

    # ── 1차 검수 ──
    review_result = _review_questions(
        questions, passage_text, question_type, api_key, model_name
    )
    if not review_result or "reviews" not in review_result:
        # 검수 실패 → 원본 그대로 반환
        return questions, {
            "status": "review_skipped",
            "reason": "검수 API 호출 실패",
            "total_generated": len(questions),
            "final_count": len(questions),
            "passed_initial": 0,
            "failed_initial": 0,
            "retried": False,
            "retry_count": 0,
        }

    reviews = review_result.get("reviews", [])
    review_map = {r.get("question_no"): r for r in reviews}

    passed = []
    failed_pairs = []
    for q in questions:
        qno = q.get("question_no", 0)
        rev = review_map.get(qno, {})
        q["review_score"] = rev.get("score", 0)
        q["review_verdict"] = rev.get("verdict", "unknown")
        q["review_issues"] = rev.get("issues", [])

        if rev.get("verdict") == "pass":
            passed.append(q)
        else:
            failed_pairs.append((q, rev))

    # ── 복수정답 가능 문제 강제 불합격 → 재생성 대상 ──
    sim_alt_to_fail = [q for q in passed if q.get("sim_alternative")]
    if sim_alt_to_fail:
        passed = [q for q in passed if not q.get("sim_alternative")]
        for q in sim_alt_to_fail:
            q["review_verdict"] = "fail"
            q["review_issues"] = q.get("review_issues", []) + [
                "시뮬레이션에서 복수정답 가능성 감지 — 자동 재생성 대상"
            ]
            failed_pairs.append((q, {
                "issues": q.get("review_issues", []),
                "fix_suggestion": "정답이 유일하도록 선지/지문을 수정하세요. 복수정답 가능성을 완전히 제거해야 합니다.",
                "score": q.get("review_score", 5),
            }))

    # ── 재시도 ──
    retry_count = 0
    while failed_pairs and retry_count < max_retries:
        retry_count += 1
        fixed = _fix_failed_questions(
            failed_pairs, passage_text, target_type, question_type,
            api_key, model_name, source_label,
        )
        if not fixed:
            break  # 재생성 실패

        # 재생성된 문제 번호 매핑
        for i, fq in enumerate(fixed):
            if i < len(failed_pairs):
                fq["question_no"] = failed_pairs[i][0].get("question_no", i + 1)

        # 재생성 문제 2차 검수
        re_review = _review_questions(
            fixed, passage_text, question_type, api_key, model_name
        )
        if re_review and "reviews" in re_review:
            re_map = {r.get("question_no"): r for r in re_review.get("reviews", [])}
            new_failed = []
            for fq in fixed:
                qno = fq.get("question_no", 0)
                rev = re_map.get(qno, {})
                fq["review_score"] = rev.get("score", 0)
                fq["review_verdict"] = rev.get("verdict", "unknown")
                fq["review_issues"] = rev.get("issues", [])
                fq["review_retry"] = retry_count

                if rev.get("verdict") == "pass":
                    passed.append(fq)
                else:
                    new_failed.append((fq, rev))
            failed_pairs = new_failed
        else:
            # 2차 검수 실패 → 재생성 문제 전부 수용
            for fq in fixed:
                fq["review_verdict"] = "accepted_without_review"
                fq["review_retry"] = retry_count
            passed.extend(fixed)
            failed_pairs = []

    # 최종 결과 정리
    # 번호 재할당
    for i, q in enumerate(passed):
        q["question_no"] = i + 1

    review_summary = {
        "total_generated": len(questions),
        "passed_initial": sum(1 for q in questions if q.get("review_verdict") == "pass"),
        "failed_initial": len([fp for fp in review_result.get("reviews", []) if fp.get("verdict") != "pass"]),
        "retried": retry_count > 0,
        "retry_count": retry_count,
        "final_count": len(passed),
        "overall_comment": review_result.get("overall_comment", ""),
    }

    return passed, review_summary


def _get_correct_answer_text(
    sub_type: Optional[str],
    answer: Any,
    choices: Any,
) -> Optional[str]:
    """정답 번호와 선지 목록에서 정답 텍스트를 추출한다."""
    if not choices or not answer:
        return None
    if isinstance(choices, str):
        try:
            choices = json.loads(choices)
        except Exception:
            return None
    if not isinstance(choices, list) or not choices:
        return None
    # answer → 0-based index
    idx = None
    if isinstance(answer, int):
        idx = answer - 1
    elif isinstance(answer, str) and answer.strip().isdigit():
        idx = int(answer.strip()) - 1
    if idx is not None and 0 <= idx < len(choices):
        return choices[idx]
    return None


def _restore_passage(
    raw: str,
    sub_type: Optional[str] = None,
    answer: Any = None,
    choices: Any = None,
) -> str:
    """지문에서 기존 문제의 가공 흔적을 제거하고, 정답 데이터를 이용해
    원래 텍스트에 가깝게 복원한다.

    복원 대상:
      - 빈칸추론: 빈칸(___)에 정답 텍스트 삽입
      - 요약문완성: (A)/(B) 빈칸에 정답 쌍 삽입
      - 어휘/지칭추론: (a)(b)(c)(d)(e) 마커 제거, 단어 유지
      - 어법/무관문장: ①②③④⑤ 마커 제거
      - 순서배열: (A)(B)(C) 헤더 제거
      - 공통: [점], 각주, 배점 마커 제거
    """
    if not raw:
        return raw
    text = raw

    # 0) 배점 마커 제거: [3점], [2점] 등
    text = re.sub(r'\s*\[\d점\]\s*', ' ', text)

    # 1) 빈칸 복원: 정답 텍스트가 있으면 빈칸에 삽입
    correct_text = _get_correct_answer_text(sub_type, answer, choices)

    if sub_type == '빈칸추론' and correct_text:
        # 1a) 명시적 빈칸 (___ 패턴)이 있으면 정답으로 교체
        if re.search(r'_{3,}', text):
            text = re.sub(r'\(?\s*[A-B]?\s*\)?\s*_{3,}', correct_text, text, count=1)
        else:
            # 1b) 빈칸이 PDF 파싱 시 사라진 경우: 여러 패턴 시도
            inserted = False
            # 패턴1: "word . Next" (공백+마침표) → 빈칸 텍스트 삽입
            if re.search(r'\w\s+\.(?:\s|\n)', text):
                text = re.sub(r'(\w)\s+(\.(?:\s|\n))', r'\1 ' + correct_text.replace('\\', '\\\\') + r' \2', text, count=1)
                inserted = correct_text in text
            # 패턴2: "word, ." (쉼표 뒤 공백+마침표)
            if not inserted and re.search(r',\s+\.', text):
                text = re.sub(r'(,)\s+(\.)', r'\1 ' + correct_text.replace('\\', '\\\\') + r'\2', text, count=1)
                inserted = correct_text in text
            # 패턴3: 문장 끝에 ". " 대신 ", ." 이 있는 경우
            if not inserted and text.rstrip().endswith('.') and re.search(r',\s*\.\s*$', text.rstrip()):
                text = re.sub(r'(,)\s*(\.\s*)$', r'\1 ' + correct_text.replace('\\', '\\\\') + r'\2', text.rstrip())
                inserted = correct_text in text
            # 최종 폴백: 정답 텍스트를 삽입하지 못한 경우 메타 정보 추가
            if not inserted and correct_text not in text:
                text += f'\n[※ 원래 빈칸 정답: {correct_text}]'

    elif sub_type == '요약문완성' and correct_text:
        # 요약문완성: 선지가 "word1 … word2" 형태
        parts = re.split(r'\s*[…·]\s*', correct_text)
        if len(parts) >= 2:
            # (A) ___ 와 (B) ___ 각각 교체
            text = re.sub(r'\(\s*A\s*\)\s*_{3,}', parts[0].strip(), text, count=1)
            text = re.sub(r'\(\s*B\s*\)\s*_{3,}', parts[1].strip(), text, count=1)
        elif re.search(r'_{3,}', text):
            text = re.sub(r'_{3,}', correct_text, text, count=1)
    elif re.search(r'_{3,}', text):
        # 다른 유형이지만 빈칸이 있는 경우: 정답으로 교체 시도
        if correct_text:
            text = re.sub(r'_{3,}', correct_text, text, count=1)
        else:
            text = re.sub(r'_{3,}', '[ ... ]', text)

    # 2) 어휘/지칭추론 마커 제거: (a)word → word
    text = re.sub(r'\(([a-e])\)(?=\w)', '', text)
    # ⓐⓑⓒⓓⓔ 마커 제거
    text = re.sub(r'[ⓐⓑⓒⓓⓔ]', '', text)

    # 3) 순서배열 마커 정리: 단독 줄의 (A), (B), (C), (D) 헤더 제거
    text = re.sub(r'^\s*\([A-D]\)\s*$', '', text, flags=re.MULTILINE)
    # 문단 첫머리 (A) (B) (C) 도 제거
    text = re.sub(r'^\s*\([A-D]\)\s*(?=\w)', '', text, flags=re.MULTILINE)

    # 4) 어법/무관문장 위치 마커 제거: ① ② ③ 등
    text = re.sub(r'[①②③④⑤]\s*', '', text)

    # 5) 각주 제거: *word: 한글해설 형태
    text = re.sub(r'\n\s*\*[a-zA-Z]+:.+$', '', text, flags=re.MULTILINE)

    # 6) 여러 줄 공백 정리
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# ── 2단계 생성: 분석 에이전트 프롬프트 ──────────────────────────────────────────

_ANALYSIS_PROMPT_MOCK = """당신은 대한민국 고등학교 영어 시험 출제 분석 전문가입니다.

아래에 모의고사 지문 여러 개와 기출 예시가 주어집니다.
당신의 임무:
1. 모의고사 지문들을 모두 읽고 **{target_type}** 유형의 문제로 변환하기에 가장 적합한 지문 {count}개를 선별하세요.
2. 선별된 각 지문에 대해 1개씩의 문제 출제 전략을 수립하세요.

## ⚠️ 핵심 원칙
- **1지문 = 1문제**: 각 문제는 반드시 서로 다른 지문을 사용해야 합니다.
- **지문 원문 보존**: 각 지문은 이미 원문으로 복원되어 있습니다. 이 영어 내용을 그대로 사용하세요. 새로운 영어 내용을 만들어내지 마세요.
- **변형은 형식만**: 빈칸 만들기, 밑줄 그기, 순서 섯기, 문장 빼기 등 출제 형식적 변형만 합니다.
- **원래 유형과 같은 유형 금지**: 각 지문 옆에 "원래 유형"이 표시되어 있습니다. 원래 빈칸추론이었던 지문으로 빈칸추론 문제를 만들면 안 됩니다. 반드시 다른 유형으로 변환하세요.

## 분석 지침
1. **지문 선별**: 각 지문이 {target_type} 유형으로 변환 가능한지 판단하세요.
   - ⛔ **필수 제외 조건**: "원래 유형"이 "{target_type}"인 지문은 절대 선택하지 마세요! 같은 유형으로 다시 문제를 만들면 안 됩니다.
   - 빈칸추론: 핵심 키워드나 주제문이 명확한 지문
   - 어법: 문법적 포인트가 다양한 지문
   - 어휘: 문맥상 중요한 단어가 많은 지문
   - 순서배열: 논리적 흐름이 있는 3단락 이상의 지문
   - 주제/요지/제목: 중심 내용이 명확한 지문
   - 서술형: 다양한 문법 구조와 핵심 내용이 있는 지문

2. **기출 예시 패턴 분석**: 예시 문제들의 형식만 참고하세요.
   - 지시문 형식, 선지 구성, 난이도

3. **각 지문별 변환 전략**: 지문의 구체적 문장을 인용하며 변형 계획을 세우세요.
4. **지시문 명확성**: 지시문(question_text)만 읽고도 무엇을 해야 하는지 명확해야 합니다. 지문 속 전문 용어('dot', 'gap' 등)를 지시문에 그대로 쓰지 마세요.

## 출력 형식 (JSON)
```json
{{
  "pattern_analysis": "기출 예시에서 발견한 출제 패턴 요약 (2-3문장)",
  "selected_passages": [선별된 지문 번호 리스트, 예: [2, 5, 7]],
  "plans": [
    {{
      "question_no": 1,
      "passage_index": 2,
      "reason": "이 지문을 선택한 이유",
      "source_sentences": "활용할 본문 문장(들)을 원문 그대로 인용",
      "transformation": "이 문장을 어떻게 변형할지 구체적 지시",
      "question_text_plan": "지시문 초안",
      "answer_plan": "예상 정답과 근거",
      "distractor_plan": "오답 선지 구성 방향 (객관식만)"
    }}
  ]
}}
```
**JSON만 출력하세요.**
"""

_ANALYSIS_PROMPT_TEXTBOOK = """당신은 대한민국 고등학교 영어 시험 출제 분석 전문가입니다.

아래에 [교과서 본문]과 [기출 예시]가 주어집니다.
당신의 임무는 이 본문으로 **{target_type}** {question_type} 문제를 {count}개 만들기 위한 **출제 전략**을 분석하는 것입니다.

## 핵심 원칙
- 교과서 본문의 영어 내용을 그대로 활용합니다.
- 문제 유형에 맞게 형식적 변형(빈칸, 밑줄, 순서 나누기 등)만 합니다.
   - **밑줄 표시**: 어법/어휘/우리말 해석 등에서 밑줄이 필요하면 `<u>밑줄 칠 부분</u>` HTML 태그 사용을 반드시 포함하여 변환 전략에 명시하세요.

## ⚠️ 지문 길이 및 다양성 규칙
- **지문은 본문 전체를 복사하지 마세요.** 문제에 필요한 1~2개 문단(약 5~15문장)만 발췌하세요.
- 교과서 본문이 길 경우, "Further Reading" 등 부록 부분은 별도 출제 소재로만 활용하세요.
- **각 문제는 본문의 서로 다른 부분(문단)을 사용해야 합니다.** 예를 들어, 문제 1은 1~2문단, 문제 2는 3~4문단, 문제 3은 5~6문단 또는 Further Reading 등.
- 동일 문단을 두 문제에 사용해야 할 경우, 반드시 **다른 문장을 대상**으로 하고 **문제 형태가 완전히 달라야** 합니다 (예: 하나는 빈칸, 하나는 어법 밑줄).

## 분석 지침
1. **기출 예시 패턴 분석**: 예시 문제들의 공통 패턴(지시문, 선지, 난이도)을 파악하세요.
2. **본문 활용 계획**: 본문에서 문제로 만들기 좋은 구체적 문장을 찾으세요. **각 문제가 본문의 다른 부분을 활용**하도록 계획하세요.
3. **변환 지시**: 각 문제별 변형 방법을 구체적으로 기술하세요.
4. **서술형 난이도**: 서술형은 단순 발췌 금지. 어형변환/추론/조건영작/요약빈칸 등 사고력 필요.
5. **지시문 명확성**: question_text_plan은 그것만 읽고도 무엇을 해야 하는지 명확해야 합니다. 지문 속 전문 용어('dot', 'gap' 등)를 지시문에 그대로 쓰지 말고, "다음 글의 빈칸에 들어갈 말을 쓰시오" 같이 구체적으로 작성하세요.
6. **passage_range**: 각 문제가 본문의 어느 부분(몇 번째 문단)을 사용할지 명시하세요.

## 출력 형식 (JSON)
```json
{{
  "pattern_analysis": "기출 예시 패턴 요약",
  "plans": [
    {{
      "question_no": 1,
      "passage_range": "사용할 문단 범위 (예: '3~4문단' 또는 'Further Reading 1문단')",
      "source_sentences": "활용할 문장 인용",
      "transformation": "변형 방법",
      "question_text_plan": "지시문 초안",
      "answer_plan": "예상 정답",
      "distractor_plan": "오답 구성 (객관식만)"
    }}
  ]
}}
```
**JSON만 출력하세요.**
"""


def _run_analysis_stage(
    passage_text: str,
    examples_text: str,
    target_type: str,
    question_type: str,
    count: int,
    api_key: str,
    model_name: str,
    source_label: str = "본문",
    is_mock: bool = False,
) -> Optional[str]:
    """Stage 1: 본문과 예시를 분석하여 출제 전략을 생성한다."""
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    if is_mock:
        system_prompt = _ANALYSIS_PROMPT_MOCK.format(
            target_type=target_type,
            question_type=question_type,
            count=count,
        )
        user_prompt = f"""## 모의고사 지문 목록
아래 지문들을 모두 읽고, **{target_type}** 유형으로 변환하기에 가장 적합한 지문 {count}개를 선별하세요.

{passage_text}

---

## [{target_type}] 유형 기출 예시
아래 예시의 출제 **형식과 패턴만** 참고하세요.

{examples_text}

---

※ 각 문제는 반드시 서로 다른 지문을 사용하세요 (1지문 = 1문제).
※ 지문의 영어 내용을 그대로 사용하고, 출제 형식적 변형(빈칸/밑줄/순서 나누기 등)만 하세요.
※ 새로운 영어 내용을 만들어내지 마세요."""
    else:
        system_prompt = _ANALYSIS_PROMPT_TEXTBOOK.format(
            target_type=target_type,
            question_type=question_type,
            count=count,
        )
        user_prompt = f"""## {source_label}
{passage_text}

---

## [{target_type}] 유형 기출 예시
아래 예시의 출제 **형식과 패턴만** 분석하세요.

{examples_text}

---

위 {source_label}로 **{target_type}** {question_type} 문제를 **{count}개** 출제하기 위한 전략을 분석하세요.
※ 반드시 위 {source_label}의 구체적 문장을 인용하며 계획하세요.
※ 지문의 영어 내용을 그대로 활용하고 형식적 변형만 하세요."""

    try:
        response = model.generate_content(
            [system_prompt, user_prompt],
            generation_config=genai.types.GenerationConfig(
                temperature=0.4,
                max_output_tokens=16384,
            ),
        )
        return response.text
    except Exception:
        return None


def _run_generation_stage(
    passage_text: str,
    analysis_plan: str,
    target_type: str,
    question_type: str,
    count: int,
    api_key: str,
    model_name: str,
    source_label: str = "본문",
) -> Optional[str]:
    """Stage 2: 분석 계획을 바탕으로 실제 문제를 생성한다."""
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(model_name)

    if question_type == "객관식":
        output_format = '''```json
[
  {
    "question_no": 1,
    "sub_type": "''' + target_type + '''",
    "question_text": "문제 지시문",
    "passage": "가공된 지문 (빈칸/밑줄/순서 등 반영)",
    "answer_text": "정답의 핵심 내용 (선지 번호가 아닌 정답 텍스트 자체)",
    "translation": "지문 전체의 자연스러운 한국어 해석",
    "explanation": "정답 근거를 구체적으로 설명 (아래 해설 작성 규칙 참고)"
  }
]
```'''
    else:
        output_format = '''```json
[
  {
    "question_no": 1,
    "sub_type": "서술형",
    "question_text": "문제 지시문",
    "passage": "가공된 지문",
    "answer": "모범 답안 (반드시 하나의 문자열로, JSON 객체가 아닌 텍스트)",
    "translation": "지문 전체의 자연스러운 한국어 해석",
    "explanation": "정답 근거를 구체적으로 설명 (아래 해설 작성 규칙 참고)"
  }
]
```'''

    system_prompt = f"""당신은 대한민국 고등학교 영어 시험 출제위원급 전문가입니다.

아래에 [출제 전략 분석]과 [원본 {source_label}]이 주어집니다.
분석 결과에 따라 실제 문제를 생성하세요.

## ⚠️ 핵심 원칙
- **지문 원문 보존**: 각 지문은 이미 원문으로 복원되어 있습니다. 이 영어 내용을 그대로 사용하세요. 새로운 영어 내용을 만들어내지 마세요.
- **변형은 형식만**: 빈칸 만들기, 밑줄 그기, 순서 섯기, 문장 빼기 등 출제 형식적 변형만 합니다.
- **1지문 = 1문제**: 각 문제는 반드시 서로 다른 지문(또는 서로 다른 문단)을 사용해야 합니다.

## ⚠️ 지문 길이 규칙 (매우 중요)
- **passage 필드에 원본 전체를 복사하지 마세요!** 문제에 필요한 1~2개 문단(약 5~15문장, 최대 약 250단어)만 발췌하세요.
- 교과서 본문이 여러 문단일 경우, 각 문제는 **서로 다른 문단**을 사용해야 합니다.
- 같은 문단을 두 문제에 쓸 경우, **대상 문장과 문제 형태가 완전히 달라야** 합니다.
- 본문에 "Further Reading" 같은 부록이 있으면, 별도의 문제 소재로 활용할 수 있습니다.

## 핵심 규칙
1. **출제 전략을 정확히 따르세요**: 분석에서 지정한 문장, 변형 방법, 지시문 형식을 따르세요.
2. **지문 가공**: 원본 {source_label}의 영어 내용을 유지하면서 문제 유형에 맞게 형식적 변형하세요.
   - 빈칸추론: 지시된 위치의 단어/구를 빈칸으로 바꿔주세요
   - 어법/어휘: 지시된 위치에 `<u>...</u>` HTML 태그로 밑줄을 표시하세요
   - 순서배열: 원본 단락을 (A)(B)(C)로 나누세요
   - 서술형: 어형 변환, 문맥 추론, 조건 영작 등 사고력이 필요한 문제를 만드세요
3. **지문(passage)에 문제가 반영되어야 합니다**: "빈칸에 들어갈 말"이라고 했으면 지문에 빈칸( ______ )이 있어야 합니다.

## ⚠️ 밑줄 표시 규칙 (매우 중요)
- **지문(passage)에서 밑줄을 표시할 때는 반드시 `<u>밑줄 칠 부분</u>` HTML 태그를 사용하세요.**
- 이 규칙은 객관식(어법/어휘/함축의미추론/지칭추론)과 서술형(우리말 해석 등) 모두에 적용됩니다.
- 예시: `The scientist <u>discovered</u> a new species.`
- "밑줄 친 문장을 해석하시오" 같은 서술형에서도 passage에 반드시 `<u>해당 문장</u>`이 있어야 합니다.
- `<u>` 태그 없이 "밑줄 친 부분"이라고만 지시문에 쓰면 불합격입니다.

## ⚠️ 문제 지시문(question_text) 명확성 규칙
- **문제 지시문만 읽고도 무엇을 해야 하는지 명확해야 합니다.**
- "본문에 따르면, 각 'dot' 옆에 단어를 쓰는 목적은?" 같이 지문 속 용어를 모호하게 인용하면 안 됩니다.
- **대신 지시문을 구체적으로 작성하세요:**
  - 빈칸 채우기: "다음 글의 빈칸 (A)~(C)에 들어갈 알맞은 말을 본문에서 찾아 쓰시오."
  - 어형 변환: "다음 글의 괄호 안에 주어진 단어를 문맥에 맞는 형태로 바꿔 쓰시오."
  - 조건 영작: "다음 <조건>에 맞게 영어 문장을 완성하시오."
  - 요약/서술: "다음 글의 내용을 바탕으로, 아래 요약문의 빈칸을 영어로 완성하시오."
  - 우리말 해석: "다음 글의 밑줄 친 부분을 우리말로 해석하시오."
- **지문 속 전문 용어('dot', 'gap', 'myth' 등)를 지시문에 그대로 쓰지 마세요.** 학생이 지시문만 읽고 무엇을 해야 하는지 알 수 있어야 합니다.
4. **정답 근거**: 정답은 원본 {source_label}에서 확인 가능해야 합니다.
5. **[해석] (translation)**: 지문(passage)의 영어 내용을 자연스러운 한국어로 전체 번역하여 translation 필드에 넣으세요. 빈칸이나 밑줄이 있는 경우 원래 내용을 복원하여 해석합니다.

## ⚠️ 객관식 선지(choices) 관련 중요 규칙
- **객관식 문제에서 선지(①②③④⑤)는 만들지 마세요!** 선지는 별도 전문 에이전트가 만듭니다.
- 대신 **answer_text 필드에 정답의 핵심 내용**을 텍스트로 작성하세요.
  - 빈칸추론: 빈칸에 들어갈 단어/구/절
  - 어법: "①~⑤ 중 정답 번호) 틀린표현 → 올바른표현" 형식 (예: "③ are → is", "(3) are → is"). 반드시 지문의 밑줄 번호를 포함해야 합니다.
  - 어휘: "①~⑤ 중 정답 번호) 문맥에 맞지 않는 어휘 → 올바른 어휘" 형식 (예: "⑤ increase → decrease"). 반드시 지문의 밑줄 번호를 포함해야 합니다.
  - 주제/요지/제목: 정답이 되는 주제문/제목 텍스트
  - 내용일치/불일치: 글의 내용과 일치(또는 불일치)하는 핵심 사실
  - 순서배열: 올바른 순서 (예: "(B)-(A)-(C)")
  - 문장삽입: 삽입 위치와 이유
- **explanation에는 왜 이것이 정답인지만 설명하세요.** 오답 관련 설명은 필요 없습니다.

## 해설(explanation) 작성 규칙
- **"원본 교과서 본문"이나 "지문의 '...' 부분에서 확인 가능" 같은 단순 인용은 금지합니다.**
- **왜 정답인지 구체적으로 설명하세요:**
  - 어법 문제: 어떤 문법 규칙이 적용되는지 명시 (예: "관계대명사 which는 전치사 뒤에서 사용 가능하지만, where는 전치사 뒤에 올 수 없으므로 'in which'가 올바릅니다")
  - 빈칸추론: 빈칸 전후 문맥을 근거로 왜 해당 단어/구가 들어가야 하는지 논리적으로 설명
  - 순서배열: 글의 논리적 흐름(지시어, 연결어, 시간 순서 등)을 근거로 순서를 설명
  - 서술형: 정답의 근거가 되는 문맥을 구체적으로 설명하고, 문법적·의미적 이유를 밝히세요
  - 주제/요지/제목: 글의 핵심 논지를 요약하고 왜 해당 답이 가장 적절한지 설명

## 출력 형식 (JSON 배열만)
{output_format}
**위 JSON 형식만 출력하세요.** 인사말 없이 순수 JSON 배열만 출력합니다.

**{count}개**의 문제를 만드세요."""

    user_prompt = f"""## 출제 전략 분석 결과
{analysis_plan}

---

## 원본 {source_label}
{passage_text}

---

위 분석 전략에 따라 **{target_type}** {question_type} 문제를 **{count}개** 생성하세요.
※ 각 문제는 반드시 서로 다른 지문(또는 서로 다른 문단)을 사용하세요.
※ 지문의 영어 내용을 그대로 사용하고, 출제 형식적 변형(빈칸/밑줄/순서 나누기 등)만 하세요.
※ passage 필드에는 **문제에 필요한 1~2문단만 발췌**하여 가공한 것을 넣으세요. 원본 전체를 복사하지 마세요!
※ 같은 지문으로 여러 문제를 만들 때, 각 문제의 passage는 서로 다른 부분이어야 합니다.
※ 새로운 영어 내용을 만들어내지 마세요."""

    try:
        response = model.generate_content(
            [system_prompt, user_prompt],
            generation_config=genai.types.GenerationConfig(
                temperature=0.8,
                max_output_tokens=65536,
            ),
        )
        return response.text
    except Exception:
        return None


# ── 모의고사 기반 문제 생성 ────────────────────────────────────────────────────

def _generate_mock_based(
    conn: sqlite3.Connection,
    count: int,
    target_type: str,
    exam_year: Optional[int] = None,
    exam_month: Optional[int] = None,
    max_examples: int = 15,
    api_key: str = "",
    model_name: str = "",
    required_question_nos: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """모의고사 지문을 소재로 다른 유형의 문제를 생성한다.
    
    핵심: 여러 지문을 읽고 적합한 것을 선별하여 1지문=1문제로 변환.
    required_question_nos: 반드시 변형 문제를 생성해야 하는 원본 문제 번호 목록.
    """

    # required_question_nos가 있으면 해당 수만큼 최소 생성 보장
    effective_count = count
    if required_question_nos:
        effective_count = max(count, len(required_question_nos))

    # 1. 모의고사에서 다른 유형의 지문을 넉넉히 가져오기 (선별용)
    fetch_count = max(effective_count * 3, 10)  # 3배 이상 가져와서 LLM이 선별
    mock_passages = _fetch_mock_passage(
        conn,
        exclude_sub_type=target_type,
        exam_year=exam_year,
        exam_month=exam_month,
        count=fetch_count,
        required_question_nos=required_question_nos,
    )

    if not mock_passages:
        conn.close()
        return {
            "success": False, "questions": [],
            "error": (
                f"모의고사에서 '{target_type}' 외 유형의 지문을 찾을 수 없습니다.\n"
                f"모의고사 데이터를 먼저 추가해 주세요."
            ),
        }

    # 2. 같은 유형(target_type) 기출 예시 가져오기 (스타일 참고용)
    examples = _fetch_example_questions(
        conn,
        sub_type=target_type,
        source_type=None,     # 출판사+모의고사 모두에서
        question_type="객관식",
        max_examples=max_examples,
    )
    examples = _filter_valid_examples(examples, "객관식")
    conn.close()

    # 3. 지문 텍스트 구성 (정답으로 원문 복원 + 원래 유형 표시)
    passages_block = ""
    for idx, mp in enumerate(mock_passages, 1):
        orig_type = mp.get("sub_type_pred", "알 수 없음")
        src_info = f"{mp.get('exam_year', '?')}년 {mp.get('exam_month', '?')}월 {mp.get('question_no', '?')}번"
        ptext = _restore_passage(
            (mp["passage_text"] or "")[:2000],
            sub_type=mp.get("sub_type_pred"),
            answer=mp.get("answer"),
            choices=mp.get("choices"),
        )
        passages_block += (
            f"\n### 지문 {idx} (출처: {src_info}, 원래 유형: {orig_type})\n"
            f"{ptext}\n"
        )

    # 3.5. required_question_nos가 있으면 필수 지문 안내 추가
    required_nos_set = set(required_question_nos) if required_question_nos else set()
    if required_nos_set:
        required_src_labels = []
        for mp in mock_passages:
            qno = mp.get("question_no")
            if qno in required_nos_set:
                required_src_labels.append(f"{mp.get('exam_year','?')}년 {mp.get('exam_month','?')}월 {qno}번")
        passages_block += (
            f"\n\n## ★ 필수 지문 (반드시 이 지문들로 문제를 만드세요)\n"
            f"다음 지문들은 반드시 각각 1문제씩 변형 문제를 생성해야 합니다: "
            f"{', '.join(required_src_labels)}\n"
        )

    # 4. 기출 예시 텍스트
    if examples:
        examples_text = "\n\n".join(
            _format_example(ex, i + 1)
            for i, ex in enumerate(examples)
        )
    else:
        examples_text = "(기출 예시 없음 — 일반적인 수능/모의고사 출제 스타일로 만드세요)"

    # 5. Stage 1 — 분석 에이전트: 지문 선별 + 출제 전략 수립
    analysis_plan = _run_analysis_stage(
        passage_text=passages_block,
        examples_text=examples_text,
        target_type=target_type,
        question_type="객관식",
        count=effective_count,
        api_key=api_key,
        model_name=model_name,
        source_label="모의고사 지문",
        is_mock=True,
    )
    if not analysis_plan:
        return {"success": False, "questions": [],
                "error": "Stage 1 분석 에이전트 호출 실패. API 키를 확인하세요."}

    # 6. Stage 2 — 생성: 분석 전략 기반 문제 생성
    try:
        raw = _run_generation_stage(
            passage_text=passages_block,
            analysis_plan=analysis_plan,
            target_type=target_type,
            question_type="객관식",
            count=effective_count,
            api_key=api_key,
            model_name=model_name,
            source_label="모의고사 지문",
        )
        if not raw:
            return {"success": False, "questions": [],
                    "error": "Stage 2 문제 생성 실패. 다시 시도해 주세요."}
    except Exception as exc:
        return {"success": False, "questions": [],
                "error": f"Gemini API 오류: {exc}"}

    # 7. 응답 파싱
    generated = _parse_llm_response(raw)
    if not generated:
        return {"success": False, "questions": [], "raw_response": raw,
                "error": "LLM 응답을 파싱할 수 없습니다."}

    # 8. 검증 — 모의고사 기반은 교과서 overlap 대신 기본 형식만 확인
    validated = []
    for q in generated:
        # Stage 2에서 answer_text만 생성됨 (선지는 나중에 생성)
        answer_text = q.get("answer_text") or q.get("answer")
        if not answer_text:
            continue
        if not q.get("question_text"):
            continue
        # answer_text를 확실히 설정
        if isinstance(answer_text, str):
            q["answer_text"] = answer_text.strip()
        else:
            q["answer_text"] = str(answer_text)
        validated.append(q)

    if not validated:
        return {"success": False, "questions": [], "raw_response": raw,
                "error": "생성된 문제가 형식 검증을 통과하지 못했습니다. 다시 시도해 주세요."}

    # 8.5. 오답 생성 에이전트: 정답을 포함한 5지선다 선지 생성
    for i, q in enumerate(validated):
        q.setdefault("question_no", i + 1)
    validated = _generate_distractors(
        validated, passages_block, api_key, model_name,
    )

    # 8.6. 학생 풀이 시뮬레이션: 정답 없이 풀어보고 불일치 탐지
    sim_result = _simulate_student(
        validated, "객관식", api_key, model_name,
    )
    validated = _check_simulation_results(validated, sim_result, "객관식")

    # 8.7. 검수 에이전트: 생성된 문제 품질 검증 + 불합격시 재생성 (시뮬레이션 결과 반영)
    validated, review_summary = _review_and_retry(
        validated, passages_block, target_type, "객관식",
        api_key, model_name, "모의고사 지문",
    )
    if review_summary:
        review_summary["simulation"] = {
            "difficulty": sim_result.get("overall_difficulty", "알 수 없음") if sim_result else "시뮬레이션 실패",
            "mismatches": sum(1 for q in validated if q.get("sim_mismatch")),
            "alternatives": sum(1 for q in validated if q.get("sim_alternative")),
        }
    if not validated:
        return {"success": False, "questions": [],
                "error": "검수 에이전트가 모든 문제를 불합격 처리했습니다. 다시 시도해 주세요."}

    # 9. 결과 정리
    for i, q in enumerate(validated):
        q["question_no"] = i + 1
        q.setdefault("sub_type", target_type)
        q.setdefault("question_type", "객관식")
        q.setdefault("source", "AI생성(모의고사)")
        q["source_passages"] = [
            {
                "exam_year": mp.get("exam_year"),
                "exam_month": mp.get("exam_month"),
                "question_no": mp.get("question_no"),
                "original_type": mp.get("sub_type_pred"),
            }
            for mp in mock_passages[:len(validated)]
        ]

    metadata = {
        "count_requested": effective_count,
        "count_generated": len(validated),
        "sub_type": target_type,
        "source_type": "모의고사",
        "question_type": "객관식",
        "model": model_name,
        "mock_passages_used": len(mock_passages),
        "review": review_summary,
        "required_question_nos": list(required_nos_set) if required_nos_set else None,
    }

    return {
        "success": True,
        "questions": validated,
        "metadata": metadata,
        "textbook_used": {"source": "모의고사", "label": "모의고사 지문 활용"},
        "mock_passages_used": [
            {
                "exam_year": mp.get("exam_year"),
                "exam_month": mp.get("exam_month"),
                "question_no": mp.get("question_no"),
                "original_type": mp.get("sub_type_pred"),
            }
            for mp in mock_passages
        ],
        "examples_used": len(examples),
        "error": None,
    }


def _generate_mock_based_subjective(
    conn: sqlite3.Connection,
    count: int,
    exam_year: Optional[int] = None,
    exam_month: Optional[int] = None,
    max_examples: int = 15,
    api_key: str = "",
    model_name: str = "",
    required_question_nos: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """모의고사 지문을 소재로 서술형 문제를 생성한다."""

    # required_question_nos가 있으면 해당 수만큼 최소 생성 보장
    effective_count = count
    if required_question_nos:
        effective_count = max(count, len(required_question_nos))

    # 1. 모의고사 지문을 넓넓히 가져오기 (선별용)
    fetch_count = max(effective_count * 3, 10)
    mock_passages = _fetch_mock_passage(
        conn,
        exclude_sub_type="__none__",  # 모든 유형 허용
        exam_year=exam_year,
        exam_month=exam_month,
        count=fetch_count,
        required_question_nos=required_question_nos,
    )

    if not mock_passages:
        conn.close()
        return {
            "success": False, "questions": [],
            "error": "모의고사 지문을 찾을 수 없습니다.\n모의고사 데이터를 먼저 추가해 주세요.",
        }

    # 2. 서술형 기출 예시 가져오기 (스타일 참고용)
    examples = _fetch_example_questions(
        conn,
        sub_type=None,
        source_type=None,
        question_type="주관식",
        max_examples=max_examples,
    )
    examples = _filter_valid_examples(examples, "주관식")
    conn.close()

    # 3. 지문 텍스트 구성 (정답으로 원문 복원 + 원래 유형 표시)
    passages_block = ""
    for idx, mp in enumerate(mock_passages, 1):
        orig_type = mp.get("sub_type_pred", "알 수 없음")
        src_info = f"{mp.get('exam_year', '?')}년 {mp.get('exam_month', '?')}월 {mp.get('question_no', '?')}번"
        ptext = _restore_passage(
            (mp["passage_text"] or "")[:2000],
            sub_type=mp.get("sub_type_pred"),
            answer=mp.get("answer"),
            choices=mp.get("choices"),
        )
        passages_block += (
            f"\n### 지문 {idx} (출처: {src_info}, 원래 유형: {orig_type})\n"
            f"{ptext}\n"
        )

    # 3.5. required_question_nos가 있으면 필수 지문 안내 추가
    required_nos_set = set(required_question_nos) if required_question_nos else set()
    if required_nos_set:
        required_src_labels = []
        for mp in mock_passages:
            qno = mp.get("question_no")
            if qno in required_nos_set:
                required_src_labels.append(f"{mp.get('exam_year','?')}년 {mp.get('exam_month','?')}월 {qno}번")
        passages_block += (
            f"\n\n## ★ 필수 지문 (반드시 이 지문들로 문제를 만드세요)\n"
            f"다음 지문들은 반드시 각각 1문제씩 서술형 문제를 생성해야 합니다: "
            f"{', '.join(required_src_labels)}\n"
        )

    # 4. 기출 예시 텍스트
    if examples:
        examples_text = "\n\n".join(
            _format_example(ex, i + 1)
            for i, ex in enumerate(examples)
        )
    else:
        examples_text = "(기출 예시 없음 — 일반적인 내신 서술형 스타일로 만드세요)"

    # 5. Stage 1 — 분석 에이전트: 지문 선별 + 서술형 출제 전략 수립
    analysis_plan = _run_analysis_stage(
        passage_text=passages_block,
        examples_text=examples_text,
        target_type="서술형",
        question_type="서술형",
        count=effective_count,
        api_key=api_key,
        model_name=model_name,
        source_label="모의고사 지문",
        is_mock=True,
    )
    if not analysis_plan:
        return {"success": False, "questions": [],
                "error": "Stage 1 분석 에이전트 호출 실패. API 키를 확인하세요."}

    # 6. Stage 2 — 생성: 분석 전략 기반 서술형 문제 생성
    try:
        raw = _run_generation_stage(
            passage_text=passages_block,
            analysis_plan=analysis_plan,
            target_type="서술형",
            question_type="서술형",
            count=effective_count,
            api_key=api_key,
            model_name=model_name,
            source_label="모의고사 지문",
        )
        if not raw:
            return {"success": False, "questions": [],
                    "error": "Stage 2 문제 생성 실패. 다시 시도해 주세요."}
    except Exception as exc:
        return {"success": False, "questions": [],
                "error": f"Gemini API 오류: {exc}"}

    # 7. 응답 파싱
    generated = _parse_llm_response(raw)
    if not generated:
        return {"success": False, "questions": [], "raw_response": raw,
                "error": "LLM 응답을 파싱할 수 없습니다."}

    # 8. 검증
    validated = []
    for q in generated:
        if not q.get("answer"):
            continue
        if not q.get("question_text"):
            continue
        validated.append(q)

    if not validated:
        return {"success": False, "questions": [], "raw_response": raw,
                "error": "생성된 문제가 형식 검증을 통과하지 못했습니다. 다시 시도해 주세요."}

    # 8.5. 학생 풀이 시뮬레이션: 정답 없이 풀어보고 불일치 탐지
    for i, q in enumerate(validated):
        q.setdefault("question_no", i + 1)
    sim_result = _simulate_student(
        validated, "주관식", api_key, model_name,
    )
    validated = _check_simulation_results(validated, sim_result, "주관식")

    # 8.6. 검수 에이전트: 생성된 문제 품질 검증 + 불합격시 재생성 (시뮬레이션 결과 반영)
    validated, review_summary = _review_and_retry(
        validated, passages_block, "서술형", "주관식",
        api_key, model_name, "모의고사 지문",
    )
    if review_summary:
        review_summary["simulation"] = {
            "difficulty": sim_result.get("overall_difficulty", "알 수 없음") if sim_result else "시뮬레이션 실패",
            "mismatches": sum(1 for q in validated if q.get("sim_mismatch")),
            "alternatives": sum(1 for q in validated if q.get("sim_alternative")),
        }
    if not validated:
        return {"success": False, "questions": [],
                "error": "검수 에이전트가 모든 문제를 불합격 처리했습니다. 다시 시도해 주세요."}

    # 9. 결과 정리
    for i, q in enumerate(validated):
        q["question_no"] = i + 1
        q.setdefault("sub_type", "서술형")
        q.setdefault("question_type", "주관식")
        q.setdefault("source", "AI생성(모의고사)")
        q["source_passages"] = [
            {
                "exam_year": mp.get("exam_year"),
                "exam_month": mp.get("exam_month"),
                "question_no": mp.get("question_no"),
                "original_type": mp.get("sub_type_pred"),
            }
            for mp in mock_passages[:len(validated)]
        ]

    metadata = {
        "count_requested": effective_count,
        "count_generated": len(validated),
        "sub_type": "서술형",
        "source_type": "모의고사",
        "question_type": "주관식",
        "model": model_name,
        "mock_passages_used": len(mock_passages),
        "review": review_summary,
        "required_question_nos": list(required_nos_set) if required_nos_set else None,
    }

    return {
        "success": True,
        "questions": validated,
        "metadata": metadata,
        "textbook_used": {"source": "모의고사", "label": "모의고사 지문 활용"},
        "mock_passages_used": [
            {
                "exam_year": mp.get("exam_year"),
                "exam_month": mp.get("exam_month"),
                "question_no": mp.get("question_no"),
                "original_type": mp.get("sub_type_pred"),
            }
            for mp in mock_passages
        ],
        "examples_used": len(examples),
        "error": None,
    }


# ── 핵심 생성 함수 ────────────────────────────────────────────────────────────

def generate_questions(
    count: int = 3,
    sub_type: str = "빈칸추론",
    source_type: Optional[str] = None,   # "출판사" | "모의고사" | None(전체)
    question_type: str = "객관식",       # "객관식" | "주관식"
    publisher: Optional[str] = None,
    textbook_label: Optional[str] = None,
    unit_no: Optional[int] = None,
    subject: Optional[str] = None,
    semester_exam: Optional[str] = None,
    exam_year: Optional[int] = None,
    exam_month: Optional[int] = None,
    max_examples: int = 15,
    api_key: Optional[str] = None,
    model_name: Optional[str] = None,
    required_question_nos: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    새 문제를 생성한다.

    Parameters:
        count: 생성할 문제 수
        sub_type: 문제 유형 (빈칸추론, 어법, 어휘 등)
        source_type: 기출 소스 ("출판사"=학교, "모의고사", None=전체)
        question_type: "객관식" 또는 "주관식"
        publisher: 출판사 필터 (예: "NE능률")
        textbook_label: 교과서명 필터 (예: "NE능률(김성곤)")
        unit_no: 단원 번호 필터
        subject: 과목 필터 ("공통영어1", "공통영어2")
        semester_exam: 시험 종류 ("1학기 중간", "1학기 기말", "2학기 중간", "2학기 기말")
        max_examples: 기출 예시 최대 개수
        api_key: Gemini API 키
        model_name: Gemini 모델명

    Returns:
        {
            "success": bool,
            "questions": [...],      # 생성된 문제 리스트
            "metadata": {...},       # 생성 조건 메타데이터
            "textbook_used": {...},  # 사용된 교과서 정보
            "examples_used": int,    # 사용된 기출 예시 수
            "error": str or None,
        }
    """
    api_key = api_key or GEMINI_API_KEY
    model_name = model_name or GEMINI_MODEL

    if not GENAI_AVAILABLE:
        return {"success": False, "questions": [], "error": "google-generativeai 패키지 미설치"}
    if not api_key:
        return {"success": False, "questions": [], "error": "Gemini API 키 없음"}

    # 유형 검증 (객관식만)
    if question_type == "객관식":
        valid_types = SUB_TYPES + ["서술형"]
        if sub_type not in valid_types and sub_type != "자유":
            return {"success": False, "questions": [],
                    "error": f"알 수 없는 유형: {sub_type}\n사용 가능: {', '.join(SUB_TYPES)}"}

    conn = _get_conn()

    # ── 모의고사 기반: 완전히 다른 로직 ──────────────────────────────
    if source_type == "모의고사":
        if question_type == "객관식":
            return _generate_mock_based(
                conn=conn,
                count=count,
                target_type=sub_type,
                exam_year=exam_year,
                exam_month=exam_month,
                max_examples=max_examples,
                api_key=api_key,
                model_name=model_name,
                required_question_nos=required_question_nos,
            )
        else:  # 주관식/서술형
            return _generate_mock_based_subjective(
                conn=conn,
                count=count,
                exam_year=exam_year,
                exam_month=exam_month,
                max_examples=max_examples,
                api_key=api_key,
                model_name=model_name,
                required_question_nos=required_question_nos,
            )

    # ── 교과서(출판사) 기반: 기존 로직 ─────────────────────────────
    # 1. 교과서 본문 가져오기
    textbook = _fetch_textbook_passage(
        conn,
        publisher=publisher,
        textbook_label=textbook_label,
        unit_no=unit_no,
        subject=subject,
        semester_exam=semester_exam,
    )
    if not textbook:
        avail = conn.execute(
            "SELECT DISTINCT textbook_label FROM textbooks ORDER BY textbook_label"
        ).fetchall()
        avail_list = [r[0] for r in avail]
        hint = "\n사용 가능한 교과서: " + ", ".join(avail_list) if avail_list else ""
        hint += "\n\n  → python generate_questions.py --list-textbooks 로 전체 목록 확인"
        conn.close()
        return {"success": False, "questions": [],
                "error": f"조건에 맞는 교과서 본문을 찾을 수 없습니다.{hint}"}

    # 2. 기출 예시 가져오기
    if question_type == "주관식":
        example_sub_type = None
    else:
        example_sub_type = sub_type if sub_type != "자유" else None

    examples = _fetch_example_questions(
        conn,
        sub_type=example_sub_type,
        source_type=source_type,
        question_type=question_type,
        publisher=publisher,
        unit_no=unit_no,
        semester_exam=semester_exam,
        exam_year=exam_year,
        exam_month=exam_month,
        max_examples=max_examples,
    )

    if not examples and (publisher or unit_no or semester_exam or exam_year or exam_month):
        examples = _fetch_example_questions(
            conn,
            sub_type=example_sub_type,
            source_type=source_type,
            question_type=question_type,
            max_examples=max_examples,
        )

    examples = _filter_valid_examples(examples, question_type)
    conn.close()

    # 3. 프롬프트 구성
    passage_text = textbook["passage_text"] or ""
    if len(passage_text) > 8000:
        passage_text = passage_text[:8000] + "\n... (이하 생략)"

    textbook_info = (
        f"교과서: {textbook['textbook_label']} "
        f"{textbook['unit_no']}과"
        f"{(' - ' + textbook['unit_title']) if textbook.get('unit_title') else ''}"
    )

    examples_text = ""
    if examples:
        examples_text = "\n\n".join(
            _format_example(ex, i + 1)
            for i, ex in enumerate(examples)
        )
    else:
        examples_text = "(기출 예시 없음 — 일반적인 수능/모의고사 출제 스타일로 만드세요)"

    type_label = sub_type if question_type == "객관식" else "서술형"

    # 교과서 본문 텍스트 앞에 교과서 정보 추가
    full_passage = f"{textbook_info}\n\n{passage_text}"

    # 4. Stage 1 — 분석 에이전트: 교과서 기반 출제 전략 수립
    analysis_plan = _run_analysis_stage(
        passage_text=full_passage,
        examples_text=examples_text,
        target_type=type_label,
        question_type=question_type,
        count=count,
        api_key=api_key,
        model_name=model_name,
        source_label="교과서 본문",
    )
    if not analysis_plan:
        return {"success": False, "questions": [],
                "error": "Stage 1 분석 에이전트 호출 실패. API 키를 확인하세요."}

    # 5. Stage 2 — 생성: 분석 전략 기반 문제 생성
    try:
        raw = _run_generation_stage(
            passage_text=full_passage,
            analysis_plan=analysis_plan,
            target_type=type_label,
            question_type=question_type,
            count=count,
            api_key=api_key,
            model_name=model_name,
            source_label="교과서 본문",
        )
        if not raw:
            return {"success": False, "questions": [],
                    "error": "Stage 2 문제 생성 실패. 다시 시도해 주세요."}
    except Exception as exc:
        return {"success": False, "questions": [],
                "error": f"Gemini API 오류: {exc}"}

    # 6. 응답 파싱
    generated = _parse_llm_response(raw)
    if not generated:
        return {"success": False, "questions": [], "raw_response": raw,
                "error": "LLM 응답을 파싱할 수 없습니다."}

    # 6.5. 생성 결과 검증 (교과서 본문 기반 + 정답 확인)
    generated = _validate_generated(generated, passage_text, question_type)
    if not generated:
        return {"success": False, "questions": [], "raw_response": raw,
                "error": "생성된 문제가 교과서 본문 검증을 통과하지 못했습니다. 다시 시도해 주세요."}

    # 6.7. 오답 생성 에이전트 (객관식만): 정답 포함 5지선다 선지 생성
    type_label = sub_type if question_type == "객관식" else "서술형"
    for i, q in enumerate(generated):
        q.setdefault("question_no", i + 1)
    if question_type == "객관식":
        generated = _generate_distractors(
            generated, passage_text, api_key, model_name,
        )

    # 6.8. 학생 풀이 시뮬레이션: 정답 없이 풀어보고 불일치 탐지
    sim_result = _simulate_student(
        generated, question_type, api_key, model_name,
    )
    generated = _check_simulation_results(generated, sim_result, question_type)

    # 6.9. 검수 에이전트: 생성된 문제 품질 검증 + 불합격시 재생성 (시뮬레이션 결과 반영)
    generated, review_summary = _review_and_retry(
        generated, passage_text, type_label, question_type,
        api_key, model_name, "교과서 본문",
    )
    if review_summary:
        review_summary["simulation"] = {
            "difficulty": sim_result.get("overall_difficulty", "알 수 없음") if sim_result else "시뮬레이션 실패",
            "mismatches": sum(1 for q in generated if q.get("sim_mismatch")),
            "alternatives": sum(1 for q in generated if q.get("sim_alternative")),
        }
    if not generated:
        return {"success": False, "questions": [],
                "error": "검수 에이전트가 모든 문제를 불합격 처리했습니다. 다시 시도해 주세요."}

    # 7. 결과 정리
    actual_sub_type = sub_type if question_type == "객관식" else "서술형"
    for i, q in enumerate(generated):
        q.setdefault("question_no", i + 1)
        q.setdefault("sub_type", actual_sub_type)
        q.setdefault("question_type", question_type)
        q.setdefault("source", "AI생성")
        q["textbook_label"] = textbook["textbook_label"]
        q["unit_no"] = textbook["unit_no"]
        q["unit_title"] = textbook.get("unit_title", "")
        q["publisher"] = textbook.get("publisher", "")
        q["subject"] = textbook.get("subject", "")

    metadata = {
        "count_requested": count,
        "count_generated": len(generated),
        "sub_type": actual_sub_type,
        "source_type": source_type or "전체",
        "question_type": question_type,
        "model": model_name,
        "review": review_summary,
    }

    textbook_meta = {
        "id": textbook["id"],
        "subject": textbook["subject"],
        "publisher": textbook["publisher"],
        "textbook_label": textbook["textbook_label"],
        "unit_no": textbook["unit_no"],
        "unit_title": textbook.get("unit_title", ""),
    }

    return {
        "success": True,
        "questions": generated,
        "metadata": metadata,
        "textbook_used": textbook_meta,
        "examples_used": len(examples),
        "error": None,
    }


def save_generated_questions(
    result: Dict[str, Any],
    output_path: Optional[str] = None,
) -> str:
    """생성된 문제를 JSON 파일로 저장."""
    from pipeline.config import OUTPUT_DIR
    import datetime

    if not result.get("success"):
        raise ValueError(f"생성 실패: {result.get('error')}")

    if not output_path:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        meta = result["metadata"]
        fname = (
            f"generated_{meta['sub_type']}_{meta['question_type']}_"
            f"{meta['count_generated']}문항_{ts}.json"
        )
        output_path = str(OUTPUT_DIR / fname)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return output_path
