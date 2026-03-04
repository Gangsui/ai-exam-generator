"""
문제 세부 유형(sub_type) 자동 분류기 — 객관식 전용

1단계: 룰 기반 선분류 (확정 케이스)
2단계: Gemini light 보완 분류 (애매한 케이스)

대상: question_type='객관식' 문항만
"""

import re
from typing import Dict, Any, Optional, List, Tuple

try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False


# ─── 세부 유형 목록 (객관식) ─────────────────────────────────────────────────────
#
# 수능/모의고사/학교시험 기준 고1 영어 객관식 문항 유형 22종
#
# ┌──────────────┬────────────────────────────────────────────────────────┐
# │  유형         │  설명 / 대표 질문 패턴                                  │
# ├──────────────┼────────────────────────────────────────────────────────┤
# │ 목적          │ 글의 목적으로 가장 적절한 것은?                         │
# │ 심경/분위기    │ 심경/분위기/감정으로 가장 적절한 것은?                   │
# │ 주장          │ 필자가 주장하는 바로 가장 적절한 것은?                   │
# │ 요지          │ 글의 요지로 가장 적절한 것은?                           │
# │ 주제          │ 글의 주제로 가장 적절한 것은?                           │
# │ 제목          │ 글의 제목으로 가장 적절한 것은?                         │
# │ 함축의미추론   │ 밑줄 친 ...이 의미하는 바로 가장 적절한 것은?           │
# │ 지칭추론      │ 가리키는 대상이 나머지 넷과 다른 것은?                   │
# │ 내용일치      │ 내용과 일치하는 것은?                                   │
# │ 내용불일치    │ 내용과 일치하지 않는 것은?                               │
# │ 내용추론      │ 추론할 수 있는/없는 것은?                               │
# │ 어법          │ 어법상 어색한/적절한/옳은 것은?                         │
# │ 어휘          │ 문맥상 낱말의 쓰임, 단어의 뜻, 영영 풀이               │
# │ 빈칸추론      │ 빈칸에 들어갈 말로 가장 적절한 것은?                    │
# │ 연결어        │ (A), (B), (C)에 들어갈 접속사/연결어                    │
# │ 무관문장      │ 전체 흐름과 관계없는/무관한 문장은?                     │
# │ 문장삽입      │ 주어진 문장이 들어가기에 가장 적절한 곳은?              │
# │ 순서배열      │ 이어질 글의 순서로 가장 적절한 것은?                    │
# │ 요약문완성    │ 요약문의 빈칸 (A), (B)에 들어갈 말은?                   │
# │ 장문독해      │ 41~42번 장문 통합형                                     │
# │ 도표/안내문   │ 도표/그래프/안내문 내용 일치                             │
# │ 대화문        │ 대화문을 읽고 답하는 문항                               │
# │ 기타          │ 위 유형에 해당하지 않는 문항                             │
# └──────────────┴────────────────────────────────────────────────────────┘

SUB_TYPES = [
    "목적", "심경/분위기", "주장", "요지", "주제", "제목",
    "함축의미추론", "지칭추론",
    "내용일치", "내용불일치", "내용추론",
    "어법", "어휘",
    "빈칸추론", "연결어",
    "무관문장", "문장삽입", "순서배열",
    "요약문완성",
    "장문독해",
    "도표/안내문",
    "대화문",
    "기타",
]


# ─── 룰 기반 분류 패턴 ──────────────────────────────────────────────────────────
#
# 순서 중요: 먼저 매칭되는 룰이 우선. 구체적 패턴 → 일반 패턴 순서로 배치.
#

_RULES: List[Tuple[re.Pattern, str, float]] = [

    # ── 목적 ──
    (re.compile(r"목적으로\s*가장\s*(?:적절|알맞)"), "목적", 0.97),
    (re.compile(r"목적을\s*고르"), "목적", 0.95),

    # ── 심경 / 분위기 ──
    (re.compile(r"심경.*(?:적절|알맞|변화)"), "심경/분위기", 0.97),
    (re.compile(r"심정.*(?:적절|알맞)"), "심경/분위기", 0.95),
    (re.compile(r"분위기.*(?:적절|알맞)"), "심경/분위기", 0.95),
    (re.compile(r"감정으로\s*(?:가장\s*)?(?:적절|알맞)"), "심경/분위기", 0.95),
    (re.compile(r"느꼈을\s*(?:감정|심경)"), "심경/분위기", 0.93),
    (re.compile(r"성격.*묘사"), "심경/분위기", 0.90),
    (re.compile(r"감정.*보기\s*어려운"), "심경/분위기", 0.90),
    (re.compile(r"관계로?\s*(?:가장\s*)?(?:적절|알맞)"), "심경/분위기", 0.85),

    # ── 주장 ──
    (re.compile(r"주장하는\s*바"), "주장", 0.97),
    (re.compile(r"주장.*(?:적절|알맞)"), "주장", 0.95),

    # ── 요지 ──
    (re.compile(r"요지로\s*(?:가장\s*)?(?:적절|알맞)"), "요지", 0.97),
    (re.compile(r"요지.*빈칸"), "요지", 0.90),

    # ── 주제 ──
    (re.compile(r"주제로\s*(?:가장\s*)?(?:적절|알맞)"), "주제", 0.97),
    (re.compile(r"main\s*(?:topic|idea|purpose)", re.I), "주제", 0.90),

    # ── 제목 ──
    (re.compile(r"제목으로\s*(?:가장\s*)?(?:적절|알맞)"), "제목", 0.97),
    (re.compile(r"best\s*title", re.I), "제목", 0.90),

    # ── 도표 / 안내문 (어법/어휘보다 먼저) ──
    (re.compile(r"도표|그래프"), "도표/안내문", 0.95),
    (re.compile(r"안내문"), "도표/안내문", 0.90),

    # ── 함축의미추론 (어법/어휘보다 먼저) ──
    (re.compile(r"의미하는\s*바로\s*(?:가장\s*)?(?:적절|알맞)"), "함축의미추론", 0.97),
    (re.compile(r"의미하는\s*바"), "함축의미추론", 0.95),
    (re.compile(r"밑줄.*의미.*(?:적절|알맞)"), "함축의미추론", 0.90),

    # ── 지칭추론 ──
    (re.compile(r"가리키는\s*(?:대상|것).*다른"), "지칭추론", 0.97),
    (re.compile(r"지칭하는\s*바.*다른"), "지칭추론", 0.95),
    (re.compile(r"가리키는\s*것.*적절"), "지칭추론", 0.92),

    # ── 어법 (다양한 패턴) ──
    (re.compile(r"어법상\s*(?:적절|올바른|바른|맞는)"), "어법", 0.97),
    (re.compile(r"어법에\s*맞는"), "어법", 0.97),
    (re.compile(r"어법상\s*(?:어색|틀린|옳지\s*않)"), "어법", 0.97),
    (re.compile(r"어법상\s*옳"), "어법", 0.97),
    (re.compile(r"어법.*어색"), "어법", 0.95),
    (re.compile(r"밑줄.*어법"), "어법", 0.93),
    (re.compile(r"어법에\s*(?:어긋|맞지)"), "어법", 0.95),
    (re.compile(r"문법.*(?:옳|맞|적절|어색|틀)"), "어법", 0.90),
    (re.compile(r"문법적\s*(?:기능|구조).*같은"), "어법", 0.88),
    (re.compile(r"분사구문.*(?:복원|원래)"), "어법", 0.88),
    (re.compile(r"흐름상\s*어색"), "어법", 0.90),
    (re.compile(r"(?:while|that|it|which|what).*쓰임.*(?:다른|같은)"), "어법", 0.88),
    (re.compile(r"문장\s*구조.*같은"), "어법", 0.85),

    # ── 어휘 ──
    (re.compile(r"낱말의\s*쓰임.*적절하지\s*않"), "어휘", 0.97),
    (re.compile(r"낱말의\s*쓰임"), "어휘", 0.95),
    (re.compile(r"문맥상.*(?:낱말|단어).*(?:적절|어색)"), "어휘", 0.95),
    (re.compile(r"뜻으로\s*(?:가장\s*)?적절"), "어휘", 0.95),
    (re.compile(r"단어의\s*뜻"), "어휘", 0.95),
    (re.compile(r"어휘.*(?:가장\s*)?적절"), "어휘", 0.90),
    (re.compile(r"밑줄\s*친.*어휘"), "어휘", 0.88),
    (re.compile(r"뜻풀이.*의미"), "어휘", 0.93),
    (re.compile(r"영어\s*뜻풀이"), "어휘", 0.92),
    (re.compile(r"(?:영어\s*)?(?:정의|definition).*단어"), "어휘", 0.93),
    (re.compile(r"단어.*(?:정의|definition)"), "어휘", 0.93),
    (re.compile(r"의미가\s*(?:가장\s*)?가까운"), "어휘", 0.90),
    (re.compile(r"(?:같은|가까운)\s*(?:뜻|의미)"), "어휘", 0.88),
    (re.compile(r"(?:유의어|반의어|동의어)"), "어휘", 0.90),
    (re.compile(r"어색한\s*낱말"), "어휘", 0.90),
    (re.compile(r"단어.*관계.*(?:다른|같은)"), "어휘", 0.88),
    (re.compile(r"뜻이\s*어색"), "어휘", 0.90),
    (re.compile(r"의미가\s*같은"), "어휘", 0.88),
    (re.compile(r"괄호.*문맥.*낱말"), "어휘", 0.90),
    (re.compile(r"문맥에\s*맞는\s*(?:낱말|표현)"), "어휘", 0.90),

    # ── 요약문완성 (빈칸추론보다 먼저) ──
    (re.compile(r"요약.*빈칸"), "요약문완성", 0.95),
    (re.compile(r"요약.*\([A-Z]\).*들어갈"), "요약문완성", 0.95),
    (re.compile(r"요약하고자\s*할\s*때"), "요약문완성", 0.95),
    (re.compile(r"요약할\s*때"), "요약문완성", 0.93),
    (re.compile(r"요약문"), "요약문완성", 0.90),

    # ── 빈칸추론 ──
    (re.compile(r"빈칸에\s*들어갈"), "빈칸추론", 0.95),
    (re.compile(r"빈칸\s*(?:\([A-Z]\)|[A-Z])에"), "빈칸추론", 0.95),
    (re.compile(r"빈칸\s*\(?[A-Z]\)?.*\(?[A-Z]\)?.*들어갈"), "빈칸추론", 0.95),
    (re.compile(r"빈칸.*들어갈\s*(?:말|표현|단어|어구|것)"), "빈칸추론", 0.93),
    (re.compile(r"빈칸.*들어가(?:기에|는)"), "빈칸추론", 0.90),
    (re.compile(r"[ⓐ-ⓩ]에\s*들어갈\s*(?:말|것)"), "빈칸추론", 0.88),
    (re.compile(r"들어갈\s*(?:말|표현).*(?:적절|알맞)"), "빈칸추론", 0.85),

    # ── 연결어 (빈칸추론 뒤에) ──
    (re.compile(r"접속[사부].*(?:적절|알맞)"), "연결어", 0.93),
    (re.compile(r"연결[어사].*(?:적절|알맞)"), "연결어", 0.93),
    # (A),(B),(C) 들어갈 → 연결어 (빈칸이 아니라 괄호형)
    (re.compile(r"괄호\s*\(?[A-Z]\)?.*들어(?:갈|가기)"), "연결어", 0.90),
    (re.compile(r"\([A-Z]\).*\([A-Z]\).*\([A-Z]\).*들어(?:갈|가기)"), "연결어", 0.88),
    (re.compile(r"문맥상.*\([A-Z]\).*들어(?:갈|가기)"), "연결어", 0.88),
    (re.compile(r"바르게\s*짝지어진"), "연결어", 0.85),

    # ── 무관문장 ──
    (re.compile(r"흐름에?\s*무관한\s*문장"), "무관문장", 0.97),
    (re.compile(r"전체\s*흐름과\s*관계\s*없는"), "무관문장", 0.97),
    (re.compile(r"내용상\s*필요\s*없는"), "무관문장", 0.90),
    (re.compile(r"흐름.*맞지\s*않"), "무관문장", 0.88),

    # ── 문장삽입 ──
    (re.compile(r"주어진\s*문장.*넣기"), "문장삽입", 0.95),
    (re.compile(r"주어진\s*문장이?\s*들어(?:갈|가기)"), "문장삽입", 0.95),
    (re.compile(r"흐름.*주어진\s*문장.*들어"), "문장삽입", 0.95),
    (re.compile(r"(?:흐름|문맥).*들어가(?:기에|야\s*할).*적절"), "문장삽입", 0.93),

    # ── 순서배열 ──
    (re.compile(r"이어질\s*글의\s*순서"), "순서배열", 0.97),
    (re.compile(r"이어질\s*(?:내용|글).*순서.*(?:적절|알맞|배열)"), "순서배열", 0.95),
    (re.compile(r"순서.*맞게\s*배열"), "순서배열", 0.93),
    (re.compile(r"순서로\s*(?:가장\s*)?(?:적절|알맞)"), "순서배열", 0.93),
    (re.compile(r"\[A\].*\[B\].*\[C\]"), "순서배열", 0.90),
    (re.compile(r"\([A-C]\).*(?:적절하게|알맞게)?\s*배열"), "순서배열", 0.90),
    (re.compile(r"자연스러운\s*(?:흐름|순서)"), "순서배열", 0.85),

    # ── 내용추론 (내용일치/불일치보다 먼저 — 추론 키워드 있으면 내용추론 우선) ──
    (re.compile(r"추론할\s*수\s*없는"), "내용추론", 0.95),
    (re.compile(r"추론.*(?:적절|알맞)"), "내용추론", 0.93),
    (re.compile(r"추론.*바르지\s*(?:못|않)"), "내용추론", 0.93),
    (re.compile(r"추론한\s*(?:내용|것)"), "내용추론", 0.92),
    (re.compile(r"추론할\s*수\s*있는"), "내용추론", 0.92),
    (re.compile(r"추론이\s*가장"), "내용추론", 0.90),

    # ── 내용일치 / 불일치 ──
    (re.compile(r"내용과\s*일치하지\s*않"), "내용불일치", 0.97),
    (re.compile(r"내용.*일치하지\s*않"), "내용불일치", 0.95),
    (re.compile(r"내용으로\s*적절하지\s*않"), "내용불일치", 0.92),
    (re.compile(r"NOT\s*(?:correct|true)", re.I), "내용불일치", 0.90),
    (re.compile(r"CANNOT\s*be\s*answered", re.I), "내용불일치", 0.88),
    (re.compile(r"답할\s*수\s*없는\s*질문"), "내용불일치", 0.88),
    (re.compile(r"알\s*수\s*(?:없는|없다)"), "내용불일치", 0.85),
    (re.compile(r"옳지\s*않은\s*것"), "내용불일치", 0.90),
    (re.compile(r"적절하지\s*않은\s*것"), "내용불일치", 0.88),
    (re.compile(r"거리가\s*먼\s*것"), "내용불일치", 0.88),
    (re.compile(r"해당하지\s*않는\s*것"), "내용불일치", 0.88),
    (re.compile(r"아닌\s*것은"), "내용불일치", 0.80),
    (re.compile(r"답.*찾을\s*수\s*없"), "내용불일치", 0.88),
    (re.compile(r"잘못\s*이해한"), "내용불일치", 0.88),
    (re.compile(r"내용과\s*다른"), "내용불일치", 0.85),
    (re.compile(r"올바르지\s*않은"), "내용불일치", 0.85),
    (re.compile(r"내용과\s*일치하는"), "내용일치", 0.97),
    (re.compile(r"내용.*일치하는"), "내용일치", 0.93),
    (re.compile(r"옳은\s*것"), "내용일치", 0.85),
    (re.compile(r"알\s*수\s*있는\s*것"), "내용일치", 0.85),
    (re.compile(r"이해한\s*(?:내용|것).*(?:적절|알맞)"), "내용일치", 0.85),
    (re.compile(r"내용으로\s*(?:적절|알맞)"), "내용일치", 0.85),
    (re.compile(r"올바른\s*것"), "내용일치", 0.80),
    (re.compile(r"열거한\s*것"), "내용일치", 0.80),

    # ── 장문독해 ──
    (re.compile(r"장문.*읽고"), "장문독해", 0.88),
    (re.compile(r"\[4[12]\s*[~\-]\s*4[23]\]"), "장문독해", 0.90),

    # ── 대화문 ──
    (re.compile(r"대화.*(?:빈칸|이어질|적절)"), "대화문", 0.85),
    (re.compile(r"대화.*순서"), "대화문", 0.85),
    (re.compile(r"대화문"), "대화문", 0.80),

    # ── 기타 영어 질문 패턴 ──
    (re.compile(r"best\s*describes?\s*the\s*(?:passage|text|author)", re.I), "주제", 0.85),
    (re.compile(r"according\s*to\s*the\s*passage", re.I), "내용일치", 0.85),

    # ── 주제 확장 (암시, 교훈 등) ──
    (re.compile(r"암시.*(?:적절|알맞)"), "주제", 0.82),
    (re.compile(r"교훈.*(?:적절|알맞)"), "주제", 0.82),
    (re.compile(r"속담.*(?:적절|알맞|어울리)"), "주제", 0.80),
    (re.compile(r"바로\s*(?:나올|이어질)\s*(?:내용|글)"), "주제", 0.78),
]


def rule_based_classify(question_text: str) -> Tuple[Optional[str], float, str]:
    """
    질문 텍스트에 룰 기반 분류 적용.
    반환: (sub_type, confidence, reason) — 매칭 없으면 (None, 0.0, "")
    """
    if not question_text:
        return None, 0.0, ""

    for pattern, sub_type, confidence in _RULES:
        if pattern.search(question_text):
            return sub_type, confidence, f"룰 매칭: {pattern.pattern}"

    return None, 0.0, ""


# ─── Gemini 분류 ────────────────────────────────────────────────────────────────

_GEMINI_INSTRUCTION = """당신은 고등학교 영어 문제 유형 분류 전문가입니다.
아래 문항 정보를 보고 세부 유형을 다음 목록 중 **하나만** 선택하세요.

분류 가능한 유형:
{sub_types}

유형별 핵심 판별 기준:
- 목적: "글의 목적"
- 심경/분위기: 심경, 분위기, 감정 변화
- 주장: "필자가 주장하는 바"
- 요지: "글의 요지"
- 주제: "글의 주제", main idea/topic
- 제목: "글의 제목", best title
- 함축의미추론: "밑줄 친 ~이 의미하는 바"
- 지칭추론: "가리키는 대상이 다른"
- 내용일치: 내용과 일치하는, 알 수 있는
- 내용불일치: 내용과 일치하지 않는, 알 수 없는, 적절하지 않은
- 어법: 어법상 어색한/적절한/옳은, 문법
- 어휘: 낱말의 쓰임, 단어의 뜻, 영영 풀이, 어휘 적절
- 빈칸추론: 빈칸에 들어갈 말
- 연결어: (A),(B),(C)에 들어갈 접속사/연결어, 괄호에 들어갈 말 (연결사)
- 무관문장: 흐름에 무관한/관계없는 문장
- 문장삽입: 주어진 문장이 들어갈 곳
- 순서배열: 이어질 글의 순서
- 요약문완성: 글 요약문의 빈칸
- 장문독해: 41~42번 장문 통합형
- 도표/안내문: 도표, 그래프, 안내문
- 대화문: 대화문 읽고 답하는 문항

출력 형식 (JSON만, 다른 텍스트 없이):
{{"sub_type": "유형명", "confidence": 0.0~1.0, "reason": "한 줄 분류 이유"}}

문항 정보:
- 질문: {question_text}
- 선지: {choices}
- 지문(일부): {passage_snippet}
"""


def gemini_classify(
    question_text: str,
    choices: List[str],
    passage_text: Optional[str],
    api_key: str,
    model_name: str = "gemini-1.5-flash",
) -> Tuple[Optional[str], float, str]:
    """Gemini light 모델로 sub_type 분류. 반환: (sub_type, confidence, reason)"""
    if not GENAI_AVAILABLE or not api_key:
        return None, 0.0, "Gemini API 키 없음"

    try:
        import json
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)

        choices_str = " / ".join(c for c in choices if c) if choices else "없음"
        passage_snippet = (passage_text or "")[:300]
        prompt = _GEMINI_INSTRUCTION.format(
            sub_types=", ".join(SUB_TYPES),
            question_text=question_text,
            choices=choices_str,
            passage_snippet=passage_snippet,
        )

        import re as _re
        response = model.generate_content(prompt)
        raw = response.text.strip()
        raw = _re.sub(r"```(?:json)?", "", raw).replace("```", "").strip()
        data = json.loads(raw)

        sub_type = data.get("sub_type", "기타")
        confidence = float(data.get("confidence", 0.5))
        reason = data.get("reason", "")
        return sub_type, confidence, reason

    except Exception as exc:
        return None, 0.0, f"Gemini 오류: {exc}"


# ─── 통합 분류기 ────────────────────────────────────────────────────────────────

def classify_question(
    question: Dict[str, Any],
    api_key: str = "",
    model_name: str = "gemini-2.5-flash",
    confidence_threshold: float = 0.75,
) -> Dict[str, Any]:
    """
    단일 문항 dict 에 sub_type 분류 결과를 채워서 반환.
    객관식 문항만 분류. 주관식은 건너뜀.
    """
    from pipeline.config import CLASSIFIER_VERSION

    # 주관식은 분류 대상이 아님
    q_type = question.get("question_type", "")
    if q_type and q_type != "객관식":
        question["sub_type_pred"] = None
        question["sub_type_confidence"] = 0.0
        question["sub_type_reason"] = "주관식 제외"
        question["needs_review"] = question.get("needs_review", 0)
        question["classifier_model"] = "skip"
        question["classifier_version"] = CLASSIFIER_VERSION
        return question

    q_text = question.get("question_text", "")
    choices = question.get("choices") or []
    if isinstance(choices, str):
        import json
        try:
            choices = json.loads(choices)
        except Exception:
            choices = []
    passage = question.get("passage_text")

    # 1단계: 룰 기반
    sub_type, confidence, reason = rule_based_classify(q_text)

    # 2단계: Gemini (룰이 만족스럽지 않을 때)
    if sub_type is None or confidence < confidence_threshold:
        if api_key:
            sub_type_g, conf_g, reason_g = gemini_classify(
                q_text, choices, passage, api_key, model_name
            )
            if sub_type_g and conf_g >= confidence:
                sub_type, confidence, reason = sub_type_g, conf_g, f"Gemini: {reason_g}"
        # 그래도 없으면 기타
        if sub_type is None:
            sub_type = "기타"
            confidence = 0.3
            reason = "분류 불가"

    needs_review = 1 if confidence < confidence_threshold else question.get("needs_review", 0)

    question["sub_type_pred"] = sub_type
    question["sub_type_confidence"] = round(confidence, 4)
    question["sub_type_reason"] = reason
    question["needs_review"] = needs_review
    question["classifier_model"] = model_name if api_key else "rule_based"
    question["classifier_version"] = CLASSIFIER_VERSION
    return question


def classify_questions_batch(
    questions: List[Dict[str, Any]],
    api_key: str = "",
    model_name: str = "gemini-2.5-flash",
    confidence_threshold: float = 0.75,
) -> List[Dict[str, Any]]:
    """문항 목록 일괄 분류."""
    return [
        classify_question(q, api_key, model_name, confidence_threshold)
        for q in questions
    ]
