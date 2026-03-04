#!/usr/bin/env python3
"""
DB 브라우저 - 내장 http.server 사용 (의존 패키지 없음)
실행: python db_server.py
열기: http://localhost:8765
"""
import json
import sqlite3
import sys
import threading
import traceback
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline.config import DB_PATH

PORT = 8765

# 문제 생성 진행 상태 저장 (thread-safe)
_generation_lock = threading.Lock()
_generation_status = {
    "running": False,
    "progress": "",
    "result": None,
    "error": None,
}

# ── 공통 CSS / 레이아웃 ────────────────────────────────────────────────────────
STYLE = """
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', 'Apple SD Gothic Neo', sans-serif;
       background:#f4f6f9; color:#333; font-size:14px; }
header { background:#2c3e50; color:#fff; padding:14px 24px;
         display:flex; align-items:center; gap:16px; }
header h1 { font-size:18px; font-weight:600; }
header a { color:#aed6f1; text-decoration:none; font-size:13px; }
header a:hover { text-decoration:underline; }
.container { max-width:1100px; margin:24px auto; padding:0 16px; }
.card { background:#fff; border-radius:8px; box-shadow:0 1px 4px rgba(0,0,0,.1);
        padding:20px; margin-bottom:20px; }
.card h2 { font-size:15px; color:#2c3e50; margin-bottom:12px; border-bottom:1px solid #eee; padding-bottom:8px; }
table { width:100%; border-collapse:collapse; font-size:13px; }
th { background:#ecf0f1; text-align:left; padding:8px 10px; font-weight:600; color:#555; }
td { padding:7px 10px; border-bottom:1px solid #f0f0f0; vertical-align:top; }
tr:hover td { background:#fafbfc; }
.badge { display:inline-block; padding:2px 8px; border-radius:12px;
         font-size:11px; font-weight:600; }
.badge-school { background:#d5f5e3; color:#1e8449; }
.badge-mock   { background:#d6eaf8; color:#1a5276; }
.badge-review { background:#fdebd0; color:#935116; }
.badge-subtype { background:#e8daef; color:#6c3483; }
.qcard { background:#fff; border-radius:8px; box-shadow:0 1px 4px rgba(0,0,0,.1);
         padding:16px 20px; margin-bottom:14px; border-left:4px solid #3498db; }
.qcard.review { border-left-color:#e67e22; }
.qno   { font-size:12px; color:#888; margin-bottom:4px; }
.qtext { font-size:14px; font-weight:600; margin-bottom:8px; line-height:1.5; }
.passage { background:#f8f9fa; border-radius:4px; padding:10px 14px;
           font-size:13px; line-height:1.7; margin-bottom:10px;
           color:#444; white-space:pre-wrap; max-height:180px; overflow-y:auto; }
.choices { list-style:none; }
.choices li { padding:4px 0; font-size:13px; line-height:1.5; }
.choices li.answer { color:#1a5276; font-weight:700; }
.choices li::before { content: attr(data-n) '. '; color:#999; font-size:12px; }
.answer-badge { background:#2ecc71; color:#fff; border-radius:4px;
                padding:1px 6px; font-size:11px; margin-left:8px; }
.stats-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; }
.stat-box { background:#fff; border-radius:8px; padding:16px; text-align:center;
            box-shadow:0 1px 4px rgba(0,0,0,.1); }
.stat-num { font-size:28px; font-weight:700; color:#2c3e50; }
.stat-lbl { font-size:12px; color:#888; margin-top:4px; }
.search-bar { display:flex; gap:8px; margin-bottom:20px; }
.search-bar input { flex:1; padding:8px 12px; border:1px solid #ddd;
                    border-radius:6px; font-size:14px; }
.search-bar button, .btn { padding:8px 16px; background:#2c3e50; color:#fff;
                    border:none; border-radius:6px; cursor:pointer; font-size:13px; }
.search-bar button:hover, .btn:hover { background:#34495e; }
a.row-link { color:#2980b9; text-decoration:none; font-weight:500; }
a.row-link:hover { text-decoration:underline; }
.meta-row { display:flex; flex-wrap:wrap; gap:12px; font-size:13px; color:#666; margin-bottom:16px; }
.meta-row span strong { color:#333; }
.pager { display:flex; gap:8px; align-items:center; margin-top:16px; font-size:13px; }
.pager a { padding:5px 12px; background:#ecf0f1; border-radius:4px;
           text-decoration:none; color:#333; }
.pager a:hover { background:#bdc3c7; }
.pager .cur { padding:5px 12px; background:#2c3e50; color:#fff; border-radius:4px; }

/* 문제 생성 UI */
.gen-form { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
.gen-form .full { grid-column:1/-1; }
.gen-group { margin-bottom:0; }
.gen-group label { display:block; font-weight:600; font-size:13px; color:#444; margin-bottom:6px; }
.gen-group select, .gen-group input[type='number'], .gen-group input[type='text'] {
  width:100%; padding:8px 12px; border:1px solid #ddd; border-radius:6px; font-size:14px; }
.chip-group { display:flex; flex-wrap:wrap; gap:6px; }
.chip-group label { font-weight:normal; font-size:12px; cursor:pointer; }
.chip-group input[type='checkbox'] { display:none; }
.chip-group span { display:inline-block; padding:5px 12px; background:#ecf0f1;
  border-radius:16px; font-size:12px; transition:all .15s; border:1px solid transparent; }
.chip-group input:checked + span { background:#3498db; color:#fff; border-color:#2980b9; }
.radio-group { display:flex; gap:10px; }
.radio-group label { font-weight:normal; cursor:pointer; }
.radio-group input[type='radio'] { display:none; }
.radio-group span { display:inline-block; padding:6px 16px; background:#ecf0f1;
  border-radius:6px; font-size:13px; transition:all .15s; border:1px solid transparent; }
.radio-group input:checked + span { background:#2c3e50; color:#fff; }
.btn-generate { padding:12px 32px; background:#27ae60; color:#fff; border:none;
  border-radius:8px; font-size:15px; font-weight:700; cursor:pointer;
  transition:background .15s; }
.btn-generate:hover { background:#219a52; }
.btn-generate:disabled { background:#bdc3c7; cursor:not-allowed; }
.result-area { margin-top:20px; min-height:50px; }
.spinner { display:inline-block; width:18px; height:18px; border:3px solid #ddd;
  border-top-color:#3498db; border-radius:50%;
  animation:spin .8s linear infinite; margin-right:8px; vertical-align:middle; }
@keyframes spin { to { transform:rotate(360deg); } }
.gen-result-card { background:#f0faf0; border:1px solid #d5e8d5; border-radius:8px;
  padding:16px; margin-top:12px; }
.gen-result-card.error { background:#fdf0f0; border-color:#e8d5d5; }
</style>
"""

def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def page_wrap(title, body, breadcrumb=""):
    nav = (f'<header><h1>📚 DB 뷰어</h1><a href="/">홈</a>'
           f'<a href="/exams">시험 목록</a><a href="/mock">모의고사</a>'
           f'<a href="/subtypes">유형별 보기</a><a href="/textbooks">교과서 본문</a>'
           f'<a href="/generate" style="color:#2ecc71;font-weight:600">⚡ 문제 생성</a>'
           f'{breadcrumb}</header>')
    return f"<!DOCTYPE html><html lang='ko'><head><meta charset='utf-8'><title>{title}</title>{STYLE}</head><body>{nav}<div class='container'>{body}</div></body></html>"


# ── 페이지들 ──────────────────────────────────────────────────────────────────

def page_home():
    conn = get_conn()
    total    = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
    exams    = conn.execute("SELECT COUNT(*) FROM exams").fetchone()[0]
    review   = conn.execute("SELECT COUNT(*) FROM questions WHERE needs_review=1").fetchone()[0]
    has_ans  = conn.execute("SELECT COUNT(*) FROM questions WHERE answer IS NOT NULL AND answer!=''").fetchone()[0]
    obj      = conn.execute("SELECT COUNT(*) FROM questions WHERE question_type='객관식'").fetchone()[0]
    subj     = conn.execute("SELECT COUNT(*) FROM questions WHERE question_type!='객관식'").fetchone()[0]

    src_rows = conn.execute("SELECT source_type,COUNT(*) c FROM exams GROUP BY source_type").fetchall()
    pub_rows = conn.execute("SELECT publisher,COUNT(*) c FROM exams WHERE publisher IS NOT NULL GROUP BY publisher ORDER BY c DESC LIMIT 8").fetchall()

    # 교과서 본문 통계
    tb_total = 0
    try:
        tb_total = conn.execute("SELECT COUNT(*) FROM textbooks").fetchone()[0]
    except Exception:
        pass
    conn.close()

    pct = f"{has_ans/total*100:.1f}" if total else "0"
    stats = f"""
    <div class='stats-grid'>
      <div class='stat-box'><div class='stat-num'>{exams}</div><div class='stat-lbl'>시험 수</div></div>
      <div class='stat-box'><div class='stat-num'>{total:,}</div><div class='stat-lbl'>총 문항</div></div>
      <div class='stat-box'><div class='stat-num'>{obj:,}</div><div class='stat-lbl'>객관식</div></div>
      <div class='stat-box'><div class='stat-num'>{subj:,}</div><div class='stat-lbl'>서술형</div></div>
      <div class='stat-box'><div class='stat-num'>{pct}%</div><div class='stat-lbl'>정답 보유율</div></div>
      <div class='stat-box'><div class='stat-num' style='color:#e67e22'>{review}</div><div class='stat-lbl'>검수 필요</div></div>
      <div class='stat-box'><div class='stat-num' style='color:#27ae60'>{tb_total}</div><div class='stat-lbl'>교과서 본문</div></div>
    </div>"""

    src_html = "".join(
        f"<tr><td><span class='badge badge-{'mock' if r[0]=='모의고사' else 'school'}'>{r[0]}</span></td><td><b>{r[1]}</b></td></tr>"
        for r in src_rows
    )
    pub_html = "".join(f"<tr><td>{r[0]}</td><td><b>{r[1]}</b></td></tr>" for r in pub_rows)

    body = f"""
    <div class='card'><h2>통계</h2>{stats}</div>
    <div style='display:grid;grid-template-columns:1fr 1fr;gap:16px'>
      <div class='card'><h2>소스 유형</h2><table><thead><tr><th>유형</th><th>수</th></tr></thead><tbody>{src_html}</tbody></table></div>
      <div class='card'><h2>출판사 (상위 8)</h2><table><thead><tr><th>출판사</th><th>수</th></tr></thead><tbody>{pub_html}</tbody></table></div>
    </div>
    <div style='text-align:center;margin-top:16px'>
      <a href='/exams' class='btn'>📋 시험 목록 보기</a>
      <a href='/mock' class='btn' style='margin-left:8px;background:#1a5276'>🎯 모의고사 보기</a>
      <a href='/subtypes' class='btn' style='margin-left:8px;background:#8e44ad'>🏷️ 유형별 보기</a>
      <a href='/textbooks' class='btn' style='margin-left:8px;background:#27ae60'>📖 교과서 본문 보기</a>
      <a href='/generate' class='btn' style='margin-left:8px;background:#e67e22'>⚡ 문제 생성</a>
    </div>"""
    return page_wrap("DB 뷰어 - 홈", body)


def page_exams(qs_params):
    conn    = get_conn()
    pub     = qs_params.get("publisher", [""])[0]
    source  = qs_params.get("source", [""])[0]
    keyword = qs_params.get("q", [""])[0]
    page    = int(qs_params.get("page", ["1"])[0])
    per     = 30
    offset  = (page - 1) * per

    clauses, params = [], []
    if pub:
        clauses.append("publisher LIKE ?"); params.append(f"%{pub}%")
    if source:
        clauses.append("source_type=?"); params.append(source)
    if keyword:
        clauses.append("(school_name LIKE ? OR file_name_raw LIKE ?)"); params += [f"%{keyword}%"]*2

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    total_cnt = conn.execute(f"SELECT COUNT(*) FROM exams {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT id,source_type,exam_year,term,school_name,publisher,textbook_author,file_name_raw "
        f"FROM exams {where} ORDER BY source_type,exam_year DESC,id LIMIT ? OFFSET ?",
        params + [per, offset]
    ).fetchall()

    def q_cnt(eid):
        return conn.execute("SELECT COUNT(*) FROM questions WHERE exam_id=?", (eid,)).fetchone()[0]

    rows_html = ""
    for r in rows:
        badge = "mock" if r["source_type"] == "모의고사" else "school"
        author = f"({r['textbook_author']})" if r["textbook_author"] else ""
        pub_str = f"{r['publisher'] or ''}{author}"
        cnt = q_cnt(r["id"])
        rows_html += (
            f"<tr><td>{r['id']}</td>"
            f"<td><span class='badge badge-{badge}'>{r['source_type']}</span></td>"
            f"<td>{r['exam_year'] or '-'}</td>"
            f"<td>{r['term'] or '-'}</td>"
            f"<td><a class='row-link' href='/exam/{r['id']}'>{r['school_name'] or '-'}</a></td>"
            f"<td>{pub_str}</td>"
            f"<td style='text-align:center'>{cnt}</td></tr>"
        )
    conn.close()

    total_pages = max(1, (total_cnt + per - 1) // per)

    def plink(p, lbl):
        base = f"/exams?page={p}"
        if pub: base += f"&publisher={urllib.parse.quote(pub)}"
        if source: base += f"&source={urllib.parse.quote(source)}"
        if keyword: base += f"&q={urllib.parse.quote(keyword)}"
        if p == page:
            return f"<span class='cur'>{lbl}</span>"
        return f"<a href='{base}'>{lbl}</a>"

    pager = "<div class='pager'>"
    if page > 1: pager += plink(page-1, "◀ 이전")
    for p in range(max(1,page-2), min(total_pages+1, page+3)):
        pager += plink(p, str(p))
    if page < total_pages: pager += plink(page+1, "다음 ▶")
    pager += f"<span style='color:#888;margin-left:8px'>총 {total_cnt}개</span></div>"

    filter_html = f"""
    <form method='get' action='/exams' style='display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap'>
      <input name='q' placeholder='학교명 검색...' value='{keyword}' style='padding:7px 12px;border:1px solid #ddd;border-radius:6px;font-size:13px;min-width:160px'>
      <select name='source' style='padding:7px;border:1px solid #ddd;border-radius:6px;font-size:13px'>
        <option value=''>전체 소스</option>
        <option {'selected' if source=='출판사' else ''} value='출판사'>출판사</option>
        <option {'selected' if source=='모의고사' else ''} value='모의고사'>모의고사</option>
      </select>
      <input name='publisher' placeholder='출판사...' value='{pub}' style='padding:7px 12px;border:1px solid #ddd;border-radius:6px;font-size:13px;min-width:100px'>
      <button type='submit'>검색</button>
    </form>"""

    body = f"""
    <div class='card'>
      <h2>시험 목록</h2>
      {filter_html}
      <table>
        <thead><tr><th>ID</th><th>소스</th><th>연도</th><th>학기</th><th>학교/파일</th><th>출판사</th><th>문항</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
      {pager}
    </div>"""
    return page_wrap("시험 목록", body)


def page_exam(exam_id, qs_params):
    conn = get_conn()
    ex = conn.execute(
        "SELECT * FROM exams WHERE id=?", (exam_id,)
    ).fetchone()
    if not ex:
        conn.close()
        return page_wrap("없음", "<div class='card'>시험을 찾을 수 없습니다.</div>")

    keyword = qs_params.get("q", [""])[0]
    clauses = ["exam_id=?"]
    params  = [exam_id]
    if keyword:
        clauses.append("question_text LIKE ?"); params.append(f"%{keyword}%")

    rows = conn.execute(
        f"SELECT * FROM questions WHERE {' AND '.join(clauses)} ORDER BY question_no",
        params
    ).fetchall()
    conn.close()

    badge = "mock" if ex["source_type"] == "모의고사" else "school"
    author = f"({ex['textbook_author']})" if ex["textbook_author"] else ""
    pub_str = f"{ex['publisher'] or ''}{author}"

    meta = f"""
    <div class='meta-row'>
      <span><span class='badge badge-{badge}'>{ex['source_type']}</span></span>
      <span><strong>파일</strong>: {ex['file_name_raw']}</span>
      <span><strong>연도</strong>: {ex['exam_year'] or '-'}</span>
      <span><strong>학기</strong>: {ex['term'] or '-'}</span>
      <span><strong>학교</strong>: {ex['school_name'] or '-'}</span>
      <span><strong>출판사</strong>: {pub_str or '-'}</span>
      <span><strong>문항 수</strong>: {len(rows)}</span>
    </div>"""

    filter_html = f"""
    <form method='get' style='display:flex;gap:8px;margin-bottom:16px'>
      <input name='q' placeholder='문항 텍스트 검색...' value='{keyword}'
             style='flex:1;padding:7px 12px;border:1px solid #ddd;border-radius:6px;font-size:13px'>
      <button type='submit'>검색</button>
    </form>"""

    cards = ""
    for r in rows:
        choices = json.loads(r["choices"]) if r["choices"] else []
        ans = str(r["answer"]) if r["answer"] else ""
        rev = r["needs_review"]
        passage = r["passage_text"] or ""

        choices_html = "<ul class='choices'>"
        for i, c in enumerate(choices):
            if c:
                # 정답 강조
                is_ans = (ans == str(i+1))
                li_cls = " class='answer'" if is_ans else ""
                ans_marker = f"<span class='answer-badge'>✓ 정답</span>" if is_ans else ""
                choices_html += f"<li{li_cls} data-n='{i+1}'>{c}{ans_marker}</li>"
        choices_html += "</ul>"

        passage_html = f"<div class='passage'>{passage}</div>" if passage else ""
        q_type_color = "#8e44ad" if r["question_type"] != "객관식" else "#3498db"
        unit_str = f"{r['unit_no']}과" if r["unit_no"] else ""
        unit_html = (f"<span style='background:#e8f4fd;color:#2471a3;padding:2px 7px;"
                     f"border-radius:4px;font-size:12px;margin-left:6px'>{unit_str}</span>"
                     if unit_str else "")

        # 세부 유형 배지
        stype = r["sub_type_pred"] if r["sub_type_pred"] else ""
        stype_html = (f"<a href='/subtypes?type={urllib.parse.quote(stype)}' "
                      f"style='text-decoration:none'>"
                      f"<span class='badge badge-subtype'>{stype}</span></a>"
                      if stype else "")

        cards += f"""
        <div class='qcard {'review' if rev else ''}'>
          <div class='qno'>
            <b style='color:{q_type_color}'>[{r['question_no']}번]</b>
            {r['question_type']}
            {unit_html}
            {stype_html}
            {'<span class="badge badge-review">검수필요</span>' if rev else ''}
            {'<span style="color:#888;font-size:12px;margin-left:8px">정답: ' + ans + '</span>' if ans else ''}
          </div>
          <div class='qtext'>{r['question_text'] or ''}</div>
          {passage_html}
          {choices_html}
        </div>"""

    body = f"""
    <div class='card'>
      <h2>시험 상세</h2>
      {meta}
      {filter_html}
    </div>
    {cards}"""
    return page_wrap(f"시험 #{exam_id}", body, f'<a href="/exams">시험목록</a>')


def page_search(keyword, page=1):
    conn  = get_conn()
    per   = 20
    offset = (page - 1) * per
    total_cnt = conn.execute(
        "SELECT COUNT(*) FROM questions WHERE question_text LIKE ?", (f"%{keyword}%",)
    ).fetchone()[0]
    rows = conn.execute("""
        SELECT q.id, q.question_no, q.question_type, q.question_text, q.answer, q.choices, q.needs_review,
               e.id as eid, e.source_type, e.school_name, e.publisher, e.exam_year
        FROM questions q JOIN exams e ON q.exam_id=e.id
        WHERE q.question_text LIKE ?
        ORDER BY e.exam_year DESC, q.question_no
        LIMIT ? OFFSET ?
    """, (f"%{keyword}%", per, offset)).fetchall()
    conn.close()

    cards = ""
    for r in rows:
        choices = json.loads(r["choices"]) if r["choices"] else []
        ans     = str(r["answer"]) if r["answer"] else ""
        src_str = r["school_name"] or r["publisher"] or r["source_type"]
        badge   = "mock" if r["source_type"] == "모의고사" else "school"

        choices_html = "<ul class='choices'>"
        for i, c in enumerate(choices):
            if c:
                is_ans = (ans == str(i+1))
                li_cls = " class='answer'" if is_ans else ""
                ans_marker = "<span class='answer-badge'>✓</span>" if is_ans else ""
                choices_html += f"<li{li_cls} data-n='{i+1}'>{c}{ans_marker}</li>"
        choices_html += "</ul>"

        cards += f"""
        <div class='qcard'>
          <div class='qno'>
            <a class='row-link' href='/exam/{r['eid']}'><span class='badge badge-{badge}'>{r['source_type']}</span> {src_str} ({r['exam_year']})</a>
            &nbsp;·&nbsp; <b>[{r['question_no']}번]</b> {r['question_type']}
            {'&nbsp;<span class="badge badge-review">검수필요</span>' if r['needs_review'] else ''}
          </div>
          <div class='qtext'>{r['question_text'] or ''}</div>
          {choices_html}
        </div>"""

    total_pages = max(1, (total_cnt + per - 1) // per)

    def plink(p, lbl):
        cls = "cur" if p == page else ""
        href = f"/search?q={urllib.parse.quote(keyword)}&page={p}"
        if p == page:
            return f"<span class='cur'>{lbl}</span>"
        return f"<a href='{href}'>{lbl}</a>"

    pager = "<div class='pager'>"
    if page > 1: pager += plink(page-1, "◀")
    for p in range(max(1,page-2), min(total_pages+1, page+3)):
        pager += plink(p, str(p))
    if page < total_pages: pager += plink(page+1, "▶")
    pager += f"<span style='color:#888;margin-left:8px'>총 {total_cnt}개</span></div>"

    body = f"""
    <div class='card'>
      <h2>검색: "{keyword}"</h2>
      {pager}
    </div>
    {cards}
    <div class='card'>{pager}</div>"""
    return page_wrap(f"검색: {keyword}", body)


# ── 교과서 본문 페이지 ────────────────────────────────────────────────────────

def page_textbooks(qs_params):
    conn     = get_conn()
    subject  = qs_params.get("subject", [""])[0]
    pub      = qs_params.get("publisher", [""])[0]
    sem      = qs_params.get("semester", [""])[0]

    clauses, params = [], []
    if subject:
        clauses.append("subject=?"); params.append(subject)
    if pub:
        clauses.append("publisher=?"); params.append(pub)
    if sem:
        clauses.append("semester_exam=?"); params.append(sem)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = conn.execute(
        f"SELECT id, subject, publisher, textbook_label, unit_no, unit_title, "
        f"semester_exam, page_count FROM textbooks {where} "
        f"ORDER BY subject, publisher, unit_no",
        params
    ).fetchall()

    # 필터 옵션 가져오기
    subjects = [r[0] for r in conn.execute("SELECT DISTINCT subject FROM textbooks ORDER BY subject")]
    publishers = [r[0] for r in conn.execute("SELECT DISTINCT publisher FROM textbooks WHERE publisher IS NOT NULL ORDER BY publisher")]
    semesters = [r[0] for r in conn.execute("SELECT DISTINCT semester_exam FROM textbooks WHERE semester_exam IS NOT NULL ORDER BY semester_exam")]
    conn.close()

    subj_opts = "".join(f"<option {'selected' if subject==s else ''} value='{s}'>{s}</option>" for s in subjects)
    pub_opts = "".join(f"<option {'selected' if pub==p else ''} value='{p}'>{p}</option>" for p in publishers)
    sem_opts = "".join(f"<option {'selected' if sem==s else ''} value='{s}'>{s}</option>" for s in semesters)

    filter_html = f"""
    <form method='get' action='/textbooks' style='display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap'>
      <select name='subject' style='padding:7px;border:1px solid #ddd;border-radius:6px;font-size:13px'>
        <option value=''>전체 과목</option>{subj_opts}
      </select>
      <select name='publisher' style='padding:7px;border:1px solid #ddd;border-radius:6px;font-size:13px'>
        <option value=''>전체 출판사</option>{pub_opts}
      </select>
      <select name='semester' style='padding:7px;border:1px solid #ddd;border-radius:6px;font-size:13px'>
        <option value=''>전체 시험기간</option>{sem_opts}
      </select>
      <button type='submit'>검색</button>
    </form>"""

    rows_html = ""
    for r in rows:
        sem_color = {"1학기 중간":"#3498db","1학기 기말":"#2980b9","2학기 중간":"#e67e22","2학기 기말":"#d35400"}.get(r["semester_exam"],"#888")
        rows_html += (
            f"<tr>"
            f"<td>{r['id']}</td>"
            f"<td><span class='badge badge-school'>{r['subject']}</span></td>"
            f"<td>{r['publisher']}</td>"
            f"<td><a class='row-link' href='/textbook/{r['id']}'>{r['textbook_label']}</a></td>"
            f"<td style='text-align:center'>{r['unit_no']}과</td>"
            f"<td>{r['unit_title'] or '-'}</td>"
            f"<td><span style='color:{sem_color};font-weight:600;font-size:12px'>{r['semester_exam'] or '-'}</span></td>"
            f"<td style='text-align:center'>{r['page_count'] or '-'}</td>"
            f"</tr>"
        )

    body = f"""
    <div class='card'>
      <h2>교과서 본문 목록 ({len(rows)}개)</h2>
      {filter_html}
      <table>
        <thead><tr><th>ID</th><th>과목</th><th>출판사</th><th>교과서</th><th>단원</th><th>단원명</th><th>시험기간</th><th>페이지</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>"""
    return page_wrap("교과서 본문", body)


def page_textbook_detail(tb_id):
    conn = get_conn()
    r = conn.execute("SELECT * FROM textbooks WHERE id=?", (tb_id,)).fetchone()
    if not r:
        conn.close()
        return page_wrap("없음", "<div class='card'>교과서를 찾을 수 없습니다.</div>")

    # 같은 교과서의 다른 단원
    siblings = conn.execute(
        "SELECT id, unit_no, unit_title FROM textbooks "
        "WHERE textbook_label=? AND subject=? ORDER BY unit_no",
        (r["textbook_label"], r["subject"])
    ).fetchall()
    conn.close()

    sem_color = {"1학기 중간":"#3498db","1학기 기말":"#2980b9","2학기 중간":"#e67e22","2학기 기말":"#d35400"}.get(r["semester_exam"],"#888")

    meta = f"""
    <div class='meta-row'>
      <span><span class='badge badge-school'>{r['subject']}</span></span>
      <span><strong>출판사</strong>: {r['textbook_label']}</span>
      <span><strong>단원</strong>: {r['unit_no']}과 {('- ' + r['unit_title']) if r['unit_title'] else ''}</span>
      <span><strong>시험기간</strong>: <span style='color:{sem_color};font-weight:600'>{r['semester_exam'] or '-'}</span></span>
      <span><strong>페이지 수</strong>: {r['page_count'] or '-'}</span>
    </div>"""

    # 같은 교과서 다른 단원 네비게이션
    sibling_links = ""
    if len(siblings) > 1:
        links = []
        for s in siblings:
            if s["id"] == tb_id:
                links.append(f"<span class='cur' style='padding:4px 10px'>{s['unit_no']}과</span>")
            else:
                links.append(f"<a href='/textbook/{s['id']}' style='padding:4px 10px;background:#ecf0f1;border-radius:4px;text-decoration:none;color:#333'>{s['unit_no']}과</a>")
        sibling_links = f"<div style='display:flex;gap:6px;margin-bottom:16px;align-items:center'><span style='font-size:12px;color:#888'>단원 이동:</span> {''.join(links)}</div>"

    passage = (r["passage_text"] or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    body = f"""
    <div class='card'>
      <h2>교과서 본문 상세</h2>
      {meta}
      {sibling_links}
    </div>
    <div class='card'>
      <h2>📖 본문</h2>
      <div style='background:#f8f9fa;border-radius:6px;padding:16px 20px;
                  font-size:14px;line-height:1.9;white-space:pre-wrap;color:#333;
                  max-height:none;font-family:"Georgia","Noto Serif",serif'>{passage}</div>
    </div>"""
    return page_wrap(
        f"{r['textbook_label']} {r['unit_no']}과", body,
        '<a href="/textbooks">교과서 목록</a>'
    )


# ── 유형별 보기 페이지 ────────────────────────────────────────────────────────

def page_subtypes(qs_params):
    conn   = get_conn()
    stype  = qs_params.get("type", [""])[0]
    page   = int(qs_params.get("page", ["1"])[0])
    per    = 30

    if not stype:
        # 유형 목록 + 카운트
        rows = conn.execute("""
            SELECT COALESCE(sub_type_pred, '미분류') AS st, COUNT(*) c
            FROM questions WHERE question_type='객관식'
            GROUP BY sub_type_pred ORDER BY c DESC
        """).fetchall()
        total_obj = sum(r["c"] for r in rows)
        classified = sum(r["c"] for r in rows if r["st"] != '미분류')
        conn.close()

        grid = ""
        colors = {
            "목적":"#e74c3c","심경/분위기":"#e67e22","주장":"#f39c12","요지":"#f1c40f",
            "주제":"#2ecc71","제목":"#27ae60","함축의미추론":"#1abc9c","지칭추론":"#16a085",
            "내용일치":"#3498db","내용불일치":"#2980b9","어법":"#9b59b6","어휘":"#8e44ad",
            "빈칸추론":"#e74c3c","연결어":"#d35400","무관문장":"#c0392b",
            "문장삽입":"#2c3e50","순서배열":"#34495e","요약문완성":"#7f8c8d",
            "장문독해":"#95a5a6","도표/안내문":"#bdc3c7","대화문":"#1a5276",
            "기타":"#999","미분류":"#ccc",
        }
        for r in rows:
            st = r["st"]
            color = colors.get(st, "#888")
            pct = f"{r['c']/total_obj*100:.1f}%" if total_obj else "0%"
            grid += (
                f"<a href='/subtypes?type={urllib.parse.quote(st)}' "
                f"style='text-decoration:none;color:inherit'>"
                f"<div class='stat-box' style='border-left:4px solid {color};cursor:pointer'>"
                f"<div class='stat-num' style='font-size:22px'>{r['c']}</div>"
                f"<div class='stat-lbl'>{st}</div>"
                f"<div style='font-size:11px;color:#aaa'>{pct}</div>"
                f"</div></a>"
            )

        body = f"""
        <div class='card'>
          <h2>객관식 유형별 분류 현황</h2>
          <div class='meta-row'>
            <span><strong>객관식 총</strong>: {total_obj:,}문항</span>
            <span><strong>분류 완료</strong>: {classified:,}문항</span>
            <span><strong>미분류</strong>: {total_obj - classified:,}문항</span>
          </div>
          <div class='stats-grid' style='grid-template-columns:repeat(auto-fit,minmax(120px,1fr))'>{grid}</div>
        </div>"""
        return page_wrap("유형별 보기", body)

    # 특정 유형의 문항 목록
    offset = (page - 1) * per
    if stype == "미분류":
        where_clause = "q.question_type='객관식' AND q.sub_type_pred IS NULL"
        params_list = []
    else:
        where_clause = "q.question_type='객관식' AND q.sub_type_pred=?"
        params_list = [stype]

    total_cnt = conn.execute(
        f"SELECT COUNT(*) FROM questions q WHERE {where_clause}", params_list
    ).fetchone()[0]
    rows = conn.execute(f"""
        SELECT q.id, q.exam_id, q.question_no, q.question_type, q.question_text,
               q.passage_text, q.choices, q.answer, q.unit_no, q.needs_review,
               q.sub_type_pred,
               e.source_type, e.school_name, e.publisher, e.exam_year, e.textbook_author
        FROM questions q JOIN exams e ON q.exam_id=e.id
        WHERE {where_clause}
        ORDER BY e.exam_year DESC, e.id, q.question_no
        LIMIT ? OFFSET ?
    """, params_list + [per, offset]).fetchall()
    conn.close()

    total_pages = max(1, (total_cnt + per - 1) // per)

    def plink(p, lbl):
        href = f"/subtypes?type={urllib.parse.quote(stype)}&page={p}"
        if p == page:
            return f"<span class='cur'>{lbl}</span>"
        return f"<a href='{href}'>{lbl}</a>"

    pager = "<div class='pager'>"
    if page > 1: pager += plink(page-1, "◀ 이전")
    for p in range(max(1, page-3), min(total_pages+1, page+4)):
        pager += plink(p, str(p))
    if page < total_pages: pager += plink(page+1, "다음 ▶")
    pager += f"<span style='color:#888;margin-left:8px'>총 {total_cnt}개</span></div>"

    cards = ""
    for r in rows:
        choices = json.loads(r["choices"]) if r["choices"] else []
        ans = str(r["answer"]) if r["answer"] else ""
        passage = r["passage_text"] or ""
        src_str = r["school_name"] or r["publisher"] or r["source_type"]
        badge = "mock" if r["source_type"] == "모의고사" else "school"
        author = f"({r['textbook_author']})" if r["textbook_author"] else ""
        pub_str = f"{r['publisher'] or ''}{author}"
        unit_str = f"{r['unit_no']}과" if r["unit_no"] else ""

        choices_html = "<ul class='choices'>"
        for i, c in enumerate(choices):
            if c:
                is_ans = (ans == str(i+1))
                li_cls = " class='answer'" if is_ans else ""
                ans_marker = "<span class='answer-badge'>✓ 정답</span>" if is_ans else ""
                choices_html += f"<li{li_cls} data-n='{i+1}'>{c}{ans_marker}</li>"
        choices_html += "</ul>"

        passage_html = f"<div class='passage'>{passage}</div>" if passage else ""

        cards += f"""
        <div class='qcard'>
          <div class='qno'>
            <a class='row-link' href='/exam/{r["exam_id"]}'><span class='badge badge-{badge}'>{r["source_type"]}</span> {src_str} ({r["exam_year"] or "-"})</a>
            &nbsp;·&nbsp; <b>[{r["question_no"]}번]</b>
            {f"<span style='color:#888;font-size:12px'>{unit_str}</span>" if unit_str else ""}
            {f"<span style='color:#888;font-size:12px;margin-left:4px'>출판사: {pub_str}</span>" if pub_str else ""}
            {'&nbsp;<span class="badge badge-review">검수필요</span>' if r["needs_review"] else ''}
            {f"<span style='color:#888;font-size:12px;margin-left:8px'>정답: {ans}</span>" if ans else ""}
          </div>
          <div class='qtext'>{r["question_text"] or ""}</div>
          {passage_html}
          {choices_html}
        </div>"""

    body = f"""
    <div class='card'>
      <h2><span class='badge badge-subtype' style='font-size:14px;padding:4px 12px'>{stype}</span> 유형 문항 ({total_cnt}개)</h2>
      <div style='margin-bottom:12px'><a href='/subtypes'>&larr; 유형 목록으로 돌아가기</a></div>
      {pager}
    </div>
    {cards}
    <div class='card'>{pager}</div>"""
    return page_wrap(f"유형: {stype}", body)


# ── 모의고사 전용 페이지 ──────────────────────────────────────────────────────

def page_mock(qs_params):
    """모의고사 시험 목록 + 문항 보기."""
    conn = get_conn()
    year_filter = qs_params.get("year", [""])[0]
    month_filter = qs_params.get("month", [""])[0]
    page = int(qs_params.get("page", ["1"])[0])
    per = 30

    # 모의고사 시험 목록
    clauses = ["e.source_type='모의고사'"]
    params = []
    if year_filter:
        clauses.append("e.exam_year=?"); params.append(int(year_filter))
    if month_filter:
        clauses.append("e.exam_month=?"); params.append(int(month_filter))

    where = " AND ".join(clauses)

    # 연도/월 필터 옵션
    years = [r[0] for r in conn.execute(
        "SELECT DISTINCT exam_year FROM exams WHERE source_type='모의고사' AND exam_year IS NOT NULL ORDER BY exam_year DESC"
    ).fetchall()]
    months = [r[0] for r in conn.execute(
        "SELECT DISTINCT exam_month FROM exams WHERE source_type='모의고사' AND exam_month IS NOT NULL ORDER BY exam_month"
    ).fetchall()]

    # 통계
    mock_exams = conn.execute(f"SELECT COUNT(DISTINCT e.id) FROM exams e WHERE {where}", params).fetchone()[0]
    mock_questions = conn.execute(f"""
        SELECT COUNT(*) FROM questions q JOIN exams e ON q.exam_id=e.id WHERE {where}
    """, params).fetchone()[0]

    # 유형별 분포
    type_dist = conn.execute(f"""
        SELECT COALESCE(q.sub_type_pred, '미분류') AS st, COUNT(*) c
        FROM questions q JOIN exams e ON q.exam_id=e.id
        WHERE {where}
        GROUP BY q.sub_type_pred ORDER BY c DESC
    """, params).fetchall()

    type_chips = ""
    for r in type_dist:
        type_chips += f"<span class='badge badge-subtype' style='margin:2px;padding:3px 10px'>{r['st']} ({r['c']})</span> "

    # 시험 목록
    exams = conn.execute(f"""
        SELECT e.id, e.exam_year, e.exam_month, e.grade, e.file_name_raw,
               COUNT(q.id) as q_cnt
        FROM exams e LEFT JOIN questions q ON q.exam_id=e.id
        WHERE {where}
        GROUP BY e.id
        ORDER BY e.exam_year DESC, e.exam_month DESC
    """, params).fetchall()

    # 상세 문항 (선택된 시험)
    exam_id = qs_params.get("eid", [""])[0]
    detail_html = ""

    if exam_id:
        questions = conn.execute("""
            SELECT * FROM questions WHERE exam_id=? ORDER BY question_no
        """, (int(exam_id),)).fetchall()
        ex = conn.execute("SELECT * FROM exams WHERE id=?", (int(exam_id),)).fetchone()

        if ex and questions:
            detail_html += f"""
            <div class='card' id='detail'>
              <h2>🎯 {ex['exam_year']}년 {ex['exam_month'] or ''}월 모의고사 ({ex['grade'] or '고1'}) — {len(questions)}문항</h2>
              <div class='meta-row'>
                <span><strong>파일</strong>: {ex['file_name_raw']}</span>
              </div>
            </div>"""

            for q in questions:
                choices = json.loads(q["choices"]) if q["choices"] else []
                ans = str(q["answer"]) if q["answer"] else ""
                passage = q["passage_text"] or ""
                stype = q["sub_type_pred"] or ""

                choices_html = "<ul class='choices'>"
                for i, c in enumerate(choices):
                    if c:
                        is_ans = (ans == str(i+1))
                        li_cls = " class='answer'" if is_ans else ""
                        ans_marker = "<span class='answer-badge'>✓ 정답</span>" if is_ans else ""
                        choices_html += f"<li{li_cls} data-n='{i+1}'>{c}{ans_marker}</li>"
                choices_html += "</ul>"

                passage_html = f"<div class='passage'>{passage}</div>" if passage else ""
                stype_html = f"<span class='badge badge-subtype'>{stype}</span>" if stype else ""
                q_type_color = "#8e44ad" if q["question_type"] != "객관식" else "#3498db"

                detail_html += f"""
                <div class='qcard'>
                  <div class='qno'>
                    <b style='color:{q_type_color}'>[{q['question_no']}번]</b>
                    {q['question_type']} {stype_html}
                    {'<span class="badge badge-review">검수필요</span>' if q['needs_review'] else ''}
                    {'<span style="color:#888;font-size:12px;margin-left:8px">정답: ' + ans + '</span>' if ans else ''}
                  </div>
                  <div class='qtext'>{q['question_text'] or ''}</div>
                  {passage_html}
                  {choices_html}
                </div>"""

    conn.close()

    # 필터 폼
    year_opts = "".join(f"<option {'selected' if year_filter==str(y) else ''} value='{y}'>{y}년</option>" for y in years)
    month_opts = "".join(f"<option {'selected' if month_filter==str(m) else ''} value='{m}'>{m}월</option>" for m in months)

    filter_html = f"""
    <form method='get' action='/mock' style='display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap'>
      <select name='year' style='padding:7px;border:1px solid #ddd;border-radius:6px;font-size:13px'>
        <option value=''>전체 연도</option>{year_opts}
      </select>
      <select name='month' style='padding:7px;border:1px solid #ddd;border-radius:6px;font-size:13px'>
        <option value=''>전체 월</option>{month_opts}
      </select>
      <button type='submit'>검색</button>
    </form>"""

    # 시험 테이블
    exam_rows = ""
    for ex in exams:
        sel = "style='background:#eaf2f8;font-weight:700'" if exam_id and str(ex['id']) == exam_id else ""
        qs_base = f"?year={year_filter}&month={month_filter}&eid={ex['id']}#detail" if year_filter or month_filter else f"?eid={ex['id']}#detail"
        exam_rows += f"""
        <tr {sel}>
          <td>{ex['id']}</td>
          <td>{ex['exam_year'] or '-'}년</td>
          <td>{ex['exam_month'] or '-'}월</td>
          <td>{ex['grade'] or '-'}</td>
          <td><a class='row-link' href='/mock{qs_base}'>{ex['file_name_raw'] or '-'}</a></td>
          <td style='text-align:center'>{ex['q_cnt']}</td>
        </tr>"""

    stats_html = f"""
    <div class='stats-grid' style='margin-bottom:16px'>
      <div class='stat-box'><div class='stat-num'>{mock_exams}</div><div class='stat-lbl'>모의고사 수</div></div>
      <div class='stat-box'><div class='stat-num'>{mock_questions}</div><div class='stat-lbl'>총 문항</div></div>
    </div>"""

    body = f"""
    <div class='card'>
      <h2>🎯 모의고사 문제 뷰어</h2>
      {stats_html}
      {filter_html}
      <div style='margin-bottom:12px'><b>유형 분포:</b> {type_chips}</div>
      <table>
        <thead><tr><th>ID</th><th>연도</th><th>월</th><th>학년</th><th>파일명</th><th>문항수</th></tr></thead>
        <tbody>{exam_rows}</tbody>
      </table>
    </div>
    {detail_html}"""
    return page_wrap("모의고사", body)


# ── 문제 생성 페이지 ──────────────────────────────────────────────────────────

def page_generate():
    """문제 생성 UI 페이지."""
    conn = get_conn()

    # 유형 목록
    types = conn.execute("""
        SELECT COALESCE(sub_type_pred, '') AS st, COUNT(*) c
        FROM questions WHERE question_type='객관식' AND sub_type_pred IS NOT NULL
        GROUP BY sub_type_pred ORDER BY c DESC
    """).fetchall()
    type_list = [r["st"] for r in types if r["st"]]

    # 교과서 목록
    textbooks = conn.execute("""
        SELECT DISTINCT textbook_label FROM textbooks ORDER BY textbook_label
    """).fetchall()
    tb_list = [r[0] for r in textbooks]

    # 과목 목록
    subjects = conn.execute("""
        SELECT DISTINCT subject FROM textbooks ORDER BY subject
    """).fetchall()
    subj_list = [r[0] for r in subjects]

    # 모의고사 연도/월
    mock_years = conn.execute("""
        SELECT DISTINCT exam_year FROM exams WHERE source_type='모의고사' AND exam_year IS NOT NULL ORDER BY exam_year DESC
    """).fetchall()
    mock_months = conn.execute("""
        SELECT DISTINCT exam_month FROM exams WHERE source_type='모의고사' AND exam_month IS NOT NULL ORDER BY exam_month
    """).fetchall()

    conn.close()

    # 유형 칩 (체크박스)
    type_chips = ""
    for t in type_list:
        checked = "checked" if t == "빈칸추론" else ""
        type_chips += f"""<label><input type='checkbox' name='types' value='{t}' {checked}><span>{t}</span></label>"""

    # 교과서 옵션
    tb_opts = "".join(f"<option value='{t}'>{t}</option>" for t in tb_list)

    # 과목 옵션
    subj_opts = "".join(f"<option value='{s}'>{s}</option>" for s in subj_list)

    # 모의고사 연도/월 옵션
    my_opts = "".join(f"<option value='{r[0]}'>{r[0]}년</option>" for r in mock_years)
    mm_opts = "".join(f"<option value='{r[0]}'>{r[0]}월</option>" for r in mock_months)

    body = f"""
    <div class='card'>
      <h2>⚡ AI 문제 생성기</h2>
      <p style='color:#666;font-size:13px;margin-bottom:16px'>교과서 본문 + 기출 예시를 바탕으로 Gemini AI가 새 문제를 생성합니다.</p>

      <div class='gen-form' id='genForm'>

        <!-- 문제 형식 (복수 선택 가능) -->
        <div class='gen-group full'>
          <label>📋 문제 형식 (복수 선택 가능)</label>
          <div class='chip-group'>
            <label><input type='checkbox' name='formats' value='객관식' checked onchange='toggleFormat()'><span>객관식 (5지선다)</span></label>
            <label><input type='checkbox' name='formats' value='주관식' onchange='toggleFormat()'><span>주관식 (서술형)</span></label>
          </div>
        </div>

        <!-- 문제 유형 선택 (객관식 전용) -->
        <div class='gen-group full' id='typeSection'>
          <label>📝 문제 유형 (복수 선택 가능)</label>
          <div class='chip-group'>
            {type_chips}
          </div>
        </div>

        <!-- 서술형 안내 (주관식 전용, 처음에 숨김) -->
        <div class='gen-group full' id='subjNotice' style='display:none'>
          <div style='background:#fef9e7;border:1px solid #f9e79f;border-radius:8px;padding:14px 18px;font-size:13px;color:#7d6608'>
            ℹ️ <b>서술형</b>은 유형 구분 없이, DB의 서술형 기출 예시를 랜덤으로 참고하여 교과서 본문 기반으로 다양한 형태(빈칸 채우기, 어형 변환, 영작, 배열 등)의 문제를 생성합니다.
          </div>
        </div>

        <!-- 문제 수 -->
        <div class='gen-group'>
          <label id='countLabel'>🔢 유형당 문제 수</label>
          <input type='number' id='count' value='3' min='1' max='10' style='max-width:120px'>
        </div>

        <!-- 기출 소스 -->
        <div class='gen-group'>
          <label>📚 기출 소스</label>
          <div class='chip-group'>
            <label><input type='checkbox' name='sources' value='출판사' checked><span>출판사 (학교 기출)</span></label>
            <label><input type='checkbox' name='sources' value='모의고사'><span>모의고사</span></label>
          </div>
        </div>

        <!-- 기출 예시 수 -->
        <div class='gen-group'>
          <label>📊 기출 예시 수</label>
          <input type='number' id='maxExamples' value='15' min='1' max='50' style='max-width:120px'>
        </div>

        <!-- 교과서 선택 -->
        <div class='gen-group'>
          <label>📖 교과서</label>
          <select id='textbook'>
            <option value=''>랜덤 (자동 선택)</option>
            {tb_opts}
          </select>
        </div>

        <!-- 단원 선택 -->
        <div class='gen-group'>
          <label>📑 단원</label>
          <select id='unit'>
            <option value=''>전체 (랜덤)</option>
            <option value='1'>1과</option>
            <option value='2'>2과</option>
            <option value='3'>3과</option>
            <option value='4'>4과</option>
          </select>
        </div>

        <!-- 과목 -->
        <div class='gen-group'>
          <label>📕 과목</label>
          <select id='subject'>
            <option value=''>전체</option>
            {subj_opts}
          </select>
        </div>

        <!-- 학기 선택 -->
        <div class='gen-group'>
          <label>🗓️ 시험 기간</label>
          <select id='semester'>
            <option value=''>전체 (무관)</option>
            <option value='1학기 중간'>1학기 중간</option>
            <option value='1학기 기말'>1학기 기말</option>
            <option value='2학기 중간'>2학기 중간</option>
            <option value='2학기 기말'>2학기 기말</option>
          </select>
        </div>

        <!-- 모의고사 연도/월 -->
        <div class='gen-group'>
          <label>📅 모의고사 연도 (선택)</label>
          <select id='examYear' onchange='loadMockQuestions()'>
            <option value=''>전체</option>
            {my_opts}
          </select>
        </div>

        <div class='gen-group'>
          <label>📅 모의고사 월 (선택)</label>
          <select id='examMonth' onchange='loadMockQuestions()'>
            <option value=''>전체</option>
            {mm_opts}
          </select>
        </div>

        <!-- 모의고사 고급: 문제 번호 선택 -->
        <div class='gen-group full' id='mockAdvancedSection' style='display:none'>
          <label>🎯 모의고사 문제 번호 선택 <span style='font-size:12px;color:#888'>(선택한 번호의 지문으로 변형 문제를 생성합니다)</span></label>
          <div style='margin-bottom:8px'>
            <button type='button' onclick='toggleAllMockQ(true)' style='font-size:12px;padding:3px 10px;margin-right:4px;cursor:pointer'>전체 선택</button>
            <button type='button' onclick='toggleAllMockQ(false)' style='font-size:12px;padding:3px 10px;cursor:pointer'>전체 해제</button>
          </div>
          <div class='chip-group' id='mockQuestionChips' style='gap:6px'>
            <span style='color:#999;font-size:13px'>연도와 월을 선택하면 문제 번호가 표시됩니다.</span>
          </div>
          <div style='margin-top:6px;font-size:12px;color:#e67e22'>⚠ 선택한 문제 번호마다 최소 1문항씩 변형 문제가 생성됩니다.</div>
        </div>

        <!-- 출력 옵션 -->
        <div class='gen-group full'>
          <label>📄 출력 옵션</label>
          <div class='chip-group'>
            <label><input type='checkbox' name='output_opts' value='word'><span>Word 파일 생성</span></label>
            <label><input type='checkbox' name='output_opts' value='no_answers'><span>정답/해설 제외</span></label>
          </div>
        </div>

        <!-- 생성 버튼 -->
        <div class='gen-group full' style='text-align:center;margin-top:8px'>
          <button class='btn-generate' id='btnGenerate' onclick='startGenerate()'>⚡ 문제 생성 시작</button>
        </div>
      </div>

      <!-- 결과 영역 -->
      <div class='result-area' id='resultArea'></div>
    </div>

    <script>
    function getChecked(name) {{
      return Array.from(document.querySelectorAll("input[name='" + name + "']:checked")).map(e => e.value);
    }}
    function getVal(id) {{ return document.getElementById(id).value; }}

    function toggleFormat() {{
      const fmts = getChecked('formats');
      const hasObj = fmts.includes('객관식');
      const hasSubj = fmts.includes('주관식');
      const typeSection = document.getElementById('typeSection');
      const subjNotice = document.getElementById('subjNotice');
      const countLabel = document.getElementById('countLabel');
      typeSection.style.display = hasObj ? 'block' : 'none';
      subjNotice.style.display = hasSubj ? 'block' : 'none';
      countLabel.textContent = hasObj ? '🔢 유형당 문제 수' : '🔢 문제 수';
    }}

    /* 모의고사 문제 번호 동적 로드 */
    async function loadMockQuestions() {{
      const year = getVal('examYear');
      const month = getVal('examMonth');
      const section = document.getElementById('mockAdvancedSection');
      const chips = document.getElementById('mockQuestionChips');
      const sources = getChecked('sources');
      // 모의고사 소스가 체크됐고 연도+월 둘 다 선택됐을 때만 표시
      if (!sources.includes('모의고사') || !year || !month) {{
        section.style.display = 'none';
        chips.innerHTML = '<span style="color:#999;font-size:13px">연도와 월을 선택하면 문제 번호가 표시됩니다.</span>';
        return;
      }}
      section.style.display = 'block';
      chips.innerHTML = '<span style="color:#999;font-size:13px">로딩 중...</span>';
      try {{
        const resp = await fetch('/api/mock_questions?year=' + year + '&month=' + month);
        const data = await resp.json();
        if (!data.questions || data.questions.length === 0) {{
          chips.innerHTML = '<span style="color:#e74c3c;font-size:13px">해당 시험의 문제를 찾을 수 없습니다.</span>';
          return;
        }}
        let html = '';
        for (const q of data.questions) {{
          const label = q.question_no + '번 (' + (q.sub_type || '미분류') + ')';
          html += "<label style='font-size:13px'><input type='checkbox' name='mockQnos' value='" + q.question_no + "'><span>" + label + "</span></label>";
        }}
        chips.innerHTML = html;
      }} catch(e) {{
        chips.innerHTML = '<span style="color:#e74c3c;font-size:13px">로딩 실패: ' + e.message + '</span>';
      }}
    }}

    function toggleAllMockQ(checked) {{
      document.querySelectorAll("input[name='mockQnos']").forEach(function(cb) {{ cb.checked = checked; }});
    }}

    // 기출 소스 변경 시 모의고사 고급 섹션도 갱신
    document.addEventListener('change', function(e) {{
      if (e.target && e.target.name === 'sources') loadMockQuestions();
    }});

    async function startGenerate() {{
      const btn = document.getElementById('btnGenerate');
      const area = document.getElementById('resultArea');
      btn.disabled = true;
      btn.textContent = '생성 중...';
      area.innerHTML = '<div style="text-align:center;padding:20px"><span class="spinner"></span> AI가 문제를 생성하고 있습니다... (30초~2분 소요)</div>';

      const types = getChecked('types');
      const sources = getChecked('sources');
      const outputOpts = getChecked('output_opts');
      const fmts = getChecked('formats');

      if (fmts.length === 0) {{
        area.innerHTML = '<div class="gen-result-card error">⚠️ 객관식 또는 주관식 중 최소 1개를 선택해주세요.</div>';
        btn.disabled = false;
        btn.textContent = '⚡ 문제 생성 시작';
        return;
      }}
      if (fmts.includes('객관식') && types.length === 0) {{
        area.innerHTML = '<div class="gen-result-card error">⚠️ 객관식을 선택했으면 최소 1개 유형을 선택해주세요.</div>';
        btn.disabled = false;
        btn.textContent = '⚡ 문제 생성 시작';
        return;
      }}

      const payload = {{
        formats: fmts,
        types: types,
        count: parseInt(getVal('count')) || 3,
        sources: sources.length > 0 ? sources : null,
        textbook: getVal('textbook') || null,
        unit: getVal('unit') ? parseInt(getVal('unit')) : null,
        subject: getVal('subject') || null,
        semester: getVal('semester') || null,
        exam_year: getVal('examYear') ? parseInt(getVal('examYear')) : null,
        exam_month: getVal('examMonth') ? parseInt(getVal('examMonth')) : null,
        max_examples: parseInt(getVal('maxExamples')) || 15,
        word: outputOpts.includes('word'),
        no_answers: outputOpts.includes('no_answers'),
        required_question_nos: getChecked('mockQnos').map(Number).filter(n => n > 0),
      }};

      try {{
        const resp = await fetch('/api/generate', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify(payload)
        }});
        const data = await resp.json();

        if (data.success) {{
          let html = '<div class="gen-result-card">✅ <b>생성 완료!</b></div>';

          for (const res of data.results) {{
            const meta = res.metadata;
            const tb = res.textbook_used;
            html += '<div class="card" style="margin-top:12px">';
            html += '<h2>[ ' + meta.sub_type + ' ] — ' + (meta.source_label || meta.source_type) + ' 기반</h2>';
            html += '<div class="meta-row">';
            if (tb && tb.textbook_label && tb.unit_no) {{
              html += '<span><strong>교과서</strong>: ' + tb.textbook_label + ' ' + tb.unit_no + '과</span>';
            }} else if (meta.source_type === '모의고사') {{
              html += '<span><strong>소스</strong>: 모의고사 지문 활용</span>';
            }}
            html += '<span><strong>기출 예시</strong>: ' + res.examples_used + '개 참조</span>' +
              '<span><strong>생성</strong>: ' + meta.count_generated + '/' + meta.count_requested + '문항</span>';
            if (meta.review) {{
              const rv = meta.review;
              let reviewColor = rv.final_count === rv.total_generated ? '#27ae60' : '#e67e22';
              html += '<span style="color:' + reviewColor + '"><strong>검수</strong>: ' + rv.final_count + '/' + rv.total_generated + '문항 합격';
              if (rv.retried) html += ' (재시도 ' + rv.retry_count + '회)';
              html += '</span>';
              if (rv.simulation) {{
                const sim = rv.simulation;
                html += '<span style="color:#8e44ad"><strong>시뮬레이션</strong>: 난이도 ' + (sim.difficulty || '?');
                if (sim.mismatches > 0) html += ' / <span style="color:#e74c3c">정답 불일치 ' + sim.mismatches + '건</span>';
                if (sim.alternatives > 0) html += ' / 복수정답 가능 ' + sim.alternatives + '건';
                html += '</span>';
              }}
            }}
            html += '</div>';

            for (const q of res.questions) {{
              html += '<div class="qcard">';
              html += '<div class="qno"><b>[' + q.question_no + '번]</b> ' + (q.sub_type || '');
              if (q.review_score) {{
                let sc = q.review_score;
                let scColor = sc >= 8 ? '#27ae60' : sc >= 6 ? '#e67e22' : '#e74c3c';
                html += ' <span style="background:' + scColor + ';color:#fff;padding:1px 6px;border-radius:8px;font-size:11px">검수 ' + sc + '/10</span>';
              }}
              if (q.review_retry) {{
                html += ' <span style="background:#3498db;color:#fff;padding:1px 6px;border-radius:8px;font-size:11px">재생성</span>';
              }}
              if (q.distractor_created) {{
                html += ' <span style="background:#8e44ad;color:#fff;padding:1px 6px;border-radius:8px;font-size:11px">선지생성</span>';
              }}
              if (q.sim_mismatch) {{
                html += ' <span style="background:#e74c3c;color:#fff;padding:1px 6px;border-radius:8px;font-size:11px">⚠ 시뮬 불일치</span>';
              }} else if (q.sim_alternative) {{
                html += ' <span style="background:#e67e22;color:#fff;padding:1px 6px;border-radius:8px;font-size:11px">복수정답?</span>';
              }} else if (q.sim_confidence && q.sim_confidence >= 8) {{
                html += ' <span style="background:#27ae60;color:#fff;padding:1px 6px;border-radius:8px;font-size:11px">시뮬 ✓</span>';
              }}
              html += '</div>';
              html += '<div class="qtext">' + (q.question_text || '') + '</div>';
              if (q.passage) {{
                html += '<div class="passage">' + q.passage + '</div>';
              }}
              if (q.choices && q.choices.length > 0) {{
                html += '<ul class="choices">';
                q.choices.forEach(function(c, i) {{
                  const isAns = (q.answer === i + 1);
                  html += '<li' + (isAns ? ' class="answer"' : '') + ' data-n="' + (i+1) + '">' +
                    c + (isAns ? ' <span class="answer-badge">✓ 정답</span>' : '') + '</li>';
                }});
                html += '</ul>';
              }}
              if (q.answer && !q.choices) {{
                let ansText = q.answer;
                if (typeof ansText === 'object') {{
                  ansText = Object.entries(ansText).map(([k,v]) => k + ': ' + v).join('<br>');
                }}
                html += '<div style="margin-top:8px"><b>모범답안:</b> ' + ansText + '</div>';
              }}
              if (q.explanation) {{
                html += '<div style="margin-top:6px;font-size:12px;color:#666"><b>해설:</b> ' + q.explanation + '</div>';
              }}
              if (q.translation) {{
                html += '<div style="margin-top:6px;font-size:12px;color:#2980b9;background:#eaf2f8;padding:8px;border-radius:4px"><b>[해석]</b> ' + q.translation + '</div>';
              }}
              html += '</div>';
            }}
            html += '</div>';
          }}

          if (data.word_file) {{
            html += '<div style="text-align:center;margin-top:16px">' +
              '<a href="/api/download?file=' + encodeURIComponent(data.word_file) + '" class="btn" style="background:#27ae60;font-size:15px;padding:10px 24px">📥 Word 파일 다운로드</a>' +
              '</div>';
          }}

          if (data.errors && data.errors.length > 0) {{
            html += '<div class="gen-result-card" style="background:#fff3cd;border-left:4px solid #ffc107;margin-top:12px">';
            html += '<b>⚠️ 일부 생성 오류:</b><ul style="margin:6px 0;font-size:13px">';
            data.errors.forEach(function(e) {{ html += '<li>' + e + '</li>'; }});
            html += '</ul></div>';
          }}

          area.innerHTML = html;
        }} else {{
          area.innerHTML = '<div class="gen-result-card error">❌ <b>생성 실패</b>: ' + (data.error || '알 수 없는 오류') + '</div>';
        }}
      }} catch(e) {{
        area.innerHTML = '<div class="gen-result-card error">❌ 네트워크 오류: ' + e.message + '</div>';
      }}

      btn.disabled = false;
      btn.textContent = '⚡ 문제 생성 시작';
    }}
    </script>
    """
    return page_wrap("문제 생성", body)


def api_mock_questions(qs):
    """모의고사 연도/월에 해당하는 문제 번호+유형 목록 반환."""
    year = qs.get("year", [None])[0]
    month = qs.get("month", [None])[0]
    if not year or not month:
        return {"questions": [], "error": "연도와 월을 지정해주세요."}
    conn = get_conn()
    rows = conn.execute("""
        SELECT q.question_no, q.sub_type_pred, LENGTH(q.passage_text) as plen
        FROM questions q JOIN exams e ON q.exam_id=e.id
        WHERE e.source_type='모의고사' AND e.exam_year=? AND e.exam_month=?
              AND q.passage_text IS NOT NULL AND LENGTH(q.passage_text) > 50
        ORDER BY q.question_no
    """, (int(year), int(month))).fetchall()
    conn.close()
    return {
        "questions": [
            {"question_no": r["question_no"], "sub_type": r["sub_type_pred"] or "미분류", "passage_len": r["plen"]}
            for r in rows
        ]
    }


def api_generate(post_data):
    """문제 생성 API 엔드포인트."""
    from pipeline.question_generator import generate_questions
    from pipeline.word_output import generate_ai_exam_docx, generate_ai_exam_docx_multi

    try:
        payload = json.loads(post_data)
    except json.JSONDecodeError:
        return {"success": False, "error": "잘못된 JSON 요청"}

    formats = payload.get("formats", ["객관식"])
    # 하위 호환: 이전 format 단일값도 지원
    if not formats and payload.get("format"):
        formats = [payload["format"]]
    obj_types = payload.get("types", ["빈칸추론"])
    count = payload.get("count", 3)
    sources = payload.get("sources") or [None]
    textbook = payload.get("textbook")
    unit = payload.get("unit")
    subject = payload.get("subject")
    semester = payload.get("semester")
    exam_year = payload.get("exam_year")
    exam_month = payload.get("exam_month")
    max_examples = payload.get("max_examples", 15)
    want_word = payload.get("word", False)
    no_answers = payload.get("no_answers", False)
    required_question_nos = payload.get("required_question_nos") or []

    # 형식별 (소스 × 유형) 조합 생성
    gen_tasks = []
    for fmt in formats:
        if fmt == "객관식":
            for sub_type in obj_types:
                gen_tasks.append((fmt, sub_type))
        else:  # 주관식
            gen_tasks.append((fmt, "서술형"))

    results = []
    errors = []

    for source in sources:
        for q_format, sub_type in gen_tasks:
            try:
                result = generate_questions(
                    count=count,
                    sub_type=sub_type,
                    source_type=source,
                    question_type=q_format,
                    textbook_label=textbook,
                    unit_no=unit,
                    subject=subject,
                    semester_exam=semester,
                    exam_year=exam_year,
                    exam_month=exam_month,
                    max_examples=max_examples,
                    required_question_nos=required_question_nos if source == '모의고사' else None,
                )
                if result.get("success"):
                    result["metadata"]["source_label"] = source or "전체"
                    results.append(result)
                else:
                    errors.append(f"[{source or '전체'}/{q_format}/{sub_type}] {result.get('error', '실패')}")
            except Exception as exc:
                errors.append(f"[{source or '전체'}/{q_format}/{sub_type}] {traceback.format_exc()}")

    if not results:
        return {"success": False, "error": "; ".join(errors) if errors else "생성된 문제가 없습니다."}

    # Word 파일 생성
    word_file = None
    if want_word:
        try:
            if len(results) == 1:
                word_file = str(generate_ai_exam_docx(results[0], include_answers=not no_answers))
            else:
                word_file = str(generate_ai_exam_docx_multi(results, include_answers=not no_answers))
        except Exception as exc:
            errors.append(f"Word 생성 실패: {exc}")

    # results의 questions에서 JSON 직렬화 불가능한 것 정리
    clean_results = []
    for r in results:
        clean_results.append({
            "questions": r["questions"],
            "metadata": r.get("metadata", {}),
            "textbook_used": r.get("textbook_used", {}),
            "examples_used": r.get("examples_used", 0),
        })

    return {
        "success": True,
        "results": clean_results,
        "word_file": word_file,
        "errors": errors if errors else None,
    }


# ── HTTP 핸들러 ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # 콘솔 로그 숨기기

    def send_html(self, html, status=200):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_file_download(self, filepath):
        """파일 다운로드 응답."""
        p = Path(filepath)
        if not p.exists():
            self.send_html("<h3>파일 없음</h3>", 404)
            return
        data = p.read_bytes()
        self.send_response(200)
        fname = urllib.parse.quote(p.name)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{fname}")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"
        qs     = urllib.parse.parse_qs(parsed.query)

        try:
            if path == "/" or path == "":
                self.send_html(page_home())
            elif path == "/exams":
                self.send_html(page_exams(qs))
            elif path.startswith("/exam/"):
                eid = int(path.split("/")[-1])
                self.send_html(page_exam(eid, qs))
            elif path == "/search":
                kw  = qs.get("q", [""])[0]
                pg  = int(qs.get("page", ["1"])[0])
                self.send_html(page_search(kw, pg) if kw else page_home())
            elif path == "/textbooks":
                self.send_html(page_textbooks(qs))
            elif path.startswith("/textbook/"):
                tid = int(path.split("/")[-1])
                self.send_html(page_textbook_detail(tid))
            elif path == "/subtypes":
                self.send_html(page_subtypes(qs))
            elif path == "/mock":
                self.send_html(page_mock(qs))
            elif path == "/generate":
                self.send_html(page_generate())
            elif path == "/api/mock_questions":
                self.send_json(api_mock_questions(qs))
            elif path == "/api/download":
                filepath = qs.get("file", [""])[0]
                self.send_file_download(filepath)
            else:
                self.send_html("<h3>404</h3>", 404)
        except Exception as e:
            self.send_html(f"<pre>오류: {e}</pre>", 500)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"

        content_len = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_len).decode("utf-8") if content_len else ""

        try:
            if path == "/api/generate":
                result = api_generate(post_data)
                self.send_json(result)
            else:
                self.send_json({"error": "Not found"}, 404)
        except Exception as e:
            self.send_json({"error": str(e)}, 500)


def main():
    if not DB_PATH.exists():
        print(f"DB 없음: {DB_PATH}")
        print("run_pipeline.py 를 먼저 실행하세요.")
        sys.exit(1)

    server = HTTPServer(("127.0.0.1", PORT), Handler)
    print(f"\n  DB 뷰어 시작!")
    print(f"  브라우저에서 열기 → http://localhost:{PORT}")
    print(f"  종료: Ctrl+C\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  서버 종료")


if __name__ == "__main__":
    main()
