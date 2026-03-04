"""
검수 큐(Review Queue) 관리 모듈

신뢰도 낮은 데이터를 JSONL 파일로 저장하고 조회한다.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Iterator

from pipeline.config import REVIEW_QUEUE_DIR


QUEUE_FILE = REVIEW_QUEUE_DIR / "review_queue.jsonl"


def add_to_review_queue(item: Dict[str, Any]):
    """문항 하나를 검수 큐에 추가."""
    item["queued_at"] = datetime.now().isoformat()
    with open(QUEUE_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


def add_batch_to_review_queue(items: List[Dict[str, Any]]):
    """문항 목록을 검수 큐에 일괄 추가."""
    for item in items:
        add_to_review_queue(item)


def iter_review_queue() -> Iterator[Dict[str, Any]]:
    """검수 큐 항목을 순서대로 순회."""
    if not QUEUE_FILE.exists():
        return
    with open(QUEUE_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def get_review_queue_count() -> int:
    if not QUEUE_FILE.exists():
        return 0
    with open(QUEUE_FILE, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def clear_review_queue():
    if QUEUE_FILE.exists():
        QUEUE_FILE.unlink()


def save_parse_failure(file_path: str, reason: str, raw_content: str = ""):
    """파싱 실패 파일을 별도 로그로 저장."""
    log_path = REVIEW_QUEUE_DIR / "parse_failures.jsonl"
    record = {
        "file_path": file_path,
        "reason": reason,
        "raw_content_snippet": raw_content[:500],
        "logged_at": datetime.now().isoformat(),
    }
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
