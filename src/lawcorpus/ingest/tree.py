"""조-항-호-목 계층을 article_version.tree(jsonb)로 직렬화하고, 항 단위 임베딩 청크로 순회한다."""

from __future__ import annotations

from collections.abc import Iterator

from lawcorpus.ingest.models import RawArticleUnit


def build_tree(unit: RawArticleUnit) -> dict:
    """RawArticleUnit의 항-호-목을 jsonb 저장용 dict로 변환한다. 항이 없는 단일 문단
    조문(예: 목적 조항)은 clauses가 빈 리스트가 된다 — body 컬럼에 전문이 이미 있다."""
    return {
        "clauses": [
            {
                "no": clause.no,
                "text": clause.text,
                "sub_clauses": [
                    {
                        "no": sub.no,
                        "text": sub.text,
                        "items": [{"no": item.no, "text": item.text} for item in sub.items],
                    }
                    for sub in clause.sub_clauses
                ],
            }
            for clause in unit.clauses
        ]
    }


def iter_chunks(tree: dict) -> Iterator[tuple[str, str]]:
    """(chunk_path, chunk_text) 쌍을 항 단위로 순회한다. 목까지는 쪼개지 않는다 —
    "제1항제3호"가 검색 단위, 목은 그 안에 포함된 세부 항목이라 같은 청크에 둔다."""
    for clause in tree.get("clauses", []):
        # 항번호가 없으면(단일 문단 조문) 항 구분 없이 본문 전체가 한 청크가 된다
        path = f"제{clause['no']}항" if clause["no"] else ""
        text_parts = [clause["text"]]
        for sub in clause["sub_clauses"]:
            text_parts.append(sub["text"])
            text_parts.extend(item["text"] for item in sub["items"])
        yield path, "\n".join(text_parts)


def tree_full_text(tree: dict) -> str:
    """tree(항/호/목 계층) 전체를 평문으로 이어붙인다. article_version.body는 항 구조가
    있는 조문에서는 "제14조(실질과세)" 같은 표제 한 줄뿐이고 실제 내용은 tree 안에 있다
    (실측으로 발견 — the-book-of-moon adversarial review) — body만 스캔/재순위 입력으로
    쓰면 대부분의 조문에서 내용이 통째로 빠진다."""
    parts = []
    for clause in tree.get("clauses", []):
        parts.append(clause["text"])
        for sub in clause["sub_clauses"]:
            parts.append(sub["text"])
            parts.extend(item["text"] for item in sub["items"])
    return "\n".join(parts)
