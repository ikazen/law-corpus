"""검색 파이프라인. 설계문서 6절: 라우터 -> 진입점검색(RRF) -> 그래프확장(as_of) ->
시점해소(PG) -> 부수컨텍스트(부칙+인용 심판례) -> 재순위. v0.x의 hybrid_search/promotion_score를
완전히 대체한다(결정 A — LLM 미호출, HyDE/질의분해는 소비처 몫).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import date

from lawcorpus.config import LawCorpusSettings
from lawcorpus.db.pg import get_pool
from lawcorpus.graph_queries import expand_refs
from lawcorpus.ingest.tree import tree_full_text
from lawcorpus.resolution import get_article_by_id, require_as_of
from lawcorpus.retrieval.fusion import rrf_fuse
from lawcorpus.retrieval.keyword_search import keyword_search
from lawcorpus.retrieval.reranker import rerank
from lawcorpus.retrieval.vector_search import vector_search
from lawcorpus.risk import get_rulings_for
from lawcorpus.router import route_direct
from lawcorpus.types import ArticleVersion, Hit, Ruling


@dataclass(frozen=True, slots=True)
class SearchResult:
    version: ArticleVersion
    score: float
    related: tuple[ArticleVersion, ...]       # 위임/준용/정의 그래프 확장으로 딸려온 조문 버전
    addenda: tuple[str, ...]                  # 관련 경과조치 본문 (부칙 target_articles 매칭)
    related_rulings: tuple[Ruling, ...]       # 이 조문을 인용한 판례/심판례/예규


async def _fetch_addenda(article_id: int) -> tuple[str, ...]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT body FROM addendum WHERE $1 = ANY(target_articles)", article_id)
    return tuple(r["body"] for r in rows)


async def _build_result(version: ArticleVersion, score: float, as_of: date, hops: int) -> SearchResult:
    subgraph = await expand_refs(version.article_id, as_of, hops=hops)
    related = []
    for aid in subgraph.article_ids:
        if aid == version.article_id:
            continue
        rv = await get_article_by_id(aid, as_of)
        if rv is not None:
            related.append(rv)

    addenda = await _fetch_addenda(version.article_id)
    rulings = await get_rulings_for(version.article_id)

    return SearchResult(
        version=version, score=score, related=tuple(related),
        addenda=addenda, related_rulings=tuple(rulings),
    )


async def search(
    query: str, as_of: date, settings: LawCorpusSettings, *, top_k: int = 20, hops: int = 1,
) -> list[SearchResult]:
    """query에 명시적 조문 인용이 있으면 라우터가 직접 조회로 우회한다. 아니면 키워드+벡터
    검색을 RRF로 융합하고, 각 후보를 그래프 확장 + 시점 해소 + 부수 컨텍스트로 완성한 뒤
    조문 전문(body) 기준으로 재순위한다 — 항 단위 청크가 아니라 완성된 문맥으로 재채점해야
    "이 조문이 실제로 관련 있는가"를 온전히 판단할 수 있다."""
    as_of = require_as_of(as_of)

    direct = await route_direct(query, as_of)
    if direct is not None:
        return [await _build_result(direct, 1.0, as_of, hops)]

    kw_hits, vec_hits = await asyncio.gather(
        keyword_search(query, as_of, top_n=top_k),
        vector_search(query, as_of, settings, top_n=top_k),
    )
    fused = rrf_fuse(kw_hits, vec_hits, top_n=top_k)

    candidates: list[SearchResult] = []
    for hit in fused:
        version = await get_article_by_id(hit.article_id, as_of)
        if version is not None:
            candidates.append(await _build_result(version, hit.score, as_of, hops))

    if not candidates:
        return []

    proxy_hits = [
        Hit(
            article_key=c.version.article_key, article_id=c.version.article_id, chunk_path="",
            text=f"{c.version.body}\n{tree_full_text(c.version.tree)}", score=c.score,
        )
        for c in candidates
    ]
    reranked = await rerank(query, proxy_hits, settings, top_k=top_k)

    by_article_id = {c.version.article_id: c for c in candidates}
    ordered = [by_article_id[h.article_id] for h in reranked if h.article_id in by_article_id]
    # rerank가 폴백(원본 순서 유지)이어도 스코어를 rerank 결과와 맞춰준다
    return [replace(result, score=hit.score) for result, hit in zip(ordered, reranked)]
