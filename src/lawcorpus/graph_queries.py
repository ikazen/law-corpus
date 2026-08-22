"""그래프 질의 — Neo4j(파생 그래프)에서 논리 관계를 훑고, PG(SoT)로 시점 해소해 되돌린다.
retrieval/graph_expand.py(v0.x, 삭제됨)를 대체한다.

설계문서 5.4절의 4개 Cypher 질의를 그대로 구현 기반으로 삼는다 — "정의 불일치", "준용
사슬의 끝", "미개정 생존 구멍", "리스크 이웃"은 관계형으로 어렵고 그래프 순회가 자연스러운
부류라 Neo4j를 두는 값이 여기서 나온다.
"""

from __future__ import annotations

import asyncpg
from datetime import date

from lawcorpus.db.neo4j import get_driver
from lawcorpus.db.pg import get_pool
from lawcorpus.resolution import get_article_by_id, require_as_of
from lawcorpus.types import Article, ArticleVersion, Subgraph, TermDefinition, UnpatchedCandidate

_EXPAND_TYPES_DEFAULT = ("DELEGATES", "REFERS_TO", "MUTATIS")
_VALID_AT_CLAUSE = (
    "(rel.valid_from IS NULL OR date(rel.valid_from) <= date($as_of)) AND "
    "(rel.valid_to IS NULL OR date(rel.valid_to) > date($as_of))"
)


async def _fetch_article(conn: asyncpg.Connection, article_id: int) -> Article | None:
    row = await conn.fetchrow(
        "SELECT article_id, statute_id, art_no, art_branch_no, chapter_title FROM article WHERE article_id = $1",
        article_id,
    )
    return Article(**dict(row)) if row else None


async def expand_refs(
    article_id: int,
    as_of: date,
    hops: int = 2,
    types: tuple[str, ...] = _EXPAND_TYPES_DEFAULT,
) -> Subgraph:
    """DELEGATES/REFERS_TO/MUTATIS를 최대 hops까지 확장한다. as_of 시점에 유효한 엣지만 탄다."""
    as_of = require_as_of(as_of)
    rel_pattern = "|".join(types)
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            f"""
            MATCH path = (a:Article {{article_id: $id}})-[rels:{rel_pattern}*1..{hops}]->(other:Article)
            WHERE ALL(rel IN relationships(path) WHERE {_VALID_AT_CLAUSE})
            RETURN DISTINCT other.article_id AS article_id,
                   [rel IN relationships(path) |
                       [startNode(rel).article_id, type(rel), endNode(rel).article_id]] AS hops
            """,
            id=article_id, as_of=str(as_of),
        )
        records = [record async for record in result]

    article_ids = {article_id}
    edges: set[tuple[int, str, int]] = set()
    for record in records:
        article_ids.add(record["article_id"])
        for src, etype, dst in record["hops"]:
            edges.add((src, etype, dst))

    return Subgraph(article_ids=tuple(sorted(article_ids)), edges=tuple(sorted(edges)))


async def get_delegation_chain(article_id: int, as_of: date) -> list[ArticleVersion]:
    """법률 -> 시행령 -> 시행규칙처럼 위임을 타고 내려가는 가장 긴 경로를 시점 해소해 반환한다.

    as_of 시점에 유효한 DELEGATES 엣지만 탄다 — 이전에는 이 필터가 빠져 있어서(실측으로
    발견, adversarial review) as_of를 인자로 받으면서도 정작 그래프 순회 자체는 시점과
    무관하게 모든 엣지를 탔다. 지금은 모든 엣지의 valid_to가 NULL이라(#31 — lsDelegated가
    현재 스냅샷 기준) 결과가 우연히 맞았을 뿐, 엣지에 실제 유효기간이 들어가기 시작하면
    바로 틀린 값을 돌려주는 상태였다."""
    as_of = require_as_of(as_of)
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            f"""
            MATCH path = (a:Article {{article_id: $id}})-[rels:DELEGATES*1..3]->(next:Article)
            WHERE ALL(rel IN rels WHERE {_VALID_AT_CLAUSE})
            RETURN [n IN nodes(path)[1..] | n.article_id] AS chain
            ORDER BY length(path) DESC LIMIT 1
            """,
            id=article_id, as_of=str(as_of),
        )
        record = await result.single()

    if record is None:
        return []

    versions = []
    for target_id in record["chain"]:
        version = await get_article_by_id(target_id, as_of)
        if version is not None:
            versions.append(version)
    return versions


async def get_mutatis_terminals(article_id: int, as_of: date) -> list[ArticleVersion]:
    """준용(MUTATIS) 사슬을 타고 들어가면 도달하는, 더 이상 준용하지 않는 실효 조문들.

    as_of 시점에 유효한 MUTATIS 엣지만 탄다(get_delegation_chain과 같은 이유로 수정 —
    adversarial review에서 발견). "더 이상 준용 안 함" 판정도 같은 시점 필터를 걸어야 한다 —
    안 그러면 as_of 시점엔 무효였던 엣지 때문에 종단이 아닌 조문이 종단으로 잘못 걸러진다."""
    as_of = require_as_of(as_of)
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            f"""
            MATCH p = (a:Article {{article_id: $id}})-[:MUTATIS*1..4]->(end:Article)
            WHERE ALL(rel IN relationships(p) WHERE {_VALID_AT_CLAUSE})
              AND NOT EXISTS {{
                MATCH (end)-[rel2:MUTATIS]->()
                WHERE {_VALID_AT_CLAUSE.replace("rel.", "rel2.")}
              }}
            RETURN DISTINCT end.article_id AS article_id
            """,
            id=article_id, as_of=str(as_of),
        )
        records = [record async for record in result]

    versions = []
    for record in records:
        version = await get_article_by_id(record["article_id"], as_of)
        if version is not None:
            versions.append(version)
    return versions


async def find_term_conflicts(term: str, as_of: date) -> list[TermDefinition]:
    """같은 용어를 정의하는 조문들 — 서로 다른 법률에서 정의가 갈리는지는 statute_id로 판별한다."""
    require_as_of(as_of)
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            "MATCH (t:Term {name: $term})<-[:DEFINES]-(a:Article) "
            "RETURN DISTINCT a.article_id AS article_id, a.statute_id AS statute_id",
            term=term,
        )
        records = [record async for record in result]

    return [TermDefinition(term=term, article_id=r["article_id"], statute_id=r["statute_id"]) for r in records]


async def get_risk_neighbors(article_id: int) -> list[tuple[Article, int]]:
    """이 조문과 함께 인용되며 납세자가 패소한 조문 — 부인 리스크가 옮아 붙는 이웃."""
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            """
            MATCH (a:Article {article_id: $id})<-[:CITES]-(r:Ruling)-[:CITES]->(b:Article)
            WHERE r.outcome = '납세자패'
            RETURN b.article_id AS article_id, count(r) AS n ORDER BY n DESC
            """,
            id=article_id,
        )
        records = [record async for record in result]

    pool = get_pool()
    neighbors = []
    async with pool.acquire() as conn:
        for record in records:
            article = await _fetch_article(conn, record["article_id"])
            if article is not None:
                neighbors.append((article, record["n"]))
    return neighbors


async def find_unpatched(since: date) -> list[UnpatchedCandidate]:
    """납세자가 승소했는데 그 근거가 된 조문이 아직도 그대로 살아있는 경우 — 미개정 생존 구멍.
    실제 loophole_candidate 행으로 굳히는 건 #37(status/risk_score 배정)의 몫이다."""
    driver = get_driver()
    async with driver.session() as session:
        result = await session.run(
            """
            MATCH (r:Ruling)-[:CITES]->(a:Article)-[:HAS_VERSION]->(v:Version)
            WHERE r.outcome = '납세자승' AND date(r.decided_on) >= date($since)
              AND date(v.valid_from) <= date(r.decided_on) AND v.valid_to IS NULL
            RETURN DISTINCT a.article_id AS article_id, r.ruling_id AS ruling_id, r.decided_on AS decided_on
            ORDER BY r.decided_on DESC
            """,
            since=str(since),
        )
        records = [record async for record in result]

    return [
        UnpatchedCandidate(
            article_id=r["article_id"], ruling_id=r["ruling_id"],
            decided_on=date.fromisoformat(str(r["decided_on"])),
        )
        for r in records
    ]


async def materialize_unpatched(since: date) -> int:
    """find_unpatched의 결과를 loophole_candidate 행으로 굳힌다.

    status='alive'로 고정한다 — find_unpatched 자체가 "현재 버전이 그대로 유효한" 경우만
    돌려주므로 patched/partial/pending은 이 경로로 나올 수 없다(그건 다른 탐지 질의가
    필요하다 — 미구현, 향후 과제). pattern_type/claim_deadline은 세무사 검증(confirmed_by)
    전까지 비워둔다 — 자동 분류는 이 저장소가 피하는 LLM 판단 영역과 맞닿아 있다.
    risk_score는 anti_avoidance_rate로 근사한다(이 조문이 다른 사건에서 실질과세 등으로
    얼마나 자주 걸렸는지 — 높을수록 이 승소도 재도전받을 여지가 크다는 신호).
    """
    from lawcorpus.risk import anti_avoidance_rate

    candidates = await find_unpatched(since)
    pool = get_pool()
    inserted = 0
    async with pool.acquire() as conn:
        for candidate in candidates:
            risk_score = await anti_avoidance_rate(candidate.article_id)
            result = await conn.execute(
                """
                INSERT INTO loophole_candidate (article_id, origin_ruling, status, risk_score)
                VALUES ($1, $2, 'alive', $3)
                ON CONFLICT (article_id, origin_ruling) DO NOTHING
                """,
                candidate.article_id, candidate.ruling_id, round(risk_score, 3),
            )
            if result == "INSERT 0 1":
                inserted += 1
    return inserted
