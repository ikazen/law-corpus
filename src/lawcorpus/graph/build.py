"""Neo4j 전량 재생성. PG(SoT)에서 읽어 그래프를 통째로 다시 만든다 — 증분 동기화 버그를
원천 차단하는 전략(설계문서 3.3). UNWIND 배치로 처리한다 — 실측 결과 이력이 깊은 법령은
30만+ Version/HAS_VERSION/SUPERSEDES가 나와 행 1건당 왕복이면 시간이 오래 걸린다.

DELEGATES/REFERS_TO/MUTATIS 엣지는 아직 PG에 저장돼 있지 않아(#31은 추출 함수만 만들었다)
이 빌더가 statute별로 lsDelegated를 다시 조회해서 즉석 반영한다 — "PG만으로 재생성"
원칙에서 벗어나는 임시 타협이다. 엣지의 valid_from/valid_to는 lsDelegated가 현재 스냅샷
기준이라 statute.enforced_on ~ NULL(열림)로 근사한다 — 정밀한 시점 이력은 다중 스냅샷을
적재해야 가능하다.
"""

from __future__ import annotations

import json

import asyncpg
from neo4j import AsyncGraphDatabase

from lawcorpus.graph.extract_refs import extract_defines, extract_delegation_edges
from lawcorpus.ingest.law_api import fetch_law_delegations
from lawcorpus.ingest.tree import tree_full_text

_MANAGED_LABELS = (
    "Statute", "Article", "Version", "Addendum", "Ruling",
    "Term", "Doctrine", "Pattern", "Loophole",
)
_BATCH_SIZE = 2000
# Neo4j 인스턴스 힙이 1.5GB(트랜잭션 풀 상한 ~1.03GiB)로 작고 pot-of-greed-api와 공유 중이다
# (실측 — infra-lookup, 2026-08-22). 실제 원인은 배치 크기가 아니라 session.run() 결과를
# consume()하지 않아 auto-commit 트랜잭션이 서버 쪽에서 완전히 끝나지 않은 채 다음 배치로
# 넘어가 트랜잭션 메모리가 회수되지 않고 쌓인 것 — consume() 추가로 해결, 배치 크기는 보수적으로만 낮춘다.


async def _run_batched(session, query: str, rows: list[dict], batch_size: int = _BATCH_SIZE) -> None:
    """UNWIND 배치로 왕복 횟수를 줄인다 — 행 1건당 session.run() 1회 왕복이면 이력이 깊은
    법령(수십만 Version)에서 그래프 재생성 자체가 수 시간씩 걸린다(the-book-of-moon #45 —
    close_versions/build_diffs와 같은 계열의 성능 버그, 실측으로 발견).

    각 결과를 반드시 consume()한다 — 안 그러면 auto-commit 트랜잭션이 서버 쪽에서 완전히
    끝나지 않은 채로 다음 session.run()이 이어져 트랜잭션 메모리 풀이 회수되지 않고 쌓인다
    (실측 — 같은 세션 안에서 wipe 다음에 이어지는 배치가 매번 MemoryPoolOutOfMemoryError로
    실패했는데, 완전히 독립된 세션으로 나눠 실행하면 각각은 성공했다)."""
    for i in range(0, len(rows), batch_size):
        result = await session.run(query, batch=rows[i:i + batch_size])
        await result.consume()


async def _wipe_managed_labels(session) -> None:
    """레이블 전체를 통째로 DETACH DELETE하면(30만+ 노드) 트랜잭션 메모리 한도(힙 1.5GB
    인스턴스에서 ~1GiB)를 넘겨 MemoryPoolOutOfMemoryError가 난다(실측) — LIMIT 배치로
    나눠서 반복 삭제한다."""
    label_match = " OR ".join(f"n:{label}" for label in _MANAGED_LABELS)
    while True:
        result = await session.run(
            f"MATCH (n) WHERE {label_match} WITH n LIMIT {_BATCH_SIZE} DETACH DELETE n RETURN count(n) AS deleted"
        )
        record = await result.single()
        await result.consume()
        if record["deleted"] == 0:
            break


async def _build_statutes(pg_conn: asyncpg.Connection, session) -> list[dict]:
    rows = await pg_conn.fetch("SELECT statute_id, name, law_type, parent_id, current_mst, enforced_on FROM statute")
    await _run_batched(
        session,
        "UNWIND $batch AS row MERGE (s:Statute {statute_id: row.id}) SET s.name = row.name, s.law_type = row.law_type",
        [{"id": r["statute_id"], "name": r["name"], "law_type": r["law_type"]} for r in rows],
    )
    parents = [{"cid": r["statute_id"], "pid": r["parent_id"]} for r in rows if r["parent_id"] is not None]
    await _run_batched(
        session,
        """
        UNWIND $batch AS row
        MATCH (child:Statute {statute_id: row.cid}), (parent:Statute {statute_id: row.pid})
        MERGE (child)-[:DELEGATES_TO]->(parent)
        """,
        parents,
    )
    return [dict(r) for r in rows]


async def _build_articles(pg_conn: asyncpg.Connection, session) -> list[dict]:
    rows = await pg_conn.fetch("SELECT article_id, statute_id, art_no, art_branch_no FROM article")
    await _run_batched(
        session,
        """
        UNWIND $batch AS row
        MERGE (a:Article {article_id: row.id})
        SET a.statute_id = row.sid, a.art_no = row.art_no, a.branch_no = row.branch_no
        """,
        [
            {"id": r["article_id"], "sid": r["statute_id"], "art_no": r["art_no"], "branch_no": r["art_branch_no"]}
            for r in rows
        ],
    )
    return [dict(r) for r in rows]


async def _build_versions(pg_conn: asyncpg.Connection, session) -> None:
    rows = await pg_conn.fetch(
        "SELECT article_key, article_id, valid_from, valid_to, title "
        "FROM article_version ORDER BY article_id, valid_from"
    )

    await _run_batched(
        session,
        """
        UNWIND $batch AS row
        MERGE (v:Version {article_key: row.key})
        SET v.valid_from = row.valid_from, v.valid_to = row.valid_to, v.title = row.title
        """,
        [
            {
                "key": r["article_key"], "valid_from": str(r["valid_from"]),
                "valid_to": str(r["valid_to"]) if r["valid_to"] else None, "title": r["title"],
            }
            for r in rows
        ],
    )
    await _run_batched(
        session,
        """
        UNWIND $batch AS row
        MATCH (a:Article {article_id: row.aid}), (v:Version {article_key: row.key})
        MERGE (a)-[:HAS_VERSION]->(v)
        """,
        [{"aid": r["article_id"], "key": r["article_key"]} for r in rows],
    )

    previous_key: dict[int, int] = {}
    supersedes: list[dict] = []
    for row in rows:
        prior = previous_key.get(row["article_id"])
        if prior is not None:
            # newer 버전이 older 버전을 SUPERSEDES(대체)한다
            supersedes.append({"prior": prior, "key": row["article_key"]})
        previous_key[row["article_id"]] = row["article_key"]
    await _run_batched(
        session,
        """
        UNWIND $batch AS row
        MATCH (older:Version {article_key: row.prior}), (newer:Version {article_key: row.key})
        MERGE (newer)-[:SUPERSEDES]->(older)
        """,
        supersedes,
    )


async def _build_delegation_edges(
    pg_conn: asyncpg.Connection, session, statutes: list[dict], settings,
) -> int:
    mst_to_statute = {row["current_mst"]: row for row in statutes if row["current_mst"]}
    article_lookup: dict[tuple[int, int, int], int] = {}
    for row in await pg_conn.fetch("SELECT article_id, statute_id, art_no, art_branch_no FROM article"):
        article_lookup[(row["statute_id"], row["art_no"], row["art_branch_no"])] = row["article_id"]

    edge_count = 0
    for statute in statutes:
        if not statute["current_mst"]:
            continue
        try:
            root = await fetch_law_delegations(statute["current_mst"], settings)
        except Exception as exc:
            print(f"[{statute['name']}] lsDelegated 조회 실패, 건너뜀: {exc}")
            continue

        valid_from = str(statute["enforced_on"]) if statute["enforced_on"] else None
        for edge in extract_delegation_edges(root):
            source_id = article_lookup.get((statute["statute_id"], edge.source_art_no, edge.source_branch_no))
            target_statute = mst_to_statute.get(edge.target_law_mst)
            if source_id is None or target_statute is None or edge.target_art_no is None:
                continue
            target_id = article_lookup.get(
                (target_statute["statute_id"], edge.target_art_no, edge.target_branch_no)
            )
            if target_id is None:
                continue

            result = await session.run(
                f"""
                MATCH (src:Article {{article_id: $sid}}), (dst:Article {{article_id: $did}})
                MERGE (src)-[r:{edge.edge_type}]->(dst)
                SET r.valid_from = $valid_from, r.valid_to = NULL
                """,
                sid=source_id, did=target_id, valid_from=valid_from,
            )
            await result.consume()
            edge_count += 1
    return edge_count


async def _build_defines_edges(pg_conn: asyncpg.Connection, session) -> int:
    rows = await pg_conn.fetch(
        "SELECT DISTINCT ON (article_id) article_id, body, tree FROM article_version "
        "WHERE valid_to IS NULL ORDER BY article_id, valid_from DESC"
    )
    edge_count = 0
    for row in rows:
        tree = json.loads(row["tree"]) if isinstance(row["tree"], str) else row["tree"]
        full_text = row["body"] + "\n" + tree_full_text(tree)
        for term in extract_defines(full_text):
            result = await session.run(
                "MERGE (t:Term {name: $term}) "
                "WITH t MATCH (a:Article {article_id: $aid}) MERGE (a)-[:DEFINES]->(t)",
                term=term, aid=row["article_id"],
            )
            await result.consume()
            edge_count += 1
    return edge_count


async def _build_patterns(pg_conn: asyncpg.Connection, session) -> None:
    rows = await pg_conn.fetch("SELECT code, description FROM pattern_type")
    for row in rows:
        result = await session.run(
            "MERGE (p:Pattern {pattern_type: $code}) SET p.description = $desc",
            code=row["code"], desc=row["description"],
        )
        await result.consume()


async def _build_rulings(pg_conn: asyncpg.Connection, session) -> int:
    """Ruling 노드 + CITES 엣지. cited_article_ids는 resolve_citation 백필 전까지 비어있어
    당장은 CITES가 거의 안 생기지만, 백필이 채워지는 즉시 다음 build-graph 재실행에서
    자동으로 반영되도록 배선은 지금 완성해둔다(#34가 get_risk_neighbors/find_unpatched로
    이 CITES 엣지를 바로 소비한다)."""
    rows = await pg_conn.fetch(
        "SELECT ruling_id, source, case_no, decided_on, outcome, cited_article_ids FROM ruling"
    )
    await _run_batched(
        session,
        """
        UNWIND $batch AS row
        MERGE (r:Ruling {ruling_id: row.id})
        SET r.source = row.source, r.case_no = row.case_no,
            r.decided_on = row.decided_on, r.outcome = row.outcome
        """,
        [
            {
                "id": r["ruling_id"], "source": r["source"], "case_no": r["case_no"],
                "decided_on": str(r["decided_on"]), "outcome": r["outcome"],
            }
            for r in rows
        ],
    )
    cites = [
        {"rid": r["ruling_id"], "aid": aid}
        for r in rows for aid in (r["cited_article_ids"] or [])
    ]
    await _run_batched(
        session,
        """
        UNWIND $batch AS row
        MATCH (r:Ruling {ruling_id: row.rid}), (a:Article {article_id: row.aid})
        MERGE (r)-[:CITES]->(a)
        """,
        cites,
    )
    return len(rows)


async def build_graph(settings) -> None:
    pg_conn = await asyncpg.connect(dsn=settings.pg_dsn)
    driver = AsyncGraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        async with driver.session() as session:
            await _wipe_managed_labels(session)

            statutes = await _build_statutes(pg_conn, session)
            print(f"Statute {len(statutes)}건")

            articles = await _build_articles(pg_conn, session)
            print(f"Article {len(articles)}건")

            await _build_versions(pg_conn, session)
            version_count = await pg_conn.fetchval("SELECT count(*) FROM article_version")
            print(f"Version {version_count}건 (HAS_VERSION/SUPERSEDES 포함)")

            edge_count = await _build_delegation_edges(pg_conn, session, statutes, settings)
            print(f"DELEGATES/REFERS_TO/MUTATIS {edge_count}건")

            defines_count = await _build_defines_edges(pg_conn, session)
            print(f"DEFINES {defines_count}건")

            await _build_patterns(pg_conn, session)
            print("Pattern 재생성 완료")

            ruling_count = await _build_rulings(pg_conn, session)
            print(f"Ruling {ruling_count}건")
    finally:
        await pg_conn.close()
        await driver.close()
