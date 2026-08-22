"""CLI 서브커맨드 구현. asyncpg/neo4j 커넥션을 직접 열고 닫는다(lawcorpus.db 풀은 장수명 서버용)."""

from __future__ import annotations

import csv
import dataclasses
import json
from datetime import date

import asyncpg
from pgvector.asyncpg import register_vector

from lawcorpus.ingest.addendum_parser import parse_addendum
from lawcorpus.ingest.diff import added_thresholds, diff_trees
from lawcorpus.ingest.ruling_mapper import MappedRuling, map_ruling
from lawcorpus.ingest.law_api import (
    fetch_eflaw,
    fetch_law_hierarchy,
    fetch_ruling,
    list_eflaws,
    list_rulings,
)
from lawcorpus.ingest.statute_mapper import MappedStatute, map_eflaw
from lawcorpus.ingest.tree import iter_chunks
from lawcorpus.resolution import _known_law_names, parse_citation
from lawcorpus.retrieval.embedder import embed_batch


# ---------------------------------------------------------------------------
# ingest-statutes (bitemporal 신 스키마 — 현행 스냅샷 1건 적재)
# ---------------------------------------------------------------------------

async def _upsert_statute(
    conn: asyncpg.Connection, mapped: MappedStatute, parent_id: int | None, *, is_current: bool,
) -> None:
    """current_mst/enforced_on은 is_current=True(현재 시행 스냅샷)일 때만 덮어쓴다 — 안 그러면
    이력 백필 루프에서 마지막으로 처리한 과거 MST가 current_mst를 덮어써 delegation edge
    매칭이 전부 실패한다(the-book-of-moon #48 — DELEGATES/REFERS_TO/MUTATIS가 946개 추출
    가능한데도 그래프에 0건으로 들어가던 원인, 실측으로 발견)."""
    await conn.execute(
        """
        INSERT INTO statute (statute_id, name, law_type, ministry_code, current_mst, enforced_on, parent_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (statute_id) DO UPDATE SET
            current_mst = CASE WHEN $8 THEN EXCLUDED.current_mst ELSE statute.current_mst END,
            enforced_on = CASE WHEN $8 THEN EXCLUDED.enforced_on ELSE statute.enforced_on END,
            parent_id = COALESCE(EXCLUDED.parent_id, statute.parent_id)
        """,
        mapped.statute_id, mapped.name, mapped.law_type, mapped.ministry_code,
        mapped.current_mst, mapped.enforced_on, parent_id, is_current,
    )


async def _get_or_create_article(
    conn: asyncpg.Connection, statute_id: int, art_no: int, branch_no: int, chapter_title: str | None,
) -> int:
    return await conn.fetchval(
        """
        INSERT INTO article (statute_id, art_no, art_branch_no, chapter_title)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (statute_id, art_no, art_branch_no) DO UPDATE SET
            chapter_title = COALESCE(EXCLUDED.chapter_title, article.chapter_title)
        RETURNING article_id
        """,
        statute_id, art_no, branch_no, chapter_title,
    )


async def _insert_article_versions(conn: asyncpg.Connection, mapped: MappedStatute) -> int:
    """article_version_no_overlap이 DEFERRABLE INITIALLY DEFERRED이므로 반드시 close_versions()와
    같은 트랜잭션 안에서 호출해야 한다 — 새 버전은 valid_to=NULL(열린 구간)로 먼저 들어가
    기존 열린 구간과 일시적으로 겹치고, close_versions()가 커밋 전에 이를 좁혀준다."""
    inserted = 0
    for v in mapped.versions:
        article_id = await _get_or_create_article(conn, mapped.statute_id, v.art_no, v.branch_no, v.chapter_title)
        result = await conn.execute(
            """
            INSERT INTO article_version
                (moleg_article_key, article_id, title, body, tree, valid_from,
                 promulgated_on, promulgation_no, revision_type, is_full_rewrite, revision_reason)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (article_id, valid_from) DO NOTHING
            """,
            v.moleg_article_key, article_id, v.title, v.body, json.dumps(v.tree),
            v.valid_from, v.promulgated_on, v.promulgation_no, v.revision_type, v.is_full_rewrite,
            v.revision_reason,
        )
        if result == "INSERT 0 1":
            inserted += 1
    return inserted


async def close_versions(conn: asyncpg.Connection, statute_id: int) -> int:
    """각 조문의 버전을 valid_from 순으로 훑어 valid_to(배타적 상한)를 다음 버전의
    valid_from으로 채운다. 가장 최근 버전만 valid_to=NULL(현재 유효)로 남는다.

    단일 SQL(LEAD 윈도우 함수)로 처리한다 — 예전에는 파이썬 루프에서 버전 1건당 UPDATE 1회를
    왕복했는데, 이력을 계속 추가할 때마다 통계 전체 버전을 매번 다시 훑어 왕복 횟수가 누적
    이력 건수의 제곱에 비례해 폭증했다(the-book-of-moon #41 — 이력 인제스트가 20분에 87행
    수준으로 멈춘 것처럼 보인 원인, pg_stat_activity로 실측)."""
    result = await conn.execute(
        """
        UPDATE article_version av
        SET valid_to = nxt.next_valid_from
        FROM (
            SELECT av2.article_key,
                   LEAD(av2.valid_from) OVER (PARTITION BY av2.article_id ORDER BY av2.valid_from) AS next_valid_from
            FROM article_version av2
            JOIN article a ON a.article_id = av2.article_id
            WHERE a.statute_id = $1
        ) nxt
        WHERE av.article_key = nxt.article_key
          AND av.valid_to IS DISTINCT FROM nxt.next_valid_from
        """,
        statute_id,
    )
    return int(result.removeprefix("UPDATE "))


async def _ingest_one_statute(
    conn: asyncpg.Connection, mst: str, settings, *,
    parent_id: int | None, ef_yd: str | None = None, is_current: bool = True,
) -> MappedStatute:
    ef_law = await fetch_eflaw(mst, settings, ef_yd=ef_yd)
    mapped = map_eflaw(ef_law)
    await _upsert_statute(conn, mapped, parent_id, is_current=is_current)
    async with conn.transaction():
        inserted = await _insert_article_versions(conn, mapped)
        await close_versions(conn, mapped.statute_id)
    print(f"[{mapped.name}] MST={mst} 완료: 조문버전 {len(mapped.versions)}건(신규 {inserted})")
    return mapped


async def _ingest_mst_with_history(
    conn: asyncpg.Connection, mst: str, law_name: str, settings, *, parent_id: int | None, include_history: bool,
) -> MappedStatute:
    mapped = await _ingest_one_statute(conn, mst, settings, parent_id=parent_id)

    if include_history:
        items = await list_eflaws(law_name, settings)
        seen_msts = {mst}
        historical = []
        for item in items:
            if item.mst in seen_msts:
                continue
            seen_msts.add(item.mst)
            historical.append(item)
        for item in historical:
            try:
                await _ingest_one_statute(
                    conn, item.mst, settings, parent_id=parent_id, ef_yd=item.effective_date,
                    is_current=False,
                )
            except Exception as exc:
                print(f"[{law_name}] MST={item.mst}({item.effective_date}) 이력 적재 오류: {exc}")
        print(f"[{law_name}] 이력 스냅샷 {len(historical)}건 처리 완료")

    return mapped


async def ingest_statutes(
    laws: list[str], settings, *, include_subordinate: bool = False, include_history: bool = False,
) -> None:
    """법령명별로 현재 시행 중인 eflaw 스냅샷을 적재한다. include_history=True면 list_eflaws가
    반환하는 과거 스냅샷 전량도 각자의 MST+ef_yd로 재조회해 적재한다 — build-diffs가 비교할
    버전 쌍을 만들려면 이 이력 스냅샷이 필요하다(현행 스냅샷 1건뿐이면 diff가 항상 0건).
    include_subordinate=True면 체계도(lsStmd)로 발견한 시행령/시행규칙도 같은 조문 트리
    파이프라인으로 함께 적재하고, statute.parent_id를 법률->시행령->시행규칙 순으로
    체이닝한다(lsStmd 응답 자체가 이 순서로 중첩돼 있다)."""
    if not settings.law_api_oc:
        raise SystemExit("LAWCORPUS_LAW_API_OC가 설정되지 않았습니다.")

    conn = await asyncpg.connect(dsn=settings.pg_dsn)
    try:
        for law_name in laws:
            try:
                items = await list_eflaws(law_name, settings)
                current = next((i for i in items if i.is_current), None)
                if current is None:
                    print(f"[{law_name}] 현행 스냅샷을 찾지 못함, 건너뜀")
                    continue

                mapped = await _ingest_mst_with_history(
                    conn, current.mst, law_name, settings, parent_id=None, include_history=include_history,
                )

                if include_subordinate:
                    hierarchy = await fetch_law_hierarchy(current.mst, settings)
                    parent_id = mapped.statute_id
                    for entry in hierarchy.entries:
                        if entry.mst == current.mst:
                            continue
                        sub_mapped = await _ingest_mst_with_history(
                            conn, entry.mst, entry.law_name, settings,
                            parent_id=parent_id, include_history=include_history,
                        )
                        parent_id = sub_mapped.statute_id  # 시행규칙은 시행령의 하위로 체이닝
            except Exception as exc:
                print(f"[{law_name}] 오류: {exc}")

        total = await conn.fetchval("SELECT count(*) FROM article_version")
        print(f"\n완료. article_version 전체: {total}행")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# ingest-addenda
# ---------------------------------------------------------------------------

async def _insert_addendum_items(conn: asyncpg.Connection, statute_id: int, ef_law) -> int:
    inserted = 0
    async with conn.transaction():
        for unit in ef_law.addenda:
            for item in parse_addendum(unit):
                result = await conn.execute(
                    """
                    INSERT INTO addendum
                        (statute_id, promulgation_no, clause_no, body, kind, applies_from, target_articles)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (statute_id, promulgation_no, clause_no) DO NOTHING
                    """,
                    statute_id, item.promulgation_no, item.clause_no, item.body,
                    item.kind, item.applies_from, [],
                )
                if result == "INSERT 0 1":
                    inserted += 1
    return inserted


async def ingest_addenda(laws: list[str], settings) -> None:
    """이미 ingest_statutes로 적재된 법령의 현행 스냅샷에서 부칙만 추가로 파싱해 적재한다.
    target_articles(조문 인용)는 아직 채우지 않는다 — resolve_citation(#30) 완료 후 별도 백필."""
    if not settings.law_api_oc:
        raise SystemExit("LAWCORPUS_LAW_API_OC가 설정되지 않았습니다.")

    conn = await asyncpg.connect(dsn=settings.pg_dsn)
    try:
        for law_name in laws:
            try:
                items = await list_eflaws(law_name, settings)
                current = next((i for i in items if i.is_current), None)
                if current is None:
                    print(f"[{law_name}] 현행 스냅샷을 찾지 못함, 건너뜀")
                    continue

                ef_law = await fetch_eflaw(current.mst, settings)
                statute_id = int(ef_law.law_id)
                inserted = await _insert_addendum_items(conn, statute_id, ef_law)
                print(f"[{law_name}] 부칙 {len(ef_law.addenda)}단위 처리, 항목 신규 {inserted}건")
            except Exception as exc:
                print(f"[{law_name}] 오류: {exc}")

        total = await conn.fetchval("SELECT count(*) FROM addendum")
        print(f"\n완료. addendum 전체: {total}행")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# build-diffs
# ---------------------------------------------------------------------------

async def _diffable_pairs(conn: asyncpg.Connection, statute_id: int) -> list[tuple[dict, dict]]:
    """statute의 각 조문에서 valid_from 순으로 인접한 (from_version, to_version) 행 쌍을 찾는다."""
    rows = await conn.fetch(
        """
        SELECT av.article_key, av.article_id, av.tree, av.valid_from, av.revision_reason
        FROM article_version av
        JOIN article a ON a.article_id = av.article_id
        WHERE a.statute_id = $1
        ORDER BY av.article_id, av.valid_from
        """,
        statute_id,
    )
    by_article: dict[int, list] = {}
    for row in rows:
        by_article.setdefault(row["article_id"], []).append(row)

    return [
        (versions[i], versions[i + 1])
        for versions in by_article.values()
        for i in range(len(versions) - 1)
    ]


def _load_tree(raw: object) -> dict:
    return json.loads(raw) if isinstance(raw, str) else raw


async def build_diffs(laws: list[str], settings) -> None:
    """이미 적재된 조문 버전들 사이에서 연속된 쌍마다 diff + added_thresholds를 계산한다.
    현재 버전이 1개뿐인 조문은 비교 대상이 없어 건너뛴다 — 역사 스냅샷을 더 적재해야 diff가 생긴다.
    INSERT는 executemany로 한 번에 보낸다 — 쌍 1건당 개별 왕복이면 이력이 깊은 법령(수만 쌍)에서
    네트워크 왕복 시간만으로 수 분씩 걸린다(the-book-of-moon #43 — 조특법/소득세법 실측)."""
    conn = await asyncpg.connect(dsn=settings.pg_dsn)
    try:
        for law_name in laws:
            row = await conn.fetchrow("SELECT statute_id FROM statute WHERE name = $1", law_name)
            if row is None:
                print(f"[{law_name}] statute를 찾지 못함(ingest-statutes 선행 필요), 건너뜀")
                continue

            pairs = await _diffable_pairs(conn, row["statute_id"])
            records = []
            for from_row, to_row in pairs:
                diff = diff_trees(_load_tree(from_row["tree"]), _load_tree(to_row["tree"]))
                thresholds = [dataclasses.asdict(t) for t in added_thresholds(diff)]
                records.append((
                    from_row["article_key"], to_row["article_key"], json.dumps(diff),
                    json.dumps(thresholds), to_row["revision_reason"], "법제처",
                ))

            before = await conn.fetchval("SELECT count(*) FROM article_diff")
            async with conn.transaction():
                await conn.executemany(
                    """
                    INSERT INTO article_diff
                        (from_version, to_version, diff, added_thresholds, reason_text, reason_source)
                    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6)
                    ON CONFLICT (from_version, to_version) DO NOTHING
                    """,
                    records,
                )
            after = await conn.fetchval("SELECT count(*) FROM article_diff")
            print(f"[{law_name}] 비교 가능 쌍 {len(pairs)}건, diff 신규 {after - before}건")

        total = await conn.fetchval("SELECT count(*) FROM article_diff")
        print(f"\n완료. article_diff 전체: {total}행")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# ingest-rulings (prec/expc/detc/admrul 통합)
# ---------------------------------------------------------------------------

def _try_parse_yyyymmdd(raw: str) -> date | None:
    """목록 API의 날짜 필드는 소스에 따라 'YYYYMMDD'와 'YYYY.MM.DD' 두 형식이 섞여 온다
    (실측 — the-book-of-moon #44, prec 검색 결과 중 법원명이 비어있는 하급심 묶음이
    점 구분 형식을 준다). 점을 먼저 제거해 두 형식을 모두 받는다."""
    s = raw.strip().replace(".", "").replace(" ", "")
    if len(s) != 8 or not s.isdigit():
        return None
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def _law_prefix_text(parsed: dict) -> str:
    """parse_citation이 성공한 결과에서 '법령명(+구법 시점 괄호)' 접두어 문자열을 재구성한다.
    콤마로 나열된 다음 조문("국세기본법 제14조, 제15조")은 법령명이 생략되므로, 직전
    성공한 인용의 접두어를 이어붙여 재시도해야 한다(아래 _resolve_ref_articles)."""
    if parsed["historical_date"]:
        d = parsed["historical_date"]
        return f"구 {parsed['law']}({d.year}. {d.month}. {d.day}. 법률로 개정되기 전의 것) "
    return f"{parsed['law']} "


async def _resolve_ref_articles(
    conn: asyncpg.Connection, refs: tuple[str, ...], law_names: list[str], decided_on: date,
) -> tuple[list[int], list[int]]:
    """ruling.ref_articles(참조조문 원문 조각들)를 조문 버전으로 해소한다.

    lawcorpus.resolution.resolve_citation과 같은 파싱 로직(parse_citation)을 쓰지만, 전역
    pool(get_pool)이 아니라 ingest_rulings가 이미 열어둔 conn을 그대로 재사용한다 —
    commands.py의 다른 함수들과 마찬가지로 커넥션을 직접 열고 닫는 방식을 유지한다.

    법제처 참조조문 원문은 "국세기본법 제14조, 제15조, 제47조 제2항"처럼 같은 법령의 여러
    조문을 콤마로 나열할 때 두 번째부터 법령명을 생략한다(실측 확인, the-book-of-moon
    adversarial review). _split_refs가 콤마 단위로 쪼개면 이 생략된 조각은 법령명이 없어
    parse_citation이 통째로 실패한다 — 직전에 성공한 인용의 법령명(+구법 시점)을 이어붙여
    재시도해서 이 조각도 같은 법령·같은 시점으로 해소한다."""
    article_keys: list[int] = []
    article_ids: list[int] = []
    last_prefix: str | None = None
    for ref in refs:
        parsed = parse_citation(ref, law_names)
        if parsed is None and last_prefix is not None:
            parsed = parse_citation(last_prefix + ref, law_names)
        if parsed is None:
            continue
        last_prefix = _law_prefix_text(parsed)
        anchor = parsed["historical_date"] or decided_on
        row = await conn.fetchrow(
            """
            SELECT av.article_key, av.article_id FROM article_version av
            JOIN article a ON a.article_id = av.article_id
            JOIN statute s ON s.statute_id = a.statute_id
            WHERE s.name = $1 AND a.art_no = $2 AND a.art_branch_no = $3
              AND av.valid_from <= $4 AND (av.valid_to IS NULL OR av.valid_to > $4)
            """,
            parsed["law"], parsed["art_no"], parsed["branch_no"], anchor,
        )
        if row:
            article_keys.append(row["article_key"])
            article_ids.append(row["article_id"])
    return article_keys, article_ids


async def _upsert_ruling(
    conn: asyncpg.Connection, mapped: MappedRuling, *, cited_articles: list[int], cited_article_ids: list[int],
) -> bool:
    result = await conn.execute(
        """
        INSERT INTO ruling
            (ruling_id, source, case_no, decided_on, outcome, gist, body,
             body_available, cited_articles, cited_article_ids, anti_avoidance, raw_uri)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (ruling_id) DO NOTHING
        """,
        mapped.ruling_id, mapped.source, mapped.case_no, mapped.decided_on, mapped.outcome,
        mapped.gist, mapped.body, mapped.body_available, cited_articles, cited_article_ids,
        list(mapped.anti_avoidance), mapped.raw_uri,
    )
    return result == "INSERT 0 1"


async def ingest_rulings(target: str, queries: list[str], settings, *, max_pages: int = 10) -> None:
    """prec/expc/detc/admrul target으로 검색해 상세를 가져와 ruling 테이블에 적재한다.
    참조조문 원문(ref_articles — prec/detc만 구조화 필드 제공, expc/admrul은 항상 빈 튜플)을
    resolve_citation과 같은 파싱 로직으로 해소해 cited_articles/cited_article_ids를 채운다."""
    if not settings.law_api_oc:
        raise SystemExit("LAWCORPUS_LAW_API_OC가 설정되지 않았습니다.")

    conn = await asyncpg.connect(dsn=settings.pg_dsn)
    try:
        law_names = await _known_law_names(conn)
        seen_ids: set[str] = set()
        inserted = 0
        for query in queries:
            items = await list_rulings(target, query, settings, max_pages=max_pages)
            for item in items:
                if item.ruling_id in seen_ids:
                    continue
                seen_ids.add(item.ruling_id)
                try:
                    raw = await fetch_ruling(target, item.ruling_id, settings)
                    mapped = map_ruling(raw)
                    if mapped is None:
                        decided_on = _try_parse_yyyymmdd(item.decided_at)
                        if decided_on is None:
                            print(f"[{target}:{item.ruling_id}] 상세 조회 실패 + 날짜 없음, 건너뜀")
                            continue
                        # 상세 조회 실패(예: "일치하는 판례가 없습니다") — 목록 메타데이터만으로
                        # body_available=False 행을 남긴다(완전히 드롭하면 존재 자체를 잊는다).
                        mapped = MappedRuling(
                            ruling_id=item.ruling_id, source=item.source, case_no=item.case_no or None,
                            decided_on=decided_on, outcome=None, gist=item.title,
                            body="", body_available=False, anti_avoidance=(), raw_uri="",
                        )
                    cited_articles, cited_article_ids = await _resolve_ref_articles(
                        conn, mapped.ref_articles, law_names, mapped.decided_on,
                    )
                    if await _upsert_ruling(
                        conn, mapped, cited_articles=cited_articles, cited_article_ids=cited_article_ids,
                    ):
                        inserted += 1
                except Exception as exc:
                    print(f"[{target}:{item.ruling_id}] 오류: {exc}")

        print(f"\n완료. 조회 {len(seen_ids)}건, 신규 {inserted}건")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# eval-citations
# ---------------------------------------------------------------------------

_EVAL_LAW_NAMES = [
    "국세기본법", "국세기본법 시행령", "소득세법", "법인세법", "부가가치세법",
    "상속세 및 증여세법", "상속세 및 증여세법 시행령", "조세특례제한법", "조세특례제한법 시행령",
    "조특법", "국기법",  # resolve_citation의 실제 경로는 _known_law_names가 _LAW_ABBREV.keys()도
                          # 후보에 넣는다 — 여기서도 같은 후보 집합으로 맞춰야 파서 자체의
                          # 정확도가 정확히 측정된다(약칭 누락은 파서 결함이 아니라 후보 목록 결함).
]


async def eval_citations(golden_path: str, settings) -> None:
    """resolve_citation의 핵심(parse_citation)을 골든셋으로 정확도 측정한다.

    설계문서 9절: "5단계에서 정확도를 반드시 수치로 측정하고 넘어간다." DB 접속 없이
    파싱 단계만 측정한다 — 법령명 인식/조문번호 추출이 이 문제의 핵심이고, 시점 해소는
    이미 적재된 코퍼스 범위에 따라 결과가 달라져 "파서 자체의 정확도"와 분리해서 봐야 한다.
    """
    from pathlib import Path

    from lawcorpus.resolution import parse_citation

    tp = fp = fn = tn = 0
    for line in Path(golden_path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        case = json.loads(line)
        expected = case["expected"]
        result = parse_citation(case["text"], _EVAL_LAW_NAMES)
        predicted = (
            {"law": result["law"], "art_no": result["art_no"], "branch_no": result["branch_no"]}
            if result is not None else None
        )

        if expected is None and predicted is None:
            tn += 1
        elif expected is None and predicted is not None:
            fp += 1
        elif expected is not None and predicted is None:
            fn += 1
        elif predicted == expected:
            tp += 1
        else:
            fp += 1
            fn += 1  # 엉뚱한 조문을 정답처럼 반환 — 정밀도와 재현율 둘 다 깎는다

    total = tp + fp + fn + tn
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    print(f"총 {total}건: TP={tp} FP={fp} FN={fn} TN={tn}")
    print(f"precision={precision:.3f} recall={recall:.3f}")


# ---------------------------------------------------------------------------
# load-rewrite-map
# ---------------------------------------------------------------------------

async def _resolve_article_id(conn: asyncpg.Connection, statute_name: str, art_no: int, branch_no: int) -> int | None:
    return await conn.fetchval(
        """
        SELECT a.article_id FROM article a JOIN statute s ON s.statute_id = a.statute_id
        WHERE s.name = $1 AND a.art_no = $2 AND a.art_branch_no = $3
        """,
        statute_name, art_no, branch_no,
    )


async def load_rewrite_map(csv_path: str, settings) -> None:
    """전부개정으로 조문번호가 갈아엎어진 경우의 수작업 매핑을 CSV에서 읽어 적재한다.

    CSV는 article_id(내부 serial) 대신 사람이 알 수 있는 (법령명, 조번호, 가지번호)로
    적는다 — 로더가 article_id로 해소한다. 컬럼: statute_name,from_art_no,from_branch_no,
    to_art_no,to_branch_no,note
    """
    conn = await asyncpg.connect(dsn=settings.pg_dsn)
    try:
        inserted = 0
        skipped = 0
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                from_id = await _resolve_article_id(
                    conn, row["statute_name"], int(row["from_art_no"]), int(row.get("from_branch_no") or 0)
                )
                to_id = await _resolve_article_id(
                    conn, row["statute_name"], int(row["to_art_no"]), int(row.get("to_branch_no") or 0)
                )
                if from_id is None or to_id is None:
                    print(f"매핑 실패(조문을 찾을 수 없음): {row}")
                    skipped += 1
                    continue
                result = await conn.execute(
                    "INSERT INTO article_rewrite_map (from_article_id, to_article_id, note) "
                    "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                    from_id, to_id, row.get("note", ""),
                )
                if result == "INSERT 0 1":
                    inserted += 1
        print(f"완료. 신규 {inserted}건, 실패 {skipped}건")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# embed-backfill
# ---------------------------------------------------------------------------

async def embed_backfill(settings, *, batch_size: int = 64) -> None:
    """"오늘 실제로 시행 중인" article_version만 항 단위로 청킹해 임베딩한다(결정 P) — 과거
    버전은 HNSW 인덱스에도 안 들어가 임베딩해도 검색에 안 쓰인다. 과거 시점 의미검색 수요가
    확인되면 그때 범위를 넓힌다.

    valid_to IS NULL("아직 다음 버전이 없다")과 "오늘 시행 중이다"는 다르다 — 세법은 시행일이
    몇 달 뒤인 개정을 미리 공포하는 경우가 흔해서, valid_to IS NULL인 버전의 valid_from이
    미래인 사례가 실제로 다수 있었다(실측 — the-book-of-moon adversarial review, 3,053건
    중 2,175건이 미래 valid_from). valid_to IS NULL만 보고 임베딩하면 "현행"이라는 이름으로
    아직 시행되지 않은 조문을 검색에 노출하고, 정작 오늘 시행 중인 버전은 하나도 임베딩되지
    않아 검색 자체가 안 되는 조문이 다수 생긴다."""
    conn = await asyncpg.connect(dsn=settings.pg_dsn)
    await register_vector(conn)
    try:
        rows = await conn.fetch(
            "SELECT article_key, tree FROM article_version "
            "WHERE valid_from <= CURRENT_DATE AND (valid_to IS NULL OR valid_to > CURRENT_DATE)"
        )

        # 오늘 시행 중인 article_key 집합 밖에 있는 낡은 is_current 표시를 내린다 —
        # 개정으로 교체된 과거 버전뿐 아니라, 이 수정 이전에 미래 버전이 잘못 is_current=true로
        # 임베딩됐던 행도 여기서 함께 정리된다.
        current_keys = [row["article_key"] for row in rows]
        await conn.execute(
            "UPDATE article_embedding SET is_current = false WHERE is_current AND NOT (article_key = ANY($1))",
            current_keys,
        )
        chunks: list[tuple[int, str, str]] = []
        for row in rows:
            tree = json.loads(row["tree"]) if isinstance(row["tree"], str) else row["tree"]
            for chunk_path, chunk_text in iter_chunks(tree):
                chunks.append((row["article_key"], chunk_path, chunk_text))

        print(f"청크 {len(chunks)}건 생성(조문버전 {len(rows)}건 대상), 임베딩 시작...")
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            vectors = await embed_batch([c[2] for c in batch], settings)
            records = [
                (article_key, chunk_path, chunk_text, vector, True)
                for (article_key, chunk_path, chunk_text), vector in zip(batch, vectors)
            ]
            await conn.executemany(
                """
                INSERT INTO article_embedding (article_key, chunk_path, chunk_text, embedding, is_current)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (article_key, chunk_path) DO UPDATE SET
                    chunk_text = EXCLUDED.chunk_text, embedding = EXCLUDED.embedding, is_current = EXCLUDED.is_current
                """,
                records,
            )
            print(f"  {min(i + batch_size, len(chunks))}/{len(chunks)} 처리")

        null_count = await conn.fetchval("SELECT count(*) FROM article_embedding WHERE embedding IS NULL")
        print(f"완료. article_embedding NULL embedding {null_count}건")
    finally:
        await conn.close()
