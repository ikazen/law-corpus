from datetime import date

import pytest

from lawcorpus.commands import _law_prefix_text, _resolve_ref_articles, _try_parse_yyyymmdd
from lawcorpus.ingest.law_api import _split_refs


def test_try_parse_yyyymmdd_accepts_plain_digits():
    assert _try_parse_yyyymmdd("20201015").isoformat() == "2020-10-15"


def test_try_parse_yyyymmdd_accepts_dotted_format():
    assert _try_parse_yyyymmdd("2020.10.15").isoformat() == "2020-10-15"


def test_try_parse_yyyymmdd_rejects_garbage():
    assert _try_parse_yyyymmdd("") is None
    assert _try_parse_yyyymmdd("미상") is None


def test_law_prefix_text_with_historical_date():
    parsed = {"law": "국세기본법", "art_no": 26, "branch_no": 2, "historical_date": date(2018, 12, 31)}
    assert _law_prefix_text(parsed) == "구 국세기본법(2018. 12. 31. 법률로 개정되기 전의 것) "


def test_law_prefix_text_without_historical_date():
    parsed = {"law": "국세기본법", "art_no": 14, "branch_no": 0, "historical_date": None}
    assert _law_prefix_text(parsed) == "국세기본법 "


async def _pg_reachable(dsn: str) -> bool:
    try:
        import asyncpg
        conn = await asyncpg.connect(dsn=dsn, timeout=3)
        await conn.close()
        return True
    except Exception:
        return False


@pytest.mark.asyncio
async def test_resolve_ref_articles_carries_forward_law_name_across_comma_list():
    """실측(대법원 605063 참조조문 원문): 콤마로 이어지는 두 번째 조문부터 법령명이
    생략된다 — 직전 인용의 법령명(+구법 시점)을 이어붙여 재시도해야 두 번째 조문도
    해소된다(수정 전에는 첫 조문만 해소되고 나머지는 통째로 유실됐다)."""
    from lawcorpus.config import get_settings
    from lawcorpus.db.pg import close_pg, init_pg
    from lawcorpus.resolution import _known_law_names
    import asyncpg

    settings = get_settings()
    if not await _pg_reachable(settings.pg_dsn):
        pytest.skip("실 DB(ops-vm)에 연결할 수 없음")

    refs = tuple(_split_refs(
        "구 국세기본법(2018. 12. 31. 법률 제16097호로 개정되기 전의 것) "
        "제26조의2 제1항 제1호(현행 제26조의2 제2항 제2호 참조), 제47조 제2항"
    ))
    assert len(refs) >= 2  # 콤마로 최소 2조각으로 쪼개졌는지 확인 (전제조건)

    await init_pg(settings.pg_dsn)
    try:
        conn = await asyncpg.connect(dsn=settings.pg_dsn)
        try:
            law_names = await _known_law_names(conn)
            article_keys, article_ids = await _resolve_ref_articles(
                conn, refs, law_names, date(2020, 1, 1),
            )
        finally:
            await conn.close()
        # 제26조의2뿐 아니라 법령명이 생략된 "제47조 제2항"도 해소돼야 한다
        assert len(article_ids) >= 2
    finally:
        await close_pg()
