from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

from lawcorpus.ingest.law_api import (
    _parse_admrul_detail,
    _parse_detc_detail,
    _parse_expc_detail,
    _parse_prec_detail,
)
from lawcorpus.ingest.models import RawRuling
from lawcorpus.ingest.ruling_mapper import map_ruling

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "law_api"


def _root(name: str) -> ET.Element:
    return ET.parse(_FIXTURES / name).getroot()


def test_map_ruling_prec_basic_fields():
    raw = _parse_prec_detail(_root("prec_detail.xml"))

    mapped = map_ruling(raw)

    assert mapped is not None
    assert mapped.source == "대법원"
    assert mapped.decided_on == date(2026, 5, 20)
    assert mapped.body_available is True


def test_map_ruling_expc_has_no_outcome():
    """법령해석례는 승패 개념이 없다 — 항상 None이어야 한다."""
    raw = _parse_expc_detail(_root("expc_detail.xml"))

    mapped = map_ruling(raw)

    assert mapped is not None
    assert mapped.source == "법제처"
    assert mapped.outcome is None


def test_map_ruling_detc_classified_as_hapheon():
    raw = _parse_detc_detail(_root("detc_detail.xml"))

    mapped = map_ruling(raw)

    assert mapped is not None
    assert mapped.outcome == "합헌"


def test_map_ruling_admrul_no_outcome_no_case_no():
    raw = _parse_admrul_detail(_root("admrul_detail.xml"))

    mapped = map_ruling(raw)

    assert mapped is not None
    assert mapped.source == "행정규칙"
    assert mapped.outcome is None
    assert mapped.case_no is None


def test_map_ruling_returns_none_when_detail_fetch_failed():
    """decided_at이 빈 응답(예: "일치하는 판례가 없습니다")은 매핑 실패로 처리한다."""
    raw = RawRuling(ruling_id="x", source="법원", case_no="", decided_at="", title="", gist="", body="")

    assert map_ruling(raw) is None


def test_classify_detc_outcome_wiheon():
    raw = RawRuling(
        ruling_id="x", source="헌법재판소", case_no="2020헌가1", decided_at="20200101",
        title="", gist="", body="[주 문]\n이 사건 법률조항은 헌법에 위반된다.\n[이 유]\n...",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "위헌"


def test_classify_detc_outcome_hunbeop_bulhapchi():
    raw = RawRuling(
        ruling_id="x", source="헌법재판소", case_no="2020헌가1", decided_at="20200101",
        title="", gist="", body="[주 문]\n이 사건 법률조항은 헌법불합치 결정을 선고한다.\n[이 유]\n...",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "헌법불합치"


def test_classify_prec_outcome_taxpayer_win():
    raw = RawRuling(
        ruling_id="x", source="대법원", case_no="2020두1", decided_at="20200101",
        title="", gist="원고의 청구를 인용한다.", body="",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "납세자승"


def test_classify_prec_outcome_taxpayer_win_first_instance_topic_marker():
    """실측(대법원 2020구합58847): '청구를' 대신 '청구는'+중간 수식어가 낀 실제 문구."""
    raw = RawRuling(
        ruling_id="x", source="서울행정법원", case_no="2020구합58847", decided_at="20200101",
        title="", gist="", body="그렇다면 원고의 이 사건 청구는 이유 있으므로 이를 인용하기로 하여,주문과 같이 판결한다.",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "납세자승"


def test_classify_prec_outcome_supreme_court_dismiss_plaintiff_appellant():
    """실측(대법원 2024두65119): 원고(납세자)가 상고했고 기각됐다 — 원심(패소) 유지, 납세자패."""
    raw = RawRuling(
        ruling_id="x", source="대법원", case_no="2024두65119", decided_at="20200101",
        title="", gist="", body="상고를 모두 기각한다.\n상고비용은 원고들이 부담한다.",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "납세자패"


def test_classify_prec_outcome_supreme_court_remand_defendant_argument_accepted():
    """실측(대법원 2023두37896): 피고(과세관청)의 상고이유가 받아들여져 파기환송 — 납세자패."""
    raw = RawRuling(
        ruling_id="x", source="대법원", case_no="2023두37896", decided_at="20200101",
        title="", gist="",
        body="이를 지적하는 피고의 상고이유 주장은 이유 있다. 원심판결을 파기하고, 사건을 원심법원에 환송한다.",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "납세자패"


def test_classify_prec_outcome_mixed_remand_and_dismiss_is_none():
    """실측(대법원 97누4661): 일부는 파기환송, 나머지는 기각 — 혼합 결과라 안전하게 None."""
    raw = RawRuling(
        ruling_id="x", source="대법원", case_no="97누4661", decided_at="20200101",
        title="", gist="",
        body="원심판결 중 가산세 부과처분에 관한 원고들 패소 부분을 파기하고 이 부분 사건을 다시 "
             "심리·판단하도록 하기 위하여 원심법원에 환송하기로 하며,원고들의 나머지 상고는 이를 기각하기로 한다.",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome is None


def test_classify_prec_outcome_appellate_dismiss_names_party_directly():
    """실측(서울고등법원 2023누16451): 상고비용 문구 없이 '원고의 항소를 기각한다'만 있는 경우."""
    raw = RawRuling(
        ruling_id="x", source="서울고등법원", case_no="2023누16451", decided_at="20200101",
        title="", gist="", body="제1심 판결은 정당하므로 원고의 항소를 기각한다.",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "납세자패"


def test_classify_prec_outcome_taxpayer_lose():
    raw = RawRuling(
        ruling_id="x", source="대법원", case_no="2020두1", decided_at="20200101",
        title="", gist="원고의 청구를 기각한다.", body="",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "납세자패"


def test_classify_prec_outcome_win_gap_wide_enough_for_real_phrasing():
    """실측(2024누47618, 1심 일부 인용): '청구는' 뒤에 수식어가 길게 낀 실제 표현 —
    기존 gap 제한(10자)이 너무 좁아 아예 매칭 실패(outcome=None)하던 문제."""
    raw = RawRuling(
        ruling_id="x", source="서울고등법원", case_no="2024누47618", decided_at="20200101",
        title="", gist="",
        body="그렇다면 원고의 이 사건 청구는 위 인정 범위 내에서 이유 있으므로 이를 인용하고, "
             "나머지 청구는 이유 없어 이를 기각하여야 한다.",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome == "납세자승"


def test_classify_prec_outcome_mixed_win_and_lose_in_same_text_is_none():
    """원고 청구 일부 인용 + 나머지 기각이 모두 뚜렷하게 잡히는 경우 — 대법원 분기의
    '혼합 결과는 None' 원칙과 일관되게 처리해야 한다(수정 전에는 WIN이 먼저 검사돼
    무조건 납세자승으로 오분류됐다)."""
    raw = RawRuling(
        ruling_id="x", source="서울행정법원", case_no="2020구합1", decided_at="20200101",
        title="", gist="", body="원고의 청구를 일부 인용하고 나머지 청구는 기각한다.",
    )
    mapped = map_ruling(raw)
    assert mapped.outcome is None


def test_extract_anti_avoidance_keywords():
    raw = RawRuling(
        ruling_id="x", source="대법원", case_no="2020두1", decided_at="20200101",
        title="", gist="", body="이 거래는 실질과세 원칙에 따라 부당행위계산부인 대상이다.",
    )
    mapped = map_ruling(raw)
    assert set(mapped.anti_avoidance) == {"실질과세", "부당행위계산부인"}


def test_map_ruling_body_available_false_when_body_empty():
    raw = RawRuling(
        ruling_id="x", source="법원", case_no="", decided_at="20200101", title="", gist="", body="",
    )
    mapped = map_ruling(raw)
    assert mapped.body_available is False
