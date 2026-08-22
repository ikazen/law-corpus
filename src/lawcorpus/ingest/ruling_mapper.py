"""RawRuling(prec/expc/detc/admrul 공통) -> ruling DB row 매핑.

outcome 판정은 본질적으로 근사치다 — 실제 승소/패소는 사건 전체를 읽어야 정확히 알 수 있고
텍스트 키워드만으로는 강한 신호가 있는 경우만 잡아낸다. 애매하면 None(불명)으로 남긴다 —
잘못 단정해 find_unpatched를 오염시키는 것보다 모른다고 하는 게 낫다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from lawcorpus.ingest.models import RawRuling

_TAXPAYER_WIN_RE = re.compile(r"(원고|납세자|청구인)(?:들)?.{0,10}?청구(?:를|는)?.{0,30}?(인용|받아들|이유\s*있)")
_TAXPAYER_LOSE_RE = re.compile(r"(원고|납세자|청구인)(?:들)?.{0,10}?청구(?:를|는)?.{0,30}?(기각|이유\s*없)")

# 대법원/항소심 판결은 "원고의 청구를 인용/기각" 대신 "상고를 기각한다"/"파기하고... 환송한다"
# 식으로 결론을 낸다 — 조세소송 구조상 원고는 항상 납세자, 피고는 항상 과세관청이므로(실측
# 확인 — the-book-of-moon #45), 상고/항소를 낸 당사자와 그 결과만 알면 승패를 판정할 수 있다.
_APPEAL_DISMISS_PARTY_RE = re.compile(r"(?P<party>원고|피고)(?:들)?의?.{0,10}?(?:상고|항소)(?:를|는)?.{0,15}?기각")
_APPEAL_DISMISS_RE = re.compile(r"(?:상고|항소)를?\s*(?:모두\s*)?기각")  # 당사자 생략형("상고를 기각한다")
_APPEAL_REMAND_RE = re.compile(r"파기.{0,80}?환송")
_APPEAL_COST_LOSER_RE = re.compile(r"(?:상고|항소)비용은\s*(?P<party>원고|피고)(?:들)?(?:이|가)\s*부담")
_DEFENDANT_ARG_ACCEPTED_RE = re.compile(r"피고(?:들)?의?\s*(?:상고이유|항소이유)?\s*주장은\s*이유\s*있다")
_PLAINTIFF_ARG_ACCEPTED_RE = re.compile(r"원고(?:들)?의?\s*(?:상고이유|항소이유)?\s*주장은\s*이유\s*있다")

_JUMUN_RE = re.compile(r"\[주\s*문\]\s*(.+?)(?:\[이\s*유\]|$)", re.S)
# 실제 헌재 주문은 "위헌"이라는 단어보다 "헌법에 위반된다"/"위반되지 아니한다" 식으로
# 쓰인다 — 합헌 패턴("위반되지 아니한다")을 먼저 제외해야 위헌 판정("위반된다")이 안 섞인다.
# 구체적인 패턴을 먼저 검사해야 한다 — "한정위헌"이 "위헌"에 앞서 걸리지 않으면
# "한정위헌" 결정이 "위헌"으로 뭉개진다.
_DETC_OUTCOME_PATTERNS: tuple[tuple[str, str], ...] = (
    ("헌법불합치", "헌법불합치"),
    ("한정위헌", "한정위헌"),
    ("한정합헌", "한정합헌"),
    ("각하", "각하"),
    ("위반된다", "위헌"),
    ("위헌", "위헌"),
)

_ANTI_AVOIDANCE_KEYWORDS = ("실질과세", "부당행위계산부인", "단계거래")


def _classify_prec_outcome(text: str) -> str | None:
    has_remand = bool(_APPEAL_REMAND_RE.search(text))
    dismiss_party = _APPEAL_DISMISS_PARTY_RE.search(text)
    has_bare_dismiss = bool(_APPEAL_DISMISS_RE.search(text))

    if has_remand and (dismiss_party or has_bare_dismiss):
        return None  # 일부 파기 + 일부 기각(혼합 결과) — 안전하게 불명 처리

    if dismiss_party:
        return "납세자패" if dismiss_party.group("party") == "원고" else "납세자승"

    if has_bare_dismiss:
        cost_match = _APPEAL_COST_LOSER_RE.search(text)
        if cost_match:
            return "납세자패" if cost_match.group("party") == "원고" else "납세자승"
        return None

    if has_remand:
        if _DEFENDANT_ARG_ACCEPTED_RE.search(text):
            return "납세자패"
        if _PLAINTIFF_ARG_ACCEPTED_RE.search(text):
            return "납세자승"
        return None

    win_match = bool(_TAXPAYER_WIN_RE.search(text))
    lose_match = bool(_TAXPAYER_LOSE_RE.search(text))
    if win_match and lose_match:
        return None  # 일부인용+일부기각(혼합 결과) — 대법원 분기와 같은 원칙으로 불명 처리
    if win_match:
        return "납세자승"
    if lose_match:
        return "납세자패"
    return None


def _classify_detc_outcome(body: str) -> str | None:
    m = _JUMUN_RE.search(body)
    jumun = m.group(1) if m else body
    if "위반되지 아니한다" in jumun or "위반된다고 할 수 없다" in jumun:
        return "합헌"
    for keyword, outcome in _DETC_OUTCOME_PATTERNS:
        if keyword in jumun:
            return outcome
    return None


def _extract_anti_avoidance(text: str) -> tuple[str, ...]:
    return tuple(k for k in _ANTI_AVOIDANCE_KEYWORDS if k in text)


def _to_date(yyyymmdd: str) -> date | None:
    s = yyyymmdd.strip()
    if len(s) != 8 or not s.isdigit():
        return None  # 일부 헌재결정례는 "0       " 같은 더미 값을 준다(판시사항 등도 전부 공란)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


@dataclass(frozen=True, slots=True)
class MappedRuling:
    ruling_id: str
    source: str
    case_no: str | None
    decided_on: date
    outcome: str | None
    gist: str
    body: str
    body_available: bool
    anti_avoidance: tuple[str, ...]
    raw_uri: str
    ref_articles: tuple[str, ...] = ()


def map_ruling(raw: RawRuling) -> MappedRuling | None:
    """상세 조회가 실패한 경우(법제처 API가 "일치하는 판례가 없습니다" 류 응답을 줄 때
    decided_at이 비어 온다) None을 반환 — 호출부가 목록 메타데이터만으로
    body_available=False 행을 만들지, 건너뛸지 결정한다."""
    decided_on = _to_date(raw.decided_at)
    if decided_on is None:
        return None

    combined = f"{raw.gist}\n{raw.body}"
    if raw.source == "헌법재판소":
        outcome = _classify_detc_outcome(raw.body)
    elif raw.source in ("법제처", "행정규칙"):
        outcome = None  # 해석례/행정규칙은 승패 개념이 없다
    else:
        outcome = _classify_prec_outcome(combined)

    return MappedRuling(
        ruling_id=raw.ruling_id,
        source=raw.source,
        case_no=raw.case_no or None,
        decided_on=decided_on,
        outcome=outcome,
        gist=raw.gist,
        body=raw.body,
        body_available=bool(raw.body.strip()),
        anti_avoidance=_extract_anti_avoidance(combined),
        raw_uri=raw.raw_uri,
        ref_articles=raw.ref_articles,
    )
