# 결정 기록

이 문서는 living document다 — 과거 결정은 언제든 정당화 없이 뒤집을 수 있다. 이력은 "수정" 블록으로 누적한다.

## A. LLM 비의존 원칙

**결정**: 이 repo는 LLM을 호출하는 코드를 포함하지 않는다. 임베딩(Ollama `/api/embed`)과
리랭킹(Ollama `/api/rerank`)은 예외 — 생성이 아니라 벡터/스코어를 반환하는 결정론적 호출이라
"검색 원자 연산"으로 취급한다.

**왜**: [pot-of-greed](https://github.com/ikazen/pot-of-greed)에서 세법/판례 코퍼스 계층을 분리하며
발견한 제약 — `search_complex`(질의 분해 + HyDE)는 LLM 호출이 두 군데(`app.agent.decompose`,
`app.rag.hyde`) 있어 이 repo로 가져오면 특정 LLM 프로바이더 추상화에 종속된다. 소비처가 여러
프로젝트일 수 있고 각자 다른 LLM 스택을 쓸 수 있으므로, 이 repo는 `hybrid_search`(임베딩+벡터+
키워드+RRF+리랭크+그래프확장, LLM 미호출)까지만 제공하고 HyDE·질의분해·RAG 오케스트레이션은
소비처 몫으로 남긴다. pot-of-greed 쪽 근거는 `docs/decisions.md` 결정 O 참조.

---

## B. 키워드 검색 엔진

**결정**: PG tsvector, `'simple'` config.

**왜**: 조문번호·판례번호 정확매칭이 핵심이고 tsvector로 충분. 자연어 키워드 재현율 부족 시
형태소 분석기(은전한닢)/ES 추후 도입 검토. `'simple'` config는 한국어 형태소 분석을 하지 않고
공백 토큰화만 한다 — 알려진 제약으로 남겨둔다.

인용 존재검증(`refs.py::verify_refs_exist`)은 tsvector가 아니라 `law_name`+`article_no`/`case_no`
컬럼 동등성 질의를 쓴다 — tsvector는 토큰 AND 매칭이라 "법명은 오귀속, 번호만 실재"인 인용도
통과시키는 오탐이 있었다(pot-of-greed adversarial review, BON-215).

*pot-of-greed 결정 B에서 이관.*

---

## C. 임베딩 모델

**결정**: `qwen3-embedding:8b`, 1024차원.

**왜**: 온프레미스에서 운용 중인 모델. 인덱싱과 쿼리에 반드시 동일 모델·버전 사용(벡터 공간 일치).
`LawCorpusSettings.embedding_model`/`embedding_dim` 단일 참조로 강제 — DDL의 `VECTOR(1024)`와
동기화 필요.

*pot-of-greed 결정 C에서 이관.*

---

## D. 데이터 수집 / 그래프 구축

**결정**: 법제처 OPEN API(open.law.go.kr) 1차 수입. 참조조문/참조판례 구조화 필드로 판례→조문
그래프를 LLM 추출 없이 수입.

**왜**: 법제처 DRF API가 조/항/호 + 연혁/시행일자/참조조문/참조판례를 구조화 XML로 제공 → LLM
추출 없이 필드 매핑만으로 그래프까지 구성 가능.

**조문 가지번호 저장 형식**: `article_no`는 `제N조의M`(정상 한국 법령 인용 표기)로 저장한다.
`제N의M조`(조립 순서가 뒤바뀐 오표기)는 인용 검증 정규식과 어긋나 가지번호 조문 인용이 전부
환각으로 오판되는 버그였다(pot-of-greed #40 — 2026-08-20, article_chunks 869행 + Neo4j 704노드
백필로 수정). `ingest/law_mapper.py`가 `art.no.partition("의")`로 분해해 조립한다.

*pot-of-greed 결정 D에서 이관.*

---

## E. DB 구성

**결정**: 2-DB 유지 (pgvector + Neo4j).

**왜**: pgvector = 하이브리드 검색(벡터+키워드 RRF). Neo4j = 관계 탐색(인용/준용/판례변경/개정이력).
역할이 겹치지 않아 단일화 시 품질 손실.

*pot-of-greed 결정 G에서 이관.*

---

## F. Hierarchical 청킹 / small-to-big

**결정**: 계층 = 조문 한정. 검색 child = 항/호, 컨텍스트 parent = 조. 계층 표현은 pgvector
`parent_chunk_id` fetch만 사용(그래프에 계층 엣지 없음). 판례는 계층 미적용.

**왜**: 항/호로 검색해야 임베딩 희석 없이 정밀 매칭되고, 조 전체를 parent로 끌어와야 리랭커·LLM이
세법 문맥을 오독 없이 본다(small-to-big). 계층을 그래프에 넣으면 Neo4j가 관계 탐색과 계층 탐색
두 역할을 지게 되므로, 단순 fetch로 충분한 계층은 PG 한 컬럼(`parent_chunk_id`)에 두고 그래프는
인용/준용 관계에만 집중시킨다.

*pot-of-greed 결정 I에서 이관.*

---

## G. 데이터 모델: 청크 스토어 → bitemporal 법령 상태 공간

**결정**: v0.x의 `article_chunks`/`case_chunks` 플랫 청크 스토어를 폐기하고, 조문 논리
식별자(`article`)와 시행일자별 버전(`article_version`)을 분리한 bitemporal 모델로 전면
재구축한다.

**왜**: 세법 "개구멍" 발굴은 "질문에 답할 문장을 찾는" RAG 검색 문제가 아니라 조문 A와 B
사이의 시점차익·정의불일치 같은 빈틈을 조합 탐색하는 문제다. 청크 하나가 곧 현재 시점
텍스트인 구조로는 "이 시점에 이 조문이 어떤 문구였는가"를 답할 수 없다. 결정 F(hierarchical
청킹)를 대체한다 — `tree` jsonb가 `parent_chunk_id` 계층을 흡수한다. 설계 근거는
[`docs/spec.md`](spec.md).

---

## H. `as_of` 필수 규칙

**결정**: 시점에 의존하는 모든 조회 함수(`get_article`, `get_effective_law`,
`expand_refs`, `get_delegation_chain`, `get_mutatis_terminals`)는 `as_of: date`를
위치 필수 인자로 받는다. 기본값 `today`를 두지 않는다.

**왜**: 기본값을 허용하는 순간 과거 거래에 현행법을 적용하는 사고가 조용히 섞인다.
Python은 타입힌트를 런타임에 강제하지 않으므로 `resolution.require_as_of()` 런타임
가드로 이 규칙을 강제한다.

---

## I. 스키마 마이그레이션: DROP + 재생성

**결정**: 스키마 변경은 버전 게이트 마이그레이션 프레임워크(alembic 등) 없이 DROP +
재생성으로 처리한다.

**왜**: 소비처가 pot-of-greed 하나뿐이고 그마저 대대적으로 재작성될 예정이라, 지금
마이그레이션 프레임워크를 도입하면 지키는 사람 없는 버전 테이블만 남는다. **이 전제가
깨지면(소비처가 여럿으로 늘어나면) 재고 트리거** — 그때는 alembic 등 도입을 다시 검토한다.

---

## J. 참조 엣지는 Article 논리 노드 + 시간 프로퍼티

**결정**: REFERS_TO/MUTATIS/DELEGATES 엣지는 `Version` 노드가 아니라 `Article`(논리) 노드
사이에 걸고, `valid_from`/`valid_to`를 엣지 프로퍼티로 준다.

**왜**: 버전 노드 간에 엣지를 걸면 개정 때마다 엣지가 곱셈으로 늘어나 폭발한다. 논리 노드 간
엣지 + 시간 프로퍼티 방식이면 엣지 수가 조문 수에 선형이고, `as_of` 필터가 Cypher에서 단순
비교로 처리된다. 결정 E(2-DB 유지)를 강화한다 — Neo4j를 두는 실질 근거가 "가변 깊이 +
종단 조건"(준용 사슬의 끝 찾기 등) 질의로 구체화됐다.

---

## K. Neo4j 레이블 무프리픽스

**결정**: Neo4j 레이블에서 `Corpus` 프리픽스를 제거한다. 이 Neo4j 인스턴스는 lawcorpus
전용으로 간주한다.

**왜**: `docs/architecture.md`(v0.x)의 프리픽스 격리 결정(다른 워크로드와 같은 인스턴스를
공유할 가능성 대비)을 뒤집는다. Community Edition은 멀티 데이터베이스가 안 돼서 프리픽스
격리는 이름만 복잡해질 뿐 실질적 격리를 못 준다. **다른 워크로드가 이 인스턴스에 실제로
들어오면 재고 트리거.**

**재고 트리거 발동 (2026-08-22)**: issue #45 작업 중 pot-of-greed-api가 실제로 같은 Neo4j
인스턴스를 공유 중임을 확인(infra-lookup 실측) — GitHub issue #74에서 추적.

---

## L. 원본 오브젝트 스토리지 보관

**결정**: 모든 원본 XML(향후 PDF/HTML 포함)은 MinIO `lawcorpus-raw` 버킷에 불변 보관한다.
MinIO(mac-server)가 intermittent 호스트라 `LAWCORPUS_RAW_DIR` fs 폴백을 기본으로 둔다.

**왜**: 파서는 반드시 여러 번 고쳐 쓰게 된다 — 원본이 없으면 파서를 고칠 때마다 API를
다시 호출해야 한다(법제처 API가 점검 중이면 그마저 불가능, 2026-08-21 세션에서 실제로
겪음). 신규 외부 의존(mac-server)이 생기지만 기존 인프라를 그대로 재사용한다.

---

## M. 한국어 전문검색: pg_trgm

**결정**: 키워드 검색 백엔드로 `pg_trgm`을 쓴다.

**왜**: 결정 B(tsvector `'simple'`)를 대체한다. 설계문서 원안은 `pg_bigm`을 제안했으나
실측 결과 이 서버의 PostgreSQL에 `pg_bigm`이 설치 불가(`pg_available_extensions`에 없음)
확인됨. `pg_trgm`은 설치 가능하고, 조문번호 정확매칭은 라우터가 앞단에서 처리하므로
전문검색 백엔드의 형태소 정밀도 요구가 낮다.

---

## N. loophole 도메인을 같은 repo/스키마에 통합

**결정**: 세법 개구멍 발굴 시스템 전용 스키마(`loophole_candidate`, `pattern_type`,
Neo4j `Pattern`/`Loophole` 노드)를 별도 repo로 분리하지 않고 the-book-of-moon 단일
스키마에 둔다.

**왜**: 분리하면 `find_unpatched`가 두 저장소를 크로스 조인해야 해서 그래프 질의(참조
엣지 + Ruling CITES + Version 시점해소를 한 Cypher로 묶는 것)의 이점이 사라진다. 사용자가
직접 이 구조를 선택했다(2026-08-22 세션) — 일반 법령 데이터 모델과 개구멍 전용 도출물이
섞이는 트레이드오프를 감수하고 조회 성능/단순성을 우선한다.

---

## O. `article_version.article_key`는 서로게이트

**결정**: `article_key`는 `bigserial` 서로게이트 PK다. 법제처 조문키는
`moleg_article_key`(`{법령일련번호}:{조문키}` 형식 텍스트)에 별도 보관한다.

**왜**: 설계문서 원안은 법제처 조문키를 그대로 PK로 쓰는 것이었으나, 실측 결과 조문키
자체(예: "0002001")는 **법령 문서 내에서만 유일**하고 전역 유일하지 않다(다른 법령/버전에서
같은 패턴이 반복된다). 서로게이트로 전역 유일성을 확보하고, 법제처 키는 추적용으로만
`{법령일련번호}:{조문키}` 합성값으로 보관한다.

---

## P. 임베딩은 현행 버전만 우선 대상

**결정**: `embed-backfill`은 "오늘 실제로 시행 중인" 버전만 임베딩한다 — SQL로는
`valid_from <= CURRENT_DATE AND (valid_to IS NULL OR valid_to > CURRENT_DATE)`. 과거 버전은
임베딩하지 않는다.

**왜**: M6에서 이력 전량 인제스트로 `article_version`이 30만 건을 넘었다 — 전량 임베딩하면
청크 약 92.6만 건(실측 추산), 과거 버전 임베딩하면 Ollama 호출 92만 회로 계산 낭비가 크다.
HNSW 인덱스 자체가 `WHERE is_current` 부분 인덱스라 과거 버전을 임베딩해도 벡터 검색에는
안 쓰인다 — 과거 시점 질의는 이미 `get_article_timeline`/`resolve_citation`이 as_of 정확
매칭으로 답한다. 메모리는 문제가 아니었다(pgvector는 디스크+캐시로 처리, ops-vm 실측 여유
충분) — 진짜 트레이드오프는 검색에 안 쓰이는 벡터를 만드는 계산 낭비였다. **과거 시점 의미
검색(예: "1998년 이 조문은 지금과 얼마나 다른 취지였나") 수요가 실제로 확인되면 재고 트리거.**

**수정 (adversarial review, 2026-08-23)**: 최초 구현은 "현행"을 `valid_to IS NULL`("아직
다음 버전이 없다")로 판단했는데, 세법은 시행일이 몇 달 뒤인 개정을 미리 공포하는 경우가
흔해서 `valid_to IS NULL`인 버전의 `valid_from`이 미래인 사례가 실제로 다수 있었다(실측:
3,053건 중 2,175건, 71%). 이 정의로 임베딩하면 아직 시행되지 않은 조문을 "현행"으로 검색에
노출하고, 정작 오늘 시행 중인 버전은 하나도 임베딩되지 않아 키워드/벡터 검색 모두에서
사실상 안 잡히는 조문이 다수 생긴다 — 결정 H(as_of 필수 규칙)가 막으려던 바로 그 사고다.
위 SQL 조건으로 수정, `vector_search.py`의 빠른 경로 분기도 `as_of >= 오늘`에서
`as_of == 오늘`로 좁혔다(임베딩 시점과 그 사이 새 개정이 시행될 수 있는 미래 as_of는
느린 경로로 보내야 한다).
