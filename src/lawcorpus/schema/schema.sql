-- bitemporal 법령 상태 공간. 조문 논리 식별자(article)와 시행일자별 버전(article_version)을
-- 분리해 시점 정합 조회를 지원한다. 상세 설계 근거는 docs/spec.md 참조.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- 법령 (법률/시행령/시행규칙 각각 별도 행, parent_id로 위임관계 연결)
CREATE TABLE IF NOT EXISTS statute (
    statute_id      bigint PRIMARY KEY,
    name            text   NOT NULL,
    short_name      text,
    law_type        text   NOT NULL,             -- 법률/대통령령/부령/고시
    ministry_code   text,
    parent_id       bigint REFERENCES statute,    -- 시행령 -> 법률
    current_mst     text,                         -- lawService.do?MST= 호출 키
    abbreviations   text[],                       -- {'조특법','상증세법'} — resolve_citation이 사용
    enforced_on     date
);

-- 조문 논리 식별자 (버전 무관 — art_branch_no는 "제30조의5"의 5)
-- chapter_title: eflaw 응답의 조문여부="전문" 헤딩(예: "제3편 거주자의 양도소득에 대한 납세의무")을
-- 문서 순서대로 추적해 그 다음 조문들에 채운다. 조특법/소득세법처럼 법률 전체가 아니라 일부 편만
-- 필요한 경우 이 컬럼으로 필터링한다(편/장/절 구분 없이 가장 최근 헤딩 텍스트 그대로 저장).
CREATE TABLE IF NOT EXISTS article (
    article_id      bigserial PRIMARY KEY,
    statute_id      bigint NOT NULL REFERENCES statute,
    art_no          int    NOT NULL,
    art_branch_no   int    NOT NULL DEFAULT 0,
    chapter_title   text,
    UNIQUE (statute_id, art_no, art_branch_no)
);

-- 조문 버전 (bitemporal 핵심)
-- article_key는 서로게이트 PK — 법제처 조문키(moleg_article_key)의 전역 유일성을 아직
-- 검증하지 못해 그걸 PK로 쓰면 대량 적재 중 깨질 수 있다.
CREATE TABLE IF NOT EXISTS article_version (
    article_key       bigserial PRIMARY KEY,
    moleg_article_key text UNIQUE,   -- '{법령일련번호}:{조문키}' — 조문키 자체는 법령 문서 내에서만 유일
    article_id        bigint NOT NULL REFERENCES article,
    title             text,
    body              text   NOT NULL,
    tree              jsonb  NOT NULL,            -- 조-항-호-목 계층
    valid_from        date   NOT NULL,
    valid_to          date,                       -- exclusive 상한. NULL = 현재 유효
    promulgated_on    date   NOT NULL,
    promulgation_no   int,
    revision_type     text,                       -- 제정/일부개정/전부개정
    is_full_rewrite   boolean NOT NULL DEFAULT false,
    revision_reason   text,        -- 제개정이유내용(법제처 제공) — article_diff.reason_text 소스
    ingested_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (article_id, valid_from)
);
ALTER TABLE article_version ADD COLUMN IF NOT EXISTS revision_reason text;
COMMENT ON COLUMN article_version.valid_to IS
    'exclusive 상한: 다음 버전의 valid_from과 동일한 값이어야 한다. NULL = 현재 유효.';

-- 같은 조문의 버전 기간은 절대 겹칠 수 없다 — 애플리케이션이 아니라 DB가 막는다.
ALTER TABLE article_version DROP CONSTRAINT IF EXISTS article_version_no_overlap;
ALTER TABLE article_version ADD CONSTRAINT article_version_no_overlap
    EXCLUDE USING gist (
        article_id WITH =,
        daterange(valid_from, coalesce(valid_to, 'infinity'::date), '[)') WITH &&
    ) DEFERRABLE INITIALLY DEFERRED;
-- DEFERRABLE 필수: 새 버전은 valid_to=NULL(열린 구간)로 먼저 삽입되고 close_versions()가
-- 같은 트랜잭션 안에서 이전 버전의 valid_to를 다음 버전 시작일로 좁혀준다. IMMEDIATE였다면
-- INSERT 시점에 기존 열린 구간과 항상 겹쳐 실패한다(이력 백필 시 매번 위반 — 실측 확인).

CREATE INDEX IF NOT EXISTS article_version_article_id_valid_from_idx
    ON article_version (article_id, valid_from DESC);
CREATE INDEX IF NOT EXISTS article_version_daterange_idx ON article_version USING gist (
    daterange(valid_from, coalesce(valid_to, 'infinity'::date), '[)')
);

-- 개정 diff
CREATE TABLE IF NOT EXISTS article_diff (
    from_version     bigint REFERENCES article_version,
    to_version       bigint REFERENCES article_version,
    diff             jsonb,       -- 항/호 단위 added/removed/changed
    added_thresholds jsonb,       -- [{kind:'ratio', op:'>=', value:30, unit:'%'}]
    reason_text      text,        -- 기재부 개정이유
    reason_source    text,
    PRIMARY KEY (from_version, to_version)
);
CREATE INDEX IF NOT EXISTS article_diff_to_version_idx ON article_diff (to_version);
CREATE INDEX IF NOT EXISTS article_diff_thresholds_idx ON article_diff USING gin (added_thresholds);

-- 전부개정으로 조문 번호 체계가 갈아엎어졌을 때의 수작업 매핑(설계문서 9절 — 자동 매핑 불가).
-- eflaw의 조문이동이전/이동이후 필드가 실제 값을 주는 사례를 찾으면 자동 채움으로 전환 검토.
CREATE TABLE IF NOT EXISTS article_rewrite_map (
    from_article_id bigint NOT NULL REFERENCES article,
    to_article_id   bigint NOT NULL REFERENCES article,
    note            text,
    PRIMARY KEY (from_article_id, to_article_id)
);

-- 부칙 경과조치
CREATE TABLE IF NOT EXISTS addendum (
    addendum_id     bigserial PRIMARY KEY,
    statute_id      bigint NOT NULL REFERENCES statute,
    promulgation_no int,
    clause_no       text,
    body            text NOT NULL,
    kind            text,        -- 시행일/적용례/경과조치/특례
    applies_from    date,
    target_articles bigint[],    -- article_id[] (resolve_citation 연결 전까지 빈 배열)
    UNIQUE (statute_id, promulgation_no, clause_no)
);

-- 쟁송 + 해석 통합 (prec/expc/detc/admrul 공통)
CREATE TABLE IF NOT EXISTS ruling (
    ruling_id          text PRIMARY KEY,
    source             text NOT NULL,   -- 대법원/조세심판원/국세청예규/법제처해석례
    case_no            text,
    decided_on         date NOT NULL,
    outcome            text,            -- 납세자승/납세자패/일부인용 (expc/detc는 NULL 허용)
    gist               text,
    body               text,
    body_available     boolean NOT NULL DEFAULT false,
    cited_articles     bigint[],        -- article_key[] (시점 해소 완료분)
    cited_article_ids  bigint[],        -- article_id[]  (논리 레벨 — 그래프질의는 이 컬럼 사용)
    anti_avoidance     text[],          -- 실질과세/부당행위계산부인/단계거래
    raw_uri            text NOT NULL
);
CREATE INDEX IF NOT EXISTS ruling_cited_articles_idx    ON ruling USING gin (cited_articles);
CREATE INDEX IF NOT EXISTS ruling_cited_article_ids_idx ON ruling USING gin (cited_article_ids);
CREATE INDEX IF NOT EXISTS ruling_anti_avoidance_idx    ON ruling USING gin (anti_avoidance);
CREATE INDEX IF NOT EXISTS ruling_decided_on_idx        ON ruling (decided_on);

-- 개구멍 패턴 분류 (룩업 테이블 — Neo4j Pattern 노드 재생성의 SoT)
CREATE TABLE IF NOT EXISTS pattern_type (
    code        text PRIMARY KEY,
    description text NOT NULL
);
INSERT INTO pattern_type (code, description) VALUES
    ('시점차익',     '두 조문의 시행일·판정기준일 불일치'),
    ('정의불일치',   '동일 용어의 법률별 정의 차이'),
    ('분류재배치',   '거래의 법적 형식 변경으로 유리한 조문에 진입'),
    ('단위조작',     '인적·물적 단위 분할/합병으로 한도·기준 회피'),
    ('경과조치활용', '부칙이 남겨둔 창')
ON CONFLICT (code) DO NOTHING;

-- 생존 라벨 (파생)
CREATE TABLE IF NOT EXISTS loophole_candidate (
    id              bigserial PRIMARY KEY,
    article_id      bigint NOT NULL REFERENCES article,
    origin_ruling   text REFERENCES ruling,
    status          text NOT NULL CHECK (status IN ('alive', 'patched', 'partial', 'pending')),
    patched_by      bigint REFERENCES article_version(article_key),
    patched_on      date,
    pattern_type    text REFERENCES pattern_type(code),
    claim_deadline  date,
    risk_score      numeric(4,3),
    confirmed_by    text,
    note            text,
    UNIQUE (article_id, origin_ruling)
);
CREATE INDEX IF NOT EXISTS loophole_candidate_status_idx  ON loophole_candidate (status, claim_deadline);
CREATE INDEX IF NOT EXISTS loophole_candidate_article_idx ON loophole_candidate (article_id);

-- 임베딩 (항 단위. is_current 부분 인덱스가 기본 검색 경로 — 과거 시점 검색은 드물어 별도 처리)
-- is_current = "오늘 실제로 시행 중"(valid_from <= CURRENT_DATE AND (valid_to IS NULL OR
-- valid_to > CURRENT_DATE)) 이다 — article_version.valid_to IS NULL("아직 다음 버전이
-- 없다")과는 다르다. 세법은 시행일이 몇 달 뒤인 개정을 미리 공포하는 경우가 흔해서 valid_to
-- IS NULL인 버전의 valid_from이 미래인 사례가 실제로 다수 있다(실측: 3,053건 중 2,175건).
CREATE TABLE IF NOT EXISTS article_embedding (
    chunk_id      bigserial PRIMARY KEY,
    article_key   bigint NOT NULL REFERENCES article_version,
    chunk_path    text   NOT NULL,   -- '제1항' (목까지는 안 쪼갠다 — ingest/tree.py::iter_chunks)
    chunk_text    text   NOT NULL,
    embedding     vector(1024),
    is_current    boolean NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS article_embedding_article_key_chunk_path_key
    ON article_embedding (article_key, chunk_path);
CREATE INDEX IF NOT EXISTS article_embedding_hnsw_idx
    ON article_embedding USING hnsw (embedding vector_cosine_ops) WHERE is_current;
CREATE INDEX IF NOT EXISTS article_embedding_text_trgm_idx
    ON article_embedding USING gin (chunk_text gin_trgm_ops);
