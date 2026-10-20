-- Analytics schema for the book pricing pipeline. Idempotent: safe to run repeatedly
-- (Docker runs it on first start; the loader re-applies it before every load).

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.daily_pricing_recommendations (
    processing_date        date           NOT NULL,
    isbn_13                char(13)       NOT NULL,
    condition              text           NOT NULL
        CHECK (condition IN ('VERY_GOOD', 'GOOD', 'ACCEPTABLE')),
    title                  text           NOT NULL,
    author                 text,
    category               text           NOT NULL,
    current_listing_price  numeric(10, 2) NOT NULL CHECK (current_listing_price >= 0),
    estimated_market_price numeric(10, 2) NOT NULL CHECK (estimated_market_price >= 0),
    recommended_price      numeric(10, 2) NOT NULL CHECK (recommended_price >= 0),
    price_change           numeric(10, 2) NOT NULL,
    inventory_count        integer        NOT NULL CHECK (inventory_count >= 0),
    recommendation_reason  text           NOT NULL,
    data_origin            text           NOT NULL,  -- 'SIMULATED_DEMO': not real market data
    loaded_at              timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (processing_date, isbn_13, condition)
);

CREATE INDEX IF NOT EXISTS daily_pricing_recommendations_category_idx
    ON analytics.daily_pricing_recommendations (processing_date, category);

CREATE TABLE IF NOT EXISTS analytics.category_pricing_summary (
    processing_date        date           NOT NULL,
    category               text           NOT NULL,
    valid_book_count       bigint         NOT NULL,
    valid_listing_count    bigint         NOT NULL,
    avg_listing_price      numeric(10, 2),  -- NULL when every row in the category was rejected
    median_listing_price   numeric(10, 2),
    avg_recommended_price  numeric(10, 2),
    total_inventory        bigint         NOT NULL,
    invalid_record_count   bigint         NOT NULL,
    loaded_at              timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (processing_date, category)
);

CREATE TABLE IF NOT EXISTS analytics.data_quality_report (
    processing_date            date             PRIMARY KEY,
    input_record_count         bigint           NOT NULL,
    valid_record_count         bigint           NOT NULL,
    rejected_record_count      bigint           NOT NULL,
    duplicate_count            bigint           NOT NULL,
    exact_duplicate_count      bigint           NOT NULL,
    business_duplicate_count   bigint           NOT NULL,
    invalid_isbn_count         bigint           NOT NULL,
    negative_price_count       bigint           NOT NULL,
    blank_title_count          bigint           NOT NULL,
    invalid_inventory_count    bigint           NOT NULL,
    malformed_record_count     bigint           NOT NULL,
    other_rule_violation_count bigint           NOT NULL,
    quality_pass_rate          double precision NOT NULL,
    min_pass_rate              double precision NOT NULL,
    quality_gate_passed        boolean          NOT NULL,
    generated_at               timestamptz      NOT NULL,
    loaded_at                  timestamptz      NOT NULL DEFAULT now()
);

-- Raw values are kept as received (wrong types/ranges are the reason they were rejected).
CREATE TABLE IF NOT EXISTS analytics.rejected_records (
    rejected_record_id     bigserial        PRIMARY KEY,
    processing_date        date             NOT NULL,
    observation_id         text,
    isbn_raw               text,
    isbn_invalid_reason    text,
    title                  text,
    category               text             NOT NULL,
    condition              text,
    listing_price          double precision,
    estimated_market_price double precision,
    inventory_count        bigint,
    observed_at            text,
    rejection_reasons      text[]           NOT NULL,
    rejection_reason       text             NOT NULL,
    raw_record             text,
    source_file            text,
    loaded_at              timestamptz      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS rejected_records_date_idx
    ON analytics.rejected_records (processing_date);
