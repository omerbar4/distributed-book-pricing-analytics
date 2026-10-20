-- Example analytics queries. Run with:  make psql   then  \i sql/example_queries.sql
-- All prices are SIMULATED demo values (data_origin = 'SIMULATED_DEMO').

-- 1. Highest recommended prices on the latest processing date
SELECT isbn_13, left(title, 40) AS title, category, condition,
       current_listing_price, recommended_price, recommendation_reason
FROM analytics.daily_pricing_recommendations
WHERE processing_date = (SELECT max(processing_date) FROM analytics.daily_pricing_recommendations)
ORDER BY recommended_price DESC
LIMIT 10;

-- 2. Lowest recommended prices on the latest processing date
SELECT isbn_13, left(title, 40) AS title, category, condition,
       current_listing_price, recommended_price, recommendation_reason
FROM analytics.daily_pricing_recommendations
WHERE processing_date = (SELECT max(processing_date) FROM analytics.daily_pricing_recommendations)
ORDER BY recommended_price ASC
LIMIT 10;

-- 3. Pricing summary by category (latest date)
SELECT category, valid_book_count, avg_listing_price, median_listing_price,
       avg_recommended_price, total_inventory, invalid_record_count
FROM analytics.category_pricing_summary
WHERE processing_date = (SELECT max(processing_date) FROM analytics.category_pricing_summary)
ORDER BY valid_book_count DESC;

-- 4. Where would repricing move the most money? (recommended - listing) x inventory
SELECT category,
       sum(price_change * inventory_count)::numeric(12, 2) AS inventory_value_change,
       count(*) FILTER (WHERE price_change < 0) AS price_cuts,
       count(*) FILTER (WHERE price_change > 0) AS price_raises
FROM analytics.daily_pricing_recommendations
WHERE processing_date = (SELECT max(processing_date) FROM analytics.daily_pricing_recommendations)
GROUP BY category
ORDER BY inventory_value_change;

-- 5. Data-quality trend: recent runs, failures first
SELECT processing_date, input_record_count, valid_record_count, rejected_record_count,
       duplicate_count, round(quality_pass_rate::numeric, 4) AS pass_rate, quality_gate_passed
FROM analytics.data_quality_report
ORDER BY quality_gate_passed, processing_date DESC
LIMIT 14;

-- 6. Recent data-quality failures by rule
SELECT processing_date, reason, count(*) AS rejected_rows
FROM analytics.rejected_records, unnest(rejection_reasons) AS reason
WHERE processing_date >= current_date - 7
GROUP BY processing_date, reason
ORDER BY processing_date DESC, rejected_rows DESC;
