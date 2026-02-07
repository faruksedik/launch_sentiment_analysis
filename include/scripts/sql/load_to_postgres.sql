
-- Purpose: Load transformed CSV data into wikipedia_pageviews table
--          while avoiding duplicate records

-- -----------------------------------------------------------------
-- 1. Create table if not exists
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wikipedia_pageviews (
    page_title_id TEXT PRIMARY KEY,
    page_title TEXT NOT NULL,
    pageviews INTEGER NOT NULL,
    event_timestamp VARCHAR(50) NOT NULL
);

-- -----------------------------------------------------------------
-- 2. Create temp staging table
-- -----------------------------------------------------------------
CREATE TEMP TABLE staging_wikipedia_pageviews (
    page_title_id TEXT,
    page_title TEXT,
    pageviews INTEGER,
    event_timestamp VARCHAR(50)
);

-- -----------------------------------------------------------------
-- 3. Load CSV via STDIN (Python will stream the file)
-- -----------------------------------------------------------------
COPY staging_wikipedia_pageviews
FROM STDIN
WITH (
    FORMAT csv,
    HEADER true
);

-- -----------------------------------------------------------------
-- 4. Insert into target table while avoiding duplicates
-- -----------------------------------------------------------------
INSERT INTO wikipedia_pageviews (
    page_title_id,
    page_title,
    pageviews,
    event_timestamp
)
SELECT
    page_title_id,
    page_title,
    pageviews,
    event_timestamp
FROM staging_wikipedia_pageviews
ON CONFLICT (page_title_id) DO NOTHING;

-- -----------------------------------------------------------------
-- 5. Drop temp table (optional, auto-dropped at session end)
-- -----------------------------------------------------------------
DROP TABLE staging_wikipedia_pageviews;
