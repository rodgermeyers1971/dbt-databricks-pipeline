WITH source AS (
SELECT *
FROM
    {{ source('jobs', 'raw_job_postings') }}
WHERE
    error IS NOT TRUE
),

cleaned AS (
SELECT
    job_id,
    job_title,
    TRIM(company_name) AS company_name,
    job_location,
    REGEXP_REPLACE(job_via, '^via ' , '') AS source_platform,
    job_posted_at,
    job_salary,
    job_schedule_type,
    job_work_from_home,
    job_detected_extensions_raw AS job_extensions_raw,
    search_time AS searched_at,
    search_date,
    search_term,
    search_location
FROM
    source
)

SELECT * FROM cleaned