WITH job_postings AS (
    SELECT job_id
    FROM {{ ref('fct_job_postings') }}
    {% if var('job_title', none) %}
    WHERE search_term = '{{ var("job_title") }}'
    {% endif %}
),

bridge AS (
    SELECT * FROM {{ ref('bridge_job_skills') }}

),

skills AS (
    SELECT * FROM {{ ref('dim_skill') }}
)

SELECT
    display_name,
    category,
    COUNT(DISTINCT job_id) as demand_count,
    ROUND((COUNT(DISTINCT job_id) / (SELECT COUNT(DISTINCT job_id) FROM job_postings))*100,1) AS demand_percent
FROM bridge
INNER JOIN skills USING(skill_id)
INNER JOIN job_postings USING(job_id)
GROUP BY display_name, category
ORDER BY demand_count DESC