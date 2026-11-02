{% set roles = dbt_utils.get_column_values(ref('stg_job_postings'), 'search_term') -%}

{% for role in roles -%}
SELECT
    search_term,
    COUNT(*) AS postings
FROM {{ ref('stg_job_postings') }}
WHERE search_term = '{{ role | title }}'
GROUP BY search_term
{{ 'UNION ALL' if not loop.last }}
{% endfor%}

