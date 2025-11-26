{{ config(
    materialized='incremental',
    unique_key='url'
) }}

with currentsapi as (
    select
        sf_id::text as sf_id,
        source_system,
        source_news as source_name,
        id::text as article_id,
        author,
        title,
        description,
        url,
        image_url,
        category,
        language,
        region,
        null::text as content,
        published_at,
        createdate,
        updatedate,
        batch_id,
        layer,
        usercreate,
        userupdate,
        activedata
    from {{ ref('silver_clean_currentsapi') }}
),

mediastack as (
    select
        sf_id::text as sf_id,
        source_system,
        source_news as source_name,
        null::text as article_id,
        author,
        title,
        description,
        url,
        image_url,
        category,
        language,
        region,
        null::text as content,
        published_at,
        createdate,
        updatedate,
        batch_id,
        layer,
        usercreate,
        userupdate,
        activedata
    from {{ ref('silver_clean_mediastack') }}
),

newsapi as (
    select
        sf_id::text as sf_id,
        source_system,
        source_news as source_name,
        source_id::text as article_id,
        author,
        title,
        description,
        url,
        image_url,
        category,
        language,
        region,
        content,
        published_at,
        createdate,
        updatedate,
        batch_id,
        layer,
        usercreate,
        userupdate,
        activedata
    from {{ ref('silver_clean_newsapi') }}
),

stg as (
    select
        null::text as sf_id,
        null::text as source_system,
        source as source_name,
        id::text as article_id,
        author,
        titlenews as title,
        description,
        url,
        imageurl as image_url,
        category,
        language,
        null::text as region,
        null::text as content,
        publishedtime as published_at,
        createdate,
        updatedate,
        null::text as batch_id,
        'silver' as layer,
        usercreate,
        userupdate,
        activedata
    from {{ ref('silver_stg_news_articles') }}
)

select *
from currentsapi
{% if is_incremental() %}
where published_at > (select max(published_at) from {{ this }})
{% endif %}

union all

select *
from mediastack
{% if is_incremental() %}
where published_at > (select max(published_at) from {{ this }})
{% endif %}

union all

select *
from newsapi
{% if is_incremental() %}
where published_at > (select max(published_at) from {{ this }})
{% endif %}

union all

select *
from stg
{% if is_incremental() %}
where published_at > (select max(published_at) from {{ this }})
{% endif %}