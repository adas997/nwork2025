{{ config(
    materialized = 'incremental',
    unique_key = ['product_id','product_code','case_id','account_id'],
    incremental_strategy = 'merge',
    incremental_predicates = [
      "DBT_INTERNAL_DEST.prod_load_date > dateadd(day, -7, current_date)"
    ],
    post_hook = [
            """
            insert into main.log_model_run_details
            select '{{this.name}}' as model_name,
            now() as run_time,
            count(*) as row_count
            from {{this}}            
            """
        ]
) }}

with prod_data
as
(
    select *
    from {{ ref('dim_mart_products')}}

    {% if is_incremental() %}

     where prod_modified_date > (select coalesce(max(prod.prod_modified_date),'1900-01-01') from {{this}} prod )


     {% endif%}
),
price_data as
(
    select *
    from {{ ref('vw_int_price_book_entry')}}

    {% if is_incremental() %}

     where price_modified_date > (select coalesce(max(price.price_modified_date),'1900-01-01') from {{this}} price )


     {% endif%}

),

case_data as
(
    select *
    from {{ ref('dim_mart_cases')}}

    {% if is_incremental() %}

     where case_modified_date > (select coalesce(max(cases.case_modified_date),'1900-01-01') from {{this}} cases )


     {% endif%}

),

fct_data as (
 
    select {{ dbt_utils.generate_surrogate_key (['prod_modified_date' ,'case_modified_date'   
               ]) }} as date_sk,
    p.product_id,
    c.case_id,
    c.account_id,
    c.contact_id,
    pr.unit_price,
    pr.unit_price,
    pr.use_standard_price,
    pr.use_standard_price,
    pr.unit_price + pr.use_standard_price,
    p.prod_modified_date,

    extract(
    month
    from p.prod_modified_date
    ) as prod_modified_month,

    extract(
    year
    from p.prod_modified_date
    ) as prod_modified_year,

    c.case_modified_date,

    extract(
    month
    from c.case_modified_date
    ) as case_modified_month,

    extract(
    year
    from c.case_modified_date
    ) as case_modified_year,
    p.prod_load_date
from prod_data p
    left join price_data pr on (pr.product_id = p.product_id)
    left join case_data c on (p.product_id = c.product_id)
where is_prod_deleted = 1
    and pr.is_deleted = 1
    and c.is_deleted = 1

    {% if is_incremental() %}
    
     and p.prod_load_date >= (select coalesce(max(prod_load_date),'1900-01-01') from {{ this }} )

     {% endif%}

),
final as 
(
    select f.date_sk,
    f.product_id,
    f.case_id,
    f.account_id,
    f.contact_id,
    min(unit_price) as min_unit_price,
    max(unit_price) as max_unit_price,
    min(use_standard_price) as min_use_standard_price,
    max(use_standard_price) as max_use_standard_price,
    sum(unit_price + use_standard_price) as total_unit_price,
    f.prod_modified_month,
    f.prod_modified_year,
    f.case_modified_month,
    f.case_modified_year,
    f.prod_load_date
    from fct_data f 
    left join {{ ref ('dim_mart_date')}} d 
     on ( d.dim_date_sk = f.date_sk 
     and d.month_of_year_number = f.prod_modified_month
     and d.year_number = f.prod_modified_year     
     and d.month_of_year_number = f.case_modified_month
     and d.year_number = f.case_modified_year
     )
     group by
    f.date_sk,
    f.product_id,
    f.case_id,
    f.account_id,
    f.contact_id,
    f.prod_modified_month,
    f.prod_modified_year,
    f.case_modified_month,
    f.case_modified_year,
    f.prod_load_date
)
select 
           {{ dbt_utils.generate_surrogate_key (['product_id' ,'case_id','account_id','contact_id'
               ]) }} as fct_prod_sk,
        product_id,
        case_id,
        account_id,
        contact_id,
        {{ cents_to_dollars('min_unit_price') }} as min_unit_price_usd,
        {{ cents_to_dollars('max_unit_price') }} as max_unit_price_usd,
        {{ cents_to_dollars('min_use_standard_price') }} as min_use_standard_price_usd,
        {{ cents_to_dollars('max_use_standard_price') }} as max_use_standard_price_usd,
        {{ cents_to_dollars('total_unit_price') }} as total_unit_price_usd,
        prod_modified_month,
        prod_modified_year,
        case_modified_month,
        case_modified_year,
        prod_load_date
from final
