{{ config(
    materialized = 'incremental',
    unique_key = ['account_id','opportunity_id'],
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
with dim_acc_data as
(
    select *
    from {{ ref ('dim_mart_accounts') }}

    {% if is_incremental() %}

     where load_date > (select coalesce(max(load_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
account_rec as 
(
    select *
     from {{ ref('vw_int_account') }}

     {% if is_incremental() %}

     where acc_modified_date > (select coalesce(max(acc.acc_modified_date),'1900-01-01') from {{this}} acc )


     {% endif%}
),
opportunity_rec as 
(
    select *
    from {{ ref ('vw_int_opportunity') }}

     {% if is_incremental() %}

     where oppr_modified_date > (select coalesce(max(oppr_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
dim_opportunity_rec as 
(
    select *
    from {{ ref ('dim_mart_opportunities') }}

    {% if is_incremental() %}

     where oppr_modified_date > (select coalesce(max(oppr_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
dim_users as 
(
    select *
    from {{ ref ('dim_mart_users') }}

    {% if is_incremental() %}

     where user_modified_date > (select coalesce(max(user_modified_date),'1900-01-01') from {{this}}  )


     {% endif%}
),
fact_data as 
(
select 
    {{ dbt_utils.generate_surrogate_key (
        ['a.acc_modified_date','o.oppr_modified_date'     
           ]
    ) }} as date_sk,
    dim_acc.account_id,
    dim_opp.opportunity_id,
    dim_acc.contact_id,
    u.user_id,
    a.annual_revenue,
    o.amount,
    o.expected_revenue,
    o.probability,
    --dim_acc.account_load_date,
    --dim_opp.opportunity_load_date,

     a.acc_modified_date,

    extract(
    month
    from a.acc_modified_date
    ) as acc_modified_month,

    extract(
    year
    from a.acc_modified_date
    ) as acc_modified_year,

    o.oppr_modified_date,

    extract(
    month
    from o.oppr_modified_date
    ) as oppr_modified_month,

    extract(
    year
    from o.oppr_modified_date
    ) as oppr_modified_year,
    dim_acc.account_load_date,
    dim_opp.opportunity_load_date,
    from account_rec a
    inner join dim_acc_data dim_acc on (a.account_id = dim_acc.account_id)
    left join opportunity_rec o on (o.account_id = a.account_id)
    inner join dim_opportunity_rec dim_opp on (
        o.opportunity_id = dim_opp.opportunity_id
        and dim_opp.account_id = dim_acc.account_id
    )
    left join dim_users u on (
        u.account_id = dim_acc.account_id
        and u.contact_id = dim_acc.contact_id
               )
where dim_acc.is_account_deleted = 1
    and dim_opp.is_opportunity_deleted = 1

    {% if is_incremental() %}
    
     and dim_acc.account_load_date >= (select coalesce(max(account_load_date),'1900-01-01') from {{ this }} )
     and dim_opp.opportunity_load_date >= (select coalesce(max(opportunity_load_date),'1900-01-01') from {{ this }} )
     

     {% endif%}
),
final as 
(  select
    f.date_sk,
    f.account_id,
    f.opportunity_id,
    f.contact_id,
    f.user_id,
    sum(f.annual_revenue) as total_revenue_earned,
    sum(f.amount) as total_opportunity_amount,
    sum(f.expected_revenue) as total_revenue_expected,
    avg(f.probability) as average_probability,
    f.acc_modified_month,
    f.acc_modified_year,
    f.oppr_modified_month,
    f.oppr_modified_year,
    f.account_load_date,
    f.opportunity_load_date
    from fact_data f  
    left join {{ ref ('dim_mart_date') }} d 
     on ( d.dim_date_sk = f.date_sk 
     and d.month_of_year_number = f.acc_modified_month
     and d.year_number = f.acc_modified_year     
     and d.month_of_year_number = f.oppr_modified_month
     and d.year_number = f.oppr_modified_year
       )
    group by 
    f.date_sk,
    f.account_id,
    f.opportunity_id,
    f.contact_id,
    f.user_id,
    f.acc_modified_month,
    f.acc_modified_year,
    f.oppr_modified_month,
    f.oppr_modified_year,
    f.account_load_date,
    f.opportunity_load_date
)
select 
    {{ dbt_utils.generate_surrogate_key (
        ['account_id','opportunity_id' ,'contact_id' ,'user_id'  
           ]
    ) }} as fct_revenue_sk,
    account_id,
    opportunity_id,
    contact_id,
    user_id,
    {{ cents_to_dollars('total_revenue_earned') }} as total_revenue_earned_usd,
    {{ cents_to_dollars('total_opportunity_amount') }} as total_opportunity_amount_usd,
    {{ cents_to_dollars('total_revenue_expected') }} as total_revenue_expected_usd,
    {{ cents_to_dollars('average_probability') }} as average_probability,
    acc_modified_month,
    acc_modified_year,
    oppr_modified_month,
    oppr_modified_year,
    account_load_date,
    opportunity_load_date
from final