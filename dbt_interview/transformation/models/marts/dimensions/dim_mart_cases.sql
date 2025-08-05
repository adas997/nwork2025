{{ config(
    materialized = 'table',
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

with 
case_rec as 
(
    select *
    from {{ ref ('vw_int_case') }}

)
select 
-- Surrogate Key
    {{ dbt_utils.generate_surrogate_key
          ([
           'cs.case_id',
           'cs.case_number'
           ]) 
          }} as case_sk,
cs.*
from case_rec cs
inner join {{ ref ('snaps_case') }} sc  on (sc.case_id = cs.case_id )
where current_date() between sc.dbt_valid_from and coalesce(sc.dbt_valid_to,'9999-12-31')
