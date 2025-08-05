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
opportunity_rec as 
(
    select *
    from {{ ref ('vw_int_opportunity') }}

),

final as 
(
    select 
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key
          ([
           'o.opportunity_id',
           'o.opportunity_name'
           ]) 
          }} as opportunity_sk,
    o.opportunity_id,
o.account_id,
o.opportunity_name,
o.is_private,
o.opportunity_description,
o.stage_name,
o.opportunity_type,
--snapshot fields (flags)
o.is_deleted as is_opportunity_deleted,
--DWH Dates
o.oppr_created_date,
--o.oppr_modified_date,
coalesce(o.oppr_created_date,o.oppr_modified_date) as oppr_modified_date ,
current_date() as opportunity_load_date
from opportunity_rec o  
inner join {{ ref ('snaps_opportunity') }} so  on (o.opportunity_id = so.opportunity_id )
where current_date() between so.dbt_valid_from and coalesce(so.dbt_valid_to,'9999-12-31') 
)

select * from final
