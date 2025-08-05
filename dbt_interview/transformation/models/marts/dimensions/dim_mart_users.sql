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
with user_data
as
(
    select *
     from {{ ref('vw_int_user')}}
),
user_role_data as 
(
    select *
     from {{ ref('vw_int_user_role') }}
)
 
select {{ dbt_utils.generate_surrogate_key (
        ['u.user_id',
           'u.user_role_id',
           'u.user_name',
           'ur.role_name'
           ]
    ) }} user_sk,
    u.user_id,
    u.contact_id,
    u.account_id,
    u.user_role_id,
    u.user_name,
    ur.role_name,
    ur.rollup_description,
    u.first_name,
    u.last_name,
    u.company_name,
    u.division,
    u.department,
    u.title,
    u.street,
    u.city,
    u.state,
    u.postalcode,
    u.country,
    u.phone,
    u.fax,
    u.mobilephone,
    ur.opportunity_for_owner,
    ur.case_access_for_accountowner,
    u.user_modified_date
from user_data u
    left join user_role_data ur on (u.user_role_id = ur.user_role_id)
    inner join {{ ref ('snaps_user') }} su  on (u.user_id = su.user_id )
where current_date() between su.dbt_valid_from and coalesce(su.dbt_valid_to,'9999-12-31') 

