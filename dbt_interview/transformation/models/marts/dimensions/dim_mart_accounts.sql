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
with account_rec as 
(
    select *
     from {{ ref('vw_int_account') }}

     
),
contact_rec as
(
    select * 
    from {{ ref ('vw_int_contact') }}

    
),
final as
(
-- Surrogate Key
select {{ dbt_utils.generate_surrogate_key
          (['a.account_id',           
           'co.contact_id'
           ]) 
          }} as acc_sk,
-- account

    a.account_id,
a.parent_id,
a.account_type,
a.billing_street,
a.billing_city,
a.billing_state,
a.billing_postal_code,
a.billing_country,
a.shipping_street,
a.shipping_city,
a.shipping_state,
a.shipping_postal_code,
a.shipping_country,
-- contact
co.contact_id,
co.first_name,
co.last_name,
co.salutation,
co.mailing_street,
co.mailing_city,
co.mailing_state,
co.phone,
co.fax,
co.mobilephone,
-- DWH 
a.is_deleted as is_account_deleted,
co.is_deleted as is_contact_deleted,
-- Dates
a.acc_created_date,
--a.acc_modified_date,
coalesce(a.acc_created_date,a.acc_modified_date) as acc_modified_date,
co.con_created_date,
coalesce(co.con_created_date,co.con_modified_date) as con_modified_date ,
current_date() as account_load_date
from account_rec a
    left join contact_rec co on (a.account_id = co.account_id)
    inner join {{ ref ('snaps_account') }} sa  on (sa.account_id = a.account_id )
    inner join {{ ref ('snaps_contact') }} sb  on (sb.contact_id = co.contact_id )
where 1 = 1
and current_date() between sa.dbt_valid_from and coalesce(sa.dbt_valid_to,'9999-12-31')
and current_date() between sb.dbt_valid_from and coalesce(sb.dbt_valid_to,'9999-12-31')
)
select *
from final
 

