with user_role_data as (
    select u.*,
    row_number () over(
        partition by u.user_role_id,
        u.name
        order by u.user_role_id,
            u.name
    ) rn
    from {{ source ('stg_source', 'users_role') }} u
)
select user_role_id,
    name as role_name,
    rollupdescription as rollup_description,
    opportunityaccessforaccountowner as opportunity_for_owner,
    caseaccessforaccountowner as case_access_for_accountowner,
    contactaccessforaccountowner as contact_access_foraccountowner,
    lastmodifieddate as modified_date,
    lastmodifiedbyid as modified_by,
    systemmodstamp as system_modstamp,
    portalaccountid as portal_accountid,
    portaltype as portal_type,
    portalrole as portal_role,
    portalaccountownerid as portal_account_ownerid
from user_role_data
where rn = 1