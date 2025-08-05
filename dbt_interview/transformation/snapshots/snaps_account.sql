{% snapshot snaps_account %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='timestamp',
      updated_at='lastmodifieddate',
      invalidate_hard_deletes =True
    )
}}

select * from {{ source('stg_source', 'account') }}

{% endsnapshot %}

