
{% snapshot snaps_user %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',
      strategy='timestamp',
      updated_at='lastmodifieddate',
      invalidate_hard_deletes =True
    )
}}

select * from {{ source('stg_source', 'users') }}

{% endsnapshot %}    