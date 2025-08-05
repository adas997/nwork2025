{% snapshot snaps_case %}

{{
    config(
      target_schema='snapshots',
      unique_key='case_id',
      strategy='timestamp',
      updated_at='lastmodifieddate',
      invalidate_hard_deletes =True
    )
}}

select * from {{ source('stg_source', 'cases') }}

{% endsnapshot %}      