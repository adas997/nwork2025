
{% snapshot snaps_opportunity %}

{{
    config(
      target_schema='snapshots',
      unique_key='opportunity_id',
      strategy='timestamp',
      updated_at='lastmodifieddate',
      invalidate_hard_deletes =True
    )
}}

select * from {{ source('stg_source', 'opportunity') }}

{% endsnapshot %}       