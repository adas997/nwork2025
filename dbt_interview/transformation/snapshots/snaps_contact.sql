
{% snapshot snaps_contact %}

{{
    config(
      target_schema='snapshots',
      unique_key='contact_id',
      strategy='timestamp',
      updated_at='lastmodifieddate',
      invalidate_hard_deletes =True
    )
}}

select * from {{ source('stg_source', 'contact') }}

{% endsnapshot %}        