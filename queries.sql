-- gim_import_query
select
    a_resource_name,
    q_resource_name,
    q_end_ts,
    q_interaction_type,
    q_entered,
    q_consult_entered,
    a_accepted,
    a_consult_received_accepted,
    a_consult_received_engage_time,
    a_engage_time,
    a_consult_received_hold_time,
    a_hold_time,
    a_consult_received_wrap_time,
    a_wrap_time,
    k_srf,
    a_interaction_type,
    k_virtual_queue
from cct0_gim_measures_gold_df
where from_unixtime(load_ts) >= '{start_ts}' and from_unixtime(load_ts) <= '{end_ts}';

-- transform_gim_staging_query
select
    a_resource_name,
    COALESCE(q_resource_name, k_virtual_queue) as q_resource_name,
    substring(COALESCE(q_resource_name, k_virtual_queue), 0, 2) as entity,
    substring(COALESCE(q_resource_name, k_virtual_queue), 3, 6) as resource_group,
    substring(COALESCE(q_resource_name, k_virtual_queue), 8, 16) as interaction_purpose,
    substring(COALESCE(q_resource_name, k_virtual_queue), 17, 1) as language,
    from_unixtime(a_end_ts) as queue_end_date_time,
    a_interaction_type,
    q_entered,
    q_end_ts,
    q_consult_entered,
    a_accepted,
    a_consult_received_accepted as accepted,
    a_consult_received_engage_time as engage_time,
    a_engage_time,
    a_consult_received_hold_time as hold_time,
    a_hold_time,
    a_consult_received_wrap_time as wrap_time,
    a_wrap_time,
    k_srf
from gim_staging_df;

-- transform_gim_output_query
select
    cast(null as integer) as event_id,
    cast(null as timestamp) as date_received,
    cast(g.queue_end_date_time as timestamp) as date_completed,
    1 as data_source,
    entity as line_of_business,
    cast(g.a_resource_name as integer) as employee_number,
    q_resource_name as activity_reference_id,
    coalesce(rg.RSRC_GRP_DESC, g.resource_group) as process,
    coalesce(ip.INTN_PURPS_DESC, g.interaction_purpose) as subprocess_1,
    g.interaction_type as subprocess_2,
    case when g.language = 'E' then 'English'
          when g.language = 'F' then 'French'
          else g.language
    end as subprocess_3,
    cast(null as integer) as subprocess_4,
    cast(null as integer) as subprocess_5,
    cast(null as integer) as subprocess_6,
    cast(g.a_resource_name as integer) as volume,
    cast(g.a_accepted as integer) as volume,
    cast(g.engage_time + g.hold_time + g.wrap_time as integer) as processing_time,
    cast(g.a_accepted as integer) as at_target,
    cast(null as boolean) as is_met,
    cast(null as timestamp) as tat_target_datetime,
    cast(null as float) as tat_target,
    cast(null as integer) as client_number,
    cast(null as string) as account_number,
    cast(null as string) as application_number,
    cast(null as string) as queue_id,
    cast(null as string) as queue_description,
    cast(null as string) as product,
    g.a_accepted,
    g.engage_time,
    g.hold_time
from gim_staging_df as g
left join interaction_purpose_map_df as ip
  on ip.INTN_PURPS_CD_VAL = g.interaction_purpose
left join resource_group_map_df as rg
  on rg.RSRC_GRP_CD_VAL = resource_group
where cast(g.queue_end_date_time as timestamp) is not null;
