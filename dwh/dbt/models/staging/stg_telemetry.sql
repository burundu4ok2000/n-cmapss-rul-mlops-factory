{{
    config(
        materialized='incremental',
        unique_key='telemetry_id',
        on_schema_change='append_new_columns',
        description='Bronze staging layer. Reads raw N-CMAPSS Parquet telemetry incrementally with audit metadata and surrogate keys.'
    )
}}

/*
  stg_telemetry — Bronze Layer
  ============================
  Source: Hive-partitioned Parquet lake (.workspace/telemetry_lake/)
  Grain: 1 row = 1 second of engine operation for a given (dataset, unit, cycle)
  
  Responsibility:
    - Read all 9 dataset partitions in one scan via hive_partitioning=True
    - Cast columns to correct types (int vs double)
    - Expose clean column names matching dictionary.yaml
    - No business logic here — this belongs in intermediate/
*/

with source_data as (
    select
        *,
        -- Partitioning by dataset, unit, and cycle ensures deterministic row indices per flight cycle
        row_number() over (partition by dataset, unit, cycle) as row_idx,
        filename as _source_file,
        now() as _ingested_at
    from read_parquet('{{ var("telemetry_lake_path") }}**/*.parquet', hive_partitioning=true, filename=true)
    
    {% if is_incremental() %}
        -- Smart build: only scan and ingest new parquet files that haven't been loaded yet
        where filename not in (select distinct _source_file from {{ this }})
    {% endif %}
),

renamed as (

    select
        -- ── Surrogate Key ────────────────────────────────────────────────
        -- MD5 hash ensures a stable, unique identifier for each telemetry point
        md5(concat_ws('-', dataset::varchar, unit::varchar, cycle::varchar, row_idx::varchar)) as telemetry_id,

        -- ── Partition / Administrative keys (Safe Casting) ───────────────
        try_cast(dataset as varchar)        as dataset_id,
        try_cast(unit as integer)           as unit_id,
        try_cast(cycle as integer)          as cycle_num,
        try_cast(split as varchar)          as data_split,

        -- ── Target variable ──────────────────────────────────────────────
        try_cast(RUL as integer)            as rul,

        -- ── Flight descriptors (W group) ────────────────────────────────
        try_cast(Fc as integer)             as flight_class,
        try_cast(hs as integer)             as health_state,
        try_cast(alt as double)             as altitude_ft,
        try_cast(Mach as double)            as mach_number,
        try_cast(TRA as double)             as throttle_pct,
        try_cast(T2 as double)              as t2_inlet_temp_r,

        -- ── Physical sensors — temperatures (Xs group) ──────────────────
        try_cast(T24 as double)             as t24_lpc_outlet_r,
        try_cast(T30 as double)             as t30_hpc_outlet_r,
        try_cast(T48 as double)             as t48_hpt_outlet_r,
        try_cast(T50 as double)             as t50_lpt_outlet_r,

        -- ── Physical sensors — pressures (Xs group) ─────────────────────
        try_cast(P2 as double)              as p2_inlet_psia,
        try_cast(P15 as double)             as p15_bypass_psia,
        try_cast(P21 as double)             as p21_fan_outlet_psia,
        try_cast(P24 as double)             as p24_lpc_outlet_psia,
        try_cast(Ps30 as double)            as ps30_hpc_static_psia,
        try_cast(P40 as double)             as p40_burner_outlet_psia,
        try_cast(P50 as double)             as p50_lpt_outlet_psia,

        -- ── Physical sensors — speeds & flow (Xs group) ─────────────────
        -- Unified naming to _pps (pounds per second) for consistency
        try_cast(Wf as double)              as wf_fuel_flow_pps,
        try_cast(Nf as double)              as nf_fan_speed_rpm,
        try_cast(Nc as double)              as nc_core_speed_rpm,

        -- ── Virtual sensors (Xv group) ──────────────────────────────────
        try_cast(T40 as double)             as t40_burner_virtual_r,
        try_cast(P30 as double)             as p30_hpc_virtual_psia,
        try_cast(P45 as double)             as p45_hpt_virtual_psia,
        try_cast(W21 as double)             as w21_fan_flow_pps,
        try_cast(W22 as double)             as w22_lpc_flow_pps,
        try_cast(W25 as double)             as w25_hpc_inlet_flow_pps,
        try_cast(W31 as double)             as w31_hpt_coolant_pps,
        try_cast(W32 as double)             as w32_lpt_case_coolant_pps,
        try_cast(W48 as double)             as w48_hpt_outlet_flow_pps,
        try_cast(W50 as double)             as w50_lpt_outlet_flow_pps,
        try_cast(SmFan as double)           as sm_fan,
        try_cast(SmLPC as double)           as sm_lpc,
        try_cast(SmHPC as double)           as sm_hpc,
        try_cast(phi as double)             as phi_wf_ps30_ratio,

        -- ── Health parameters / degradation modifiers (Theta group) ─────
        try_cast(fan_eff_mod as double)     as fan_eff_mod,
        try_cast(fan_flow_mod as double)    as fan_flow_mod,
        try_cast(LPC_eff_mod as double)     as lpc_eff_mod,
        try_cast(LPC_flow_mod as double)    as lpc_flow_mod,
        try_cast(HPC_eff_mod as double)     as hpc_eff_mod,
        try_cast(HPC_flow_mod as double)    as hpc_flow_mod,
        try_cast(HPT_eff_mod as double)     as hpt_eff_mod,
        try_cast(HPT_flow_mod as double)    as hpt_flow_mod,
        try_cast(LPT_eff_mod as double)     as lpt_eff_mod,
        try_cast(LPT_flow_mod as double)    as lpt_flow_mod,

        -- ── Metadata ─────────────────────────────────────────────────────
        _source_file,
        _ingested_at

    from source_data

)

select * from renamed

