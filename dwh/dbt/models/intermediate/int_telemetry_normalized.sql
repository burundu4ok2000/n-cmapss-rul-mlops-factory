{{
    config(
        materialized='table',
        description='Silver layer. Applies physics-based transformations to raw telemetry: ISO corrections, dynamic fuel density, TAS and distance, and stall risk index.'
    )
}}

/*
  int_telemetry_normalized — Silver Layer
  ========================================
  Source: stg_telemetry (Bronze)
  Grain: 1 row = 1 second of engine operation (same as staging)

  Responsibility:
    - Apply ISO/ICAO atmospheric corrections (theta, delta) to remove altitude noise
    - Calculate dynamic Jet-A fuel density ρ(T2) for volumetric accounting
    - Compute True Airspeed (TAS) and per-second distance increment (Nautical Miles)
    - Compute Phi index (Wf/Ps30) as NASA stall risk proxy
    - All raw columns pass through unchanged for downstream use

  Reference formulas: NASA C-MAPSS paper (20080043619), ISA standard
*/

with base as (
    select * from {{ ref('stg_telemetry') }}
),

with_iso as (
    select
        base.*,

        -- ── ISO Correction Factors ───────────────────────────────────────
        {{ iso_theta('t2_inlet_temp_r') }}                           as theta,
        {{ iso_delta('p2_inlet_psia') }}                             as delta,

        -- ── ISO Square Root (Optimization) ───────────────────────────────
        -- Calculated once here to save money/compute in downstream macros
        SQRT(GREATEST({{ iso_theta('t2_inlet_temp_r') }}, 0.00001)) as sqrt_theta,

        -- ── Static Air Temperature (SAT) ─────────────────────────────────
        -- Calculated here so it can be used for density in the next CTE
        {{ static_temp_rankine('t2_inlet_temp_r', 'mach_number') }}  as t_static_r

    from base
),

with_corrections as (
    select
        with_iso.*,

        -- ── ISO-Corrected Temperatures (removes altitude/weather noise) ──
        {{ iso_correct_temp('t30_hpc_outlet_r', 'theta') }}          as t30_corr_r,
        {{ iso_correct_temp('t48_hpt_outlet_r', 'theta') }}          as t48_corr_r,

        -- ── ISO-Corrected Fan Speed ──────────────────────────────────────
        {{ iso_correct_speed('nf_fan_speed_rpm', 'theta') }}         as nf_corr_rpm,

        -- ── ISO-Corrected Fuel Flow (core degradation signal) ───────────
        {{ iso_correct_flow('wf_fuel_flow_pps', 'delta', 'theta') }} as wf_corr_pps,

        -- ── Dynamic Fuel Density ρ(SAT) ──────────────────────────────────
        -- Using Static Air Temperature (SAT) instead of T2 to avoid Ram Rise bias
        {{ fuel_density_lbs_gal('t_static_r') }}                as rho_lbs_gal,

        -- ── Speed of Sound (a) ───────────────────────────────────────────
        -- Pre-calculated here to save compute costs (avoiding redundant SQRT calls)
        {{ speed_of_sound_fps('t_static_r') }}                   as a_fps,

        -- ── Stall Risk: Phi index (Wf/Ps30) ─────────────────────────────
        -- Rising phi → engine pushing harder against compressor limit
        (wf_fuel_flow_pps / NULLIF(ps30_hpc_static_psia, 0))    as phi_index

    from with_iso
),

with_derived as (

    select
        with_corrections.*,

        -- ── Volumetric Fuel Flow (accounts for density at altitude) ──────
        {{ fuel_flow_gph('wf_fuel_flow_pps', 'rho_lbs_gal') }}       as wf_gph,
        {{ fuel_flow_kg_s('wf_fuel_flow_pps') }}                     as wf_kg_s,

        -- ── TAS and distance per second ──────────────────────────────────
        -- Using pre-calculated a_fps for performance
        {{ tas_knots('mach_number', 'a_fps', is_speed_of_sound=true) }}                as tas_kts,
        {{ dist_nm_increment('mach_number', 'a_fps', is_speed_of_sound=true) }}        as dist_nm_increment

    from with_corrections
)

select * from with_derived
