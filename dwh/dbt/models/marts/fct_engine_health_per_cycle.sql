{{
    config(
        materialized='table',
        description='Gold mart. One row per (dataset, unit, cycle). Aggregates normalized telemetry into cycle-level engine health KPIs: EGT margin, stall margin, SFC degradation, fuel economy per nautical mile.'
    )
}}

/*
  fct_engine_health_per_cycle — Gold Mart (Fact Table)
  =====================================================
  Source: int_telemetry_normalized (Silver)
  Grain: 1 row = 1 flight cycle for a given engine unit

  Columns:
    - Lifecycle metadata (total cycles, lifecycle %, dominant Fc)
    - Health parameters (HPC, HPT efficiency modifiers)
    - EGT/T48 tracking: peak corrected temperature vs baseline
    - Stall margin: avg and minimum SmHPC per cycle
    - SFC: actual vs nominal (baseline = mean of first 5 cycles)
    - Fuel volume: total gallons per cycle (density-corrected)
    - Distance: nautical miles flown per cycle
    - Efficiency: kg of fuel per 100 nm flown

  Materialized as TABLE for query performance (large aggregation).
*/

with normalized as (
    select * from {{ ref('int_telemetry_normalized') }}
),

-- ── Step 1: Physical & Conversion Constants ──────────────────────────────────
constants as (
    select
        1.852::double   as nm_to_km,
        3.785::double   as gal_to_liters,
        0.804::double   as jet_a_density_kg_l, -- Average density for SI conversion
        10.0::double    as min_dist_threshold_nm
),

-- ── Step 2: Establish nominal baselines per engine (first 5 cycles) ──────────
engine_baseline as (
    select
        dataset_id,
        unit_id,
        -- Nominal SFC at fresh engine state
        avg(wf_fuel_flow_pps / NULLIF(throttle_pct, 0.0)) as nominal_sfc,
        -- Nominal SmHPC for stall margin ratio calculation
        avg(sm_hpc)                                      as nominal_sm_hpc,
        -- Nominal peak EGT (corrected) for EGT margin
        max(t48_corr_r)                                  as nominal_peak_t48_corr
    from normalized
    where cycle_num <= 5
    group by 1, 2
),

-- ── Step 3: Aggregate metrics at cycle grain ──────────────────────────────────
cycle_agg as (
    select
        n.dataset_id,
        n.unit_id,
        n.cycle_num,

        -- Flight profile
        max(n.flight_class)                                         as flight_class,
        max(n.health_state)                                         as health_state,
        max(n.rul)                                                  as rul_at_cycle_end,

        -- Health Parameters (Transition signals)
        avg(n.hpc_eff_mod)                                          as avg_hpc_eff_mod,
        avg(n.hpt_eff_mod)                                          as avg_hpt_eff_mod,
        avg(n.lpt_eff_mod)                                          as avg_lpt_eff_mod,
        min(n.hpc_eff_mod)                                          as min_hpc_eff_mod,

        -- EGT / T48 (Observation: thermal stress)
        max(n.t48_corr_r)                                           as peak_t48_corr_r,
        avg(n.t48_corr_r)                                           as avg_t48_corr_r,

        -- Stall Margin (Safety signal)
        avg(n.sm_hpc)                                               as avg_sm_hpc,
        min(n.sm_hpc)                                               as min_sm_hpc,
        avg(n.phi_index)                                            as avg_phi_index,

        -- SFC (Fuel efficiency — mass flow basis)
        avg(n.wf_fuel_flow_pps / NULLIF(n.throttle_pct, 0.0))       as actual_sfc,

        -- Fuel consumption (volumetric, density-corrected)
        sum(n.wf_gph) / 3600.0                                      as total_wf_gal_per_cycle,

        -- Distance flown this cycle
        sum(n.dist_nm_increment)                                    as total_dist_nm,

        -- Altitude & Mach profile
        avg(n.altitude_ft)                                          as avg_alt_ft,
        avg(n.mach_number)                                          as avg_mach

    from normalized n
    group by 1, 2, 3
),

-- ── Step 4: Lifecycle context per engine ──────────────────────────────────────
engine_lifecycle as (
    select
        dataset_id,
        unit_id,
        max(cycle_num) as total_cycles
    from cycle_agg
    group by 1, 2
),

-- ── Step 5: Join and compute derived business metrics ─────────────────────────
final as (
    select
        -- Primary key (MD5 hash for stable identification)
        md5(concat_ws('-', c.dataset_id, c.unit_id::varchar, c.cycle_num::varchar))
                                                        as engine_cycle_pk,
        c.dataset_id::varchar                           as dataset_id,
        c.unit_id::integer                              as unit_id,
        c.cycle_num::integer                            as cycle_num,

        -- Synthetic flight date for Semantic Layer (MetricFlow) time-series support
        (date '2024-01-01' + c.cycle_num)::date         as flight_date,

        -- Lifecycle context
        l.total_cycles::integer                         as total_cycles,
        round(c.cycle_num * 100.0 / NULLIF(l.total_cycles, 0), 1)::double 
                                                        as lifecycle_pct,
        c.flight_class::integer                         as flight_class,
        c.health_state::integer                         as health_state,
        c.rul_at_cycle_end::integer                     as rul_at_cycle_end,

        -- ── Transition: Health Parameters ────────────────────────────────
        round(c.avg_hpc_eff_mod, 5)::double             as avg_hpc_eff_mod,
        round(c.avg_hpt_eff_mod, 5)::double             as avg_hpt_eff_mod,
        round(c.avg_lpt_eff_mod, 5)::double             as avg_lpt_eff_mod,
        round(c.min_hpc_eff_mod, 5)::double             as min_hpc_eff_mod,

        -- ── Observation: EGT Margin Erosion ──────────────────────────────
        round(c.peak_t48_corr_r, 1)::double             as peak_t48_corr_r,
        round(c.peak_t48_corr_r - b.nominal_peak_t48_corr, 1)::double
                                                        as egt_margin_erosion_r,

        -- ── Observation: Stall Margin ────────────────────────────────────
        round(c.avg_sm_hpc, 4)::double                  as avg_sm_hpc,
        round(c.min_sm_hpc, 4)::double                  as min_sm_hpc,
        round(c.min_sm_hpc / NULLIF(b.nominal_sm_hpc, 0.0), 4)::double
                                                        as sm_hpc_ratio,  -- < 0.85 = RED FLAG
        round(c.avg_phi_index, 5)::double               as avg_phi_index,

        -- ── Fuel Efficiency: SFC Degradation ─────────────────────────────
        round(c.actual_sfc, 5)::double                  as actual_sfc,
        round(b.nominal_sfc, 5)::double                 as baseline_sfc,
        round(c.actual_sfc - b.nominal_sfc, 5)::double  as sfc_delta,
        round(((c.actual_sfc - b.nominal_sfc) / NULLIF(b.nominal_sfc, 0.0)) * 100, 2)::double
                                                        as sfc_pct_degradation,

        -- ── Fuel Volume (Density-Corrected) ──────────────────────────────
        round(c.total_wf_gal_per_cycle, 1)::double      as total_wf_gal,
        round(c.total_wf_gal_per_cycle * k.gal_to_liters, 1)::double
                                                        as total_wf_liters,

        -- ── Excess Fuel & Financial Penalty ──────────────────────────────
        -- excess_gallons = (sfc_delta / actual_sfc) * total_wf_gal
        round(
            case when c.actual_sfc > b.nominal_sfc
                then ( (c.actual_sfc - b.nominal_sfc) / NULLIF(c.actual_sfc, 0.0) ) * c.total_wf_gal_per_cycle
                else 0.0
            end, 2
        )::double                                       as excess_gallons,

        round(
            case when c.actual_sfc > b.nominal_sfc
                then ( (c.actual_sfc - b.nominal_sfc) / NULLIF(c.actual_sfc, 0.0) ) * c.total_wf_gal_per_cycle 
                     * p.eia_spot_price_usd_per_gal
                else 0.0
            end, 2
        )::double                                       as market_penalty_usd,

        -- ── Distance & Efficiency per NM ─────────────────────────────────
        round(c.total_dist_nm, 0)::double               as total_dist_nm,
        round(c.total_dist_nm * k.nm_to_km, 0)::double  as total_dist_km,
        
        -- kg per 100 NM (airline efficiency KPI)
        round(
            case when c.total_dist_nm > k.min_dist_threshold_nm
                then (c.total_wf_gal_per_cycle * k.gal_to_liters * k.jet_a_density_kg_l) / (c.total_dist_nm / 100.0)
                else null
            end, 1
        )::double                                       as fuel_kg_per_100nm,

        -- ── Flight Profile ────────────────────────────────────────────────
        round(c.avg_alt_ft, 0)::double                  as avg_alt_ft,
        round(c.avg_mach, 3)::double                    as avg_mach

    from cycle_agg c
    cross join constants k
    join engine_baseline b
        on c.dataset_id = b.dataset_id and c.unit_id = b.unit_id
    join engine_lifecycle l
        on c.dataset_id = l.dataset_id and c.unit_id = l.unit_id
    left join {{ ref('fuel_prices') }} p
        on c.dataset_id = p.dataset_id
)

select * from final
order by dataset_id, unit_id, cycle_num
