{{
    config(
        materialized='table',
        description='Safety monitoring report. One row per (dataset, unit). Tracks FADEC limit proximity and stall margin erosion across the lifecycle. Outputs composite alert priority score.'
    )
}}

/*
  rpt_safety_alerts — Safety Monitoring Vitrine
  ==============================================
  Source: fct_engine_health_per_cycle
  Grain: 1 row per (dataset, unit) — fleet-level dashboard view

  FADEC Limit Thresholds (from NASA C-MAPSS paper, Table 1):
    - T48 (EGT): Alert if peak corrected T48 > 1950°R
    - Ps30 (HPC Pressure): Alert if max Ps30 > 440 psia
    - SmHPC (Stall Margin): Alert if min_sm_hpc_ratio < 0.85 (< 85% of nominal)

  Composite Alert Priority (1-10 scale):
    Each breach adds severity points. >7 = immediate maintenance required.
*/

with health as (
    select * from {{ ref('fct_engine_health_per_cycle') }}
),

-- ── End-of-life snapshot (last 5 cycles) ─────────────────────────────────────
eol_snapshot as (
    select
        dataset_id,
        unit_id,
        avg(peak_t48_corr_r)        as eol_avg_peak_t48,
        avg(egt_margin_erosion_r)   as eol_avg_egt_erosion,
        avg(avg_sm_hpc)             as eol_avg_sm_hpc,
        min(sm_hpc_ratio)           as eol_min_sm_hpc_ratio,
        avg(avg_phi_index)          as eol_avg_phi,
        avg(sfc_pct_degradation)    as eol_avg_sfc_degradation
    from health
    where lifecycle_pct >= 80
    group by 1, 2
),

-- ── Beginning-of-life snapshot (first 5 cycles) ───────────────────────────────
bol_snapshot as (
    select
        dataset_id,
        unit_id,
        avg(peak_t48_corr_r)        as bol_avg_peak_t48,
        max(flight_class)           as dominant_flight_class,
        max(total_cycles)           as total_cycles
    from health
    where cycle_num <= 5
    group by 1, 2
),

final as (
    select
        b.dataset_id,
        b.unit_id,
        b.total_cycles,
        b.dominant_flight_class,

        -- ── EGT Safety ────────────────────────────────────────────────────
        round(b.bol_avg_peak_t48, 1)                as bol_peak_t48_r,
        round(e.eol_avg_peak_t48, 1)                as eol_peak_t48_r,
        round(e.eol_avg_egt_erosion, 1)             as egt_erosion_r,
        e.eol_avg_peak_t48 > 1950                   as flag_egt_limit,

        -- ── Stall Margin Safety ───────────────────────────────────────────
        round(e.eol_avg_sm_hpc, 4)                  as eol_avg_sm_hpc,
        round(e.eol_min_sm_hpc_ratio, 4)            as eol_min_sm_hpc_ratio,
        e.eol_min_sm_hpc_ratio < 0.85               as flag_stall_risk,

        -- ── SFC Degradation ───────────────────────────────────────────────
        round(e.eol_avg_sfc_degradation, 2)         as eol_sfc_degradation_pct,
        e.eol_avg_sfc_degradation > 5.0             as flag_fuel_inefficiency,

        -- ── Phi Index (Stall Proximity) ───────────────────────────────────
        round(e.eol_avg_phi, 5)                     as eol_avg_phi,

        -- ── Composite Alert Priority (0-10) ──────────────────────────────
        -- Each flag contributes weighted points
        round(
            (case when e.eol_avg_peak_t48 > 1950 then 3.0
                  when e.eol_avg_peak_t48 > 1920 then 1.5
                  else 0 end)
            +
            (case when e.eol_min_sm_hpc_ratio < 0.85 then 4.0
                  when e.eol_min_sm_hpc_ratio < 0.90 then 2.0
                  else 0 end)
            +
            (case when e.eol_avg_sfc_degradation > 8 then 2.0
                  when e.eol_avg_sfc_degradation > 5 then 1.0
                  else 0 end)
            +
            (case when e.eol_avg_egt_erosion > 80 then 1.0
                  when e.eol_avg_egt_erosion > 50 then 0.5
                  else 0 end)
        , 1)                                        as alert_priority,

        -- ── Human-readable alert level ────────────────────────────────────
        case
            when (e.eol_min_sm_hpc_ratio < 0.85 or e.eol_avg_peak_t48 > 1950) then 'CRITICAL'
            when (e.eol_min_sm_hpc_ratio < 0.90 or e.eol_avg_peak_t48 > 1920) then 'WARNING'
            when e.eol_avg_sfc_degradation > 5 then 'MONITOR'
            else 'NOMINAL'
        end                                         as alert_level

    from bol_snapshot b
    join eol_snapshot e
        on b.dataset_id = e.dataset_id and b.unit_id = e.unit_id
)

select * from final
order by alert_priority desc
