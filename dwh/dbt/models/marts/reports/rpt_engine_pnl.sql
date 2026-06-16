{{
    config(
        materialized='table',
        description='Financial P&L report. One row per (dataset, unit, cycle). Calculates daily excess fuel cost using density-corrected volumetric fuel and EIA spot prices.'
    )
}}

/*
  rpt_engine_pnl — Financial Vitrine (Report Layer)
  ==================================================
  Source: fct_engine_health_per_cycle + fuel_prices seed
  Grain: 1 row per (dataset, unit, cycle)

  Key Concept — Opportunity Cost:
    We use EIA Spot Price as the "Replacement Cost" benchmark.
    Even if the airline hedged fuel cheaply, excess burned fuel displaces
    inventory that must be replenished at the marginal spot price.
    Formula: market_penalty_usd = excess_gallons × eia_spot_price
    Dual metric: actual_cash_penalty_usd = excess_gallons × COALESCE(hedged_price, spot_price)
    
  Excess gallons = (sfc_delta × throttle_avg × cycle_seconds) / rho
*/

with health as (
    select * from {{ ref('fct_engine_health_per_cycle') }}
),

prices as (
    select * from {{ ref('fuel_prices') }}
),

final as (
    select
        h.engine_cycle_pk,
        h.dataset_id,
        h.unit_id,
        h.cycle_num,
        h.flight_date,
        h.lifecycle_pct,
        h.flight_class,
        h.rul_at_cycle_end,

        -- ── Financial Context ───────────────────────────────────────────
        p.region_label,
        p.eia_spot_price_usd_per_gal,
        p.hedged_price_usd_per_gal,

        -- ── Operational Efficiency (from Gold Layer) ────────────────────
        h.excess_gallons,
        h.market_penalty_usd,
        
        -- Actual Cash Penalty: uses hedged price if available, else spot
        round(
            h.excess_gallons * COALESCE(p.hedged_price_usd_per_gal, p.eia_spot_price_usd_per_gal),
            2
        )::double                                       as actual_cash_penalty_usd,

        h.fuel_kg_per_100nm,
        h.sfc_pct_degradation,

        -- ── Annualized Economic Impact ──────────────────────────────────
        -- Fc=1 (Short) -> 730 cyc/yr, Fc=2 (Med) -> 365, Fc=3 (Long) -> 260
        round(
            h.market_penalty_usd * case h.flight_class 
                when 1 then 730 
                when 2 then 365 
                else 260 
            end, 0
        )::double                                       as market_penalty_usd_annualized,

        -- ── Health Summary ──────────────────────────────────────────────
        h.avg_hpc_eff_mod,
        h.egt_margin_erosion_r,
        h.sm_hpc_ratio

    from health h
    left join prices p on h.dataset_id = p.dataset_id
)

select * from final
order by market_penalty_usd desc
