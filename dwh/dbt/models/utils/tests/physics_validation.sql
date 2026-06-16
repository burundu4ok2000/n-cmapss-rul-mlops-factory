-- models/utils/tests/physics_validation.sql
-- This model exists SOLELY to enable Unit Testing of our physical/ISO macros.
-- It maps raw inputs to macro outputs so we can verify the math in isolation.

with raw_data as (
    select * from {{ ref('stg_telemetry') }}
),

processed as (
    select
        -- 1. Atmosphere / ISO Basics
        {{ iso_theta('t2_inlet_temp_r') }}                  as theta,
        {{ iso_delta('p2_inlet_psia') }}                    as delta,
        {{ static_temp_rankine('t2_inlet_temp_r', 'mach_number') }} as t_static_r,
        
        -- Density in lbs/gal (for GPH calculation)
        {{ fuel_density_lbs_gal('t2_inlet_temp_r') }}       as fuel_rho_lbs_gal,

        -- 2. Core Corrections
        {{ iso_correct_temp('t24_lpc_outlet_r', iso_theta('t2_inlet_temp_r')) }} as t24_corr,
        {{ iso_correct_press('p24_lpc_outlet_psia', iso_delta('p2_inlet_psia')) }} as p24_corr,
        
        -- 3. Advanced Flows/Speeds
        {{ iso_correct_speed('nc_core_speed_rpm', iso_theta('t2_inlet_temp_r')) }} as n2_corr,
        {{ iso_correct_flow('w21_fan_flow_pps', iso_delta('p2_inlet_psia'), iso_theta('t2_inlet_temp_r')) }} as w21_corr,

        -- 4. Fuel Physics
        wf_fuel_flow_pps

    from raw_data
)

select
    *,
    -- Conversion to kg/l for Gold layer compatibility (1 lb/gal ≈ 0.119826 kg/l)
    (fuel_rho_lbs_gal * 0.119826)                           as fuel_rho_kg_l,
    
    -- Volumetric flow in GPH
    {{ fuel_flow_gph('wf_fuel_flow_pps', 'fuel_rho_lbs_gal') }} as fuel_gph

from processed
limit 10
