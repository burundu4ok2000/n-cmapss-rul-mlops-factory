-- =============================================================================
-- Macro: fuel_density.sql
-- Purpose: Dynamic Jet-A fuel density based on inlet temperature.
-- Reference: Aviation standard (ASTM D1655), k ≈ 0.0031 lbs/gal per °F
--   Base density: 6.7 lbs/gal at 60°F (15.5°C)
--   At cruise altitude (T2 ≈ 460°R / 0°F), density rises to ~6.89 lbs/gal.
--   Using a constant 6.7 introduces ~2.87% error in volumetric accounting.
-- =============================================================================

{#
  Fuel density (lbs/gal) as a function of T2 in Rankine.
  T2 (°R) → T2_Fahrenheit = T2 - 459.67
  rho = 6.7 - 0.0031 * (T2_F - 60)
  
  - Baseline: 6.7 lbs/gal at 60°F reference temp.
  
  - Expansion Coeff: 0.0031 lbs/gal per 1°F.
  
  - Cold Clamp (7.1): Near Jet A-1 freezing point (-47°C/-52.6°F). 
    Ex: 6.7 - 0.0031 * (-50°F - 60°F) ≈ 7.04 lbs/gal.
  
  - Hot Clamp (6.4): Extreme ambient/operating temp (approx. +150°F).
    Ex: 6.7 - 0.0031 * (122°F - 60°F) ≈ 6.51 lbs/gal.
#}
{% macro fuel_density_lbs_gal(temp_rankine_col) %}
    LEAST(
        GREATEST(
            (6.7 - 0.0031 * (({{ temp_rankine_col }} - 459.67) - 60.0)),
            6.4
        ),
        7.1
    )
{% endmacro %}

{#
  Convert mass flow (Wf in lbs/sec) to volumetric flow (gallons/hour).
  Formula: GPH = (Wf [lbs/s] × 3600 [s/hr]) / rho [lbs/gal]
#}
{% macro fuel_flow_gph(wf_col, rho_col) %}
    (({{ wf_col }} * 3600.0) / NULLIF({{ rho_col }}, 0))
{% endmacro %}

{#
  Convert mass flow (lbs/sec) to kg/sec (SI standard).
  1 lb = 0.453592 kg
#}
{% macro fuel_flow_kg_s(wf_col) %}
    ({{ wf_col }} * 0.453592)
{% endmacro %}
