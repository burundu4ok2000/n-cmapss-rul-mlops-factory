-- =============================================================================
-- Macro: iso_corrections.sql
-- Purpose: ISO/ICAO standard atmospheric correction factors for turbofan data.
-- Reference: N-CMAPSS dataset (NASA), ISA standard (ISA 518.67°R / 14.696 psia)
-- =============================================================================

{% macro iso_theta(t2_col) %}
    (CAST({{ t2_col }} AS DOUBLE) / 518.670)
{% endmacro %}

{% macro iso_delta(p2_col) %}
    (CAST({{ p2_col }} AS DOUBLE) / CAST(14.6960 AS DOUBLE))
{% endmacro %}

{#
  Corrected Temperature — removes the "altitude mask"
  Formula: T_corr = T_raw / theta
  Note: Simple linear correction (standard for CMAPSS gas path).
  Sensor Failure: Returns NULL if T_raw is NULL.
#}
{% macro iso_correct_temp(t_raw_col, theta_col) %}
    (CAST({{ t_raw_col }} AS DOUBLE) / NULLIF(({{ theta_col }}), 0.0))
{% endmacro %}

{#
  Corrected Pressure
  Formula: P_corr = P_raw / delta
#}
{% macro iso_correct_press(p_raw_col, delta_col) %}
    (CAST({{ p_raw_col }} AS DOUBLE) / NULLIF(({{ delta_col }}), 0.0))
{% endmacro %}

{#
  Corrected Fan Speed (N1/Nf)
  Formula: Nf_corr = Nf / sqrt(theta)
#}
{% macro iso_correct_speed(n_raw_col, theta_col, sqrt_theta_col=none) %}
    {% set root_theta = sqrt_theta_col if sqrt_theta_col else "SQRT(GREATEST((" ~ theta_col ~ "), 0.00001))" %}
    (CAST({{ n_raw_col }} AS DOUBLE) / NULLIF(({{ root_theta }}), 0.0))
{% endmacro %}

{#
  Corrected Fuel Flow (Wf)
  Formula: Wf_corr = Wf / (delta * sqrt(theta))
#}
{% macro iso_correct_flow(wf_col, delta_col, theta_col, sqrt_theta_col=none) %}
    {% set root_theta = sqrt_theta_col if sqrt_theta_col else "SQRT(GREATEST((" ~ theta_col ~ "), 0.00001))" %}
    (CAST({{ wf_col }} AS DOUBLE) / NULLIF(CAST({{ delta_col }} AS DOUBLE) * ({{ root_theta }}), 0.0))
{% endmacro %}
