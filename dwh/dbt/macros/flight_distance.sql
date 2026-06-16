-- =============================================================================
-- Macro: flight_distance.sql
-- Purpose: Calculate True Airspeed (TAS) and distance from Mach + T2.
-- Reference: ISA atmosphere model (speed of sound formula).
--   a = 49.02 * sqrt(Ts)   [speed of sound in ft/s]
--   Ts = T2 / (1 + 0.2 * M²)  [static temp from total temp]
--   TAS = M * a
-- =============================================================================

{#
  Static temperature (Ts) in Rankine from total temperature T2 and Mach number.
  Formula: Ts = T2 / (1 + 0.2 * M²)
  Added COALESCE(..., 0.0) to force FLOAT context for POW() function.
#}
{% macro static_temp_rankine(t2_col, mach_col) %}
    ({{ t2_col }} / (1.0 + 0.2 * POW(COALESCE({{ mach_col }}, 0.0), 2)))
{% endmacro %}

{#
  Speed of sound in ft/s given static temperature Ts in Rankine.
  Formula: a = 49.021 * SQRT(Ts)
  Note: This is a Dry Air Approximation (ISA standard).
#}
{% macro speed_of_sound_fps(ts_col) %}
    (49.021 * SQRT(CAST({{ ts_col }} AS DOUBLE)))
{% endmacro %}

{#
  True Airspeed (TAS) in knots.
  Uses pre-calculated speed of sound (a_fps) if provided, otherwise calculates from Ts.
#}
{% macro tas_knots(mach_col, ts_col_or_a_fps, is_speed_of_sound=false) %}
    {% if is_speed_of_sound %}
        ((COALESCE({{ mach_col }}, 0.0) * {{ ts_col_or_a_fps }}) / 1.68780986)
    {% else %}
        ((COALESCE({{ mach_col }}, 0.0) * {{ speed_of_sound_fps(ts_col_or_a_fps) }}) / 1.68780986)
    {% endif %}
{% endmacro %}

{#
  Distance increment in Nautical Miles.
  Uses pre-calculated speed of sound (a_fps) if provided.
#}
{% macro dist_nm_increment(mach_col, ts_col_or_a_fps, time_diff_sec=1.0, is_speed_of_sound=false) %}
    {% if is_speed_of_sound %}
        ((COALESCE({{ mach_col }}, 0.0) * {{ ts_col_or_a_fps }} * CAST({{ time_diff_sec }} AS DOUBLE)) / 6076.11549)
    {% else %}
        ((COALESCE({{ mach_col }}, 0.0) * {{ speed_of_sound_fps(ts_col_or_a_fps) }} * CAST({{ time_diff_sec }} AS DOUBLE)) / 6076.11549)
    {% endif %}
{% endmacro %}
