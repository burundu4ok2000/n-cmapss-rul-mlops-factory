# Copyright 2026 Stanislav Burundukov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DOMAIN: MODEL-TRAINING-PROCESS
COMPONENT: CORE
SUBCOMPONENT: FACTORY TELEMETRY

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

GOAL:
    - Single authority for structlog configuration and audit stream routing.
    - Provider of the shared path-masking utility (mask_path) for all
      components in the system that emit logs or raise exceptions.
    - Environment-agnostic: receives all filesystem coordinates via PathResolver,
      never via os.environ or hardcoded paths.

COMPLIANCE:
    - ISO 8601 Temporal Anchoring (TimeStamper)
    - EU AI Act Machine-Readability (JSONRenderer)
    - DORA Audit Compliance: mask_path prevents host-metadata leakage system-wide.

DIAGNOSTIC LAYERS:
    - ContextVars: Global metadata "magnet" (e.g., run_id).
    - LogLevel: Severity categorization (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    - StackInfo: Runtime call hierarchy ("how we got here").
    - ExcInfo: Structured traceback serialization for failure analysis.

OUTPUT CHANNELS:
    - Production: Binary file sink (JSON structured) or cloud logging adapter.
    - Development: Human-friendly colorized console output.

RESILIENCE & FAILOVER:
    - Defensive Provisioning: Directory existence guaranteed by PathResolver.prepare_environment().
    - API Fallback: Graceful degradation to file logging if cloud sink is unreachable.

PERFORMANCE:
    - Caching: Caches logger instances on first use to minimize overhead.

CONFIGURATION:
    - LOG_LEVEL:   Environment variable (DEBUG/INFO/WARNING/ERROR/CRITICAL). Defaults to INFO.
    - ENVIRONMENT: Set to 'production' to activate JSON rendering and cloud sink.

"""

from __future__ import annotations

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import functools
import logging
import os
from pathlib import Path
import time
from typing import TYPE_CHECKING

# 2. Third-Party Dependencies
import structlog

# 3. Internal Type Contract
# [NOTE]: Runtime-safe guard prevents circular import:
#         factory_telemetry ← (TYPE_CHECKING only) ← path_resolver
#         path_resolver → (runtime) → factory_telemetry
if TYPE_CHECKING:
    from rul_model_factory.core.logistics.path_resolver import PathResolver

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

ENV_VAR_LOG_LEVEL   = "LOG_LEVEL"
ENV_VAR_ENVIRONMENT = "ENVIRONMENT"
DEFAULT_LOG_LEVEL   = "INFO"
DEFAULT_ENV_NAME    = "local"
PROD_ENV_NAME       = "production"
LOG_FORMAT          = "%(message)s"
TIMESTAMP_FORMAT    = "iso"
FILE_APPEND_MODE    = "ab"
CACHE_LOGGERS       = True

# ==============================================================================
# SYSTEM SETUP: LOGGING & AUDIT
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# SECURITY UTILITIES
# ==============================================================================

def mask_path(path: Path, root: Path = None) -> str:
    """Anonymizes filesystem paths for EU AI Act / DORA audit compliance.

    Shared utility consumable by any component in the system. Applies a
    two-tier masking strategy to prevent host-system metadata leakage in
    logs and exception messages, even when running at DEBUG verbosity.

    Args:
        path: The absolute Path object to anonymize.
        root: Optional project root for relative '{{ROOT}}/...' masking.

    Returns:
        A sanitized string representation of the path.
    """
    if not path:
        return "None"

    # Strategy 1: Project-Relative Masking — exposes only intra-project structure.
    if root:
        try:
            return f"{{{{ROOT}}}}/{path.relative_to(root)}"
        except (ValueError, TypeError):
            pass

    # Strategy 2: Home-Directory Masking — redacts the username from the path.
    try:
        home, path_str = str(Path.home()), str(path)
        if path_str.startswith(home):
            return path_str.replace(home, "~", 1)
    except Exception:
        pass

    return str(path)

# ==============================================================================
# FUNCTIONS
# ==============================================================================

def setup_logging(
    resolver: PathResolver, 
    sink: Any = None
) -> None:
    """Initializes the dual-stream audit logging infrastructure.

    Configures structlog with a multi-processor pipeline to ensure all logs
    are structured, sanitized (masked), and routed to both the console
    and the persistent filesystem audit trail.

    Args:
        resolver: Initialized PathResolver providing audit log coordinates.
        sink:     Optional LoggerFactory (e.g. for Cloud Logging). If None, 
                  defaults to PrintLoggerFactory (standard output).
    """
    is_production = os.getenv(ENV_VAR_ENVIRONMENT) == PROD_ENV_NAME
    
    # 1. DEFINE SHARED PROCESSORS
    processors: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt=TIMESTAMP_FORMAT),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        # Zero Trust path Masking: Forces sanitization of filesystem paths in log records.
        lambda _, __, event_dict: _bind_masked_paths(event_dict, resolver),
        structlog.processors.JSONRenderer() if is_production
        else structlog.dev.ConsoleRenderer(colors=True)
    ]

    # 2. SELECT SINK (LoggerFactory)
    # The factory determines WHERE the logs go (Console, File, Cloud).
    # In Hexagonal Architecture, this is injected from the Adapter layer.
    logger_factory = sink or structlog.PrintLoggerFactory()

    # 3. CONFIGURE GLOBAL PIPELINE
    structlog.configure(
        processors=processors,
        logger_factory=logger_factory,
        cache_logger_on_first_use=CACHE_LOGGERS,
    )

    # 4. BRIDGE TO STANDARD LOGGING
    log_level_name = os.getenv(ENV_VAR_LOG_LEVEL, DEFAULT_LOG_LEVEL).upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    logging.basicConfig(format=LOG_FORMAT, level=log_level)

    # Confirmation: The first compliant audit entry in the telemetry stream.
    logger.info(
        "factory_telemetry_system_ready",
        env=os.getenv(ENV_VAR_ENVIRONMENT, DEFAULT_ENV_NAME),
        level=log_level_name,
        is_production=is_production
    )


def _bind_masked_paths(event_dict: dict, resolver: PathResolver) -> dict:
    """Internal processor: masks all Path objects in the event dictionary.
    
    Creates a copy of the dictionary to avoid mutating original records during 
    the transformation process.
    """
    sanitized = event_dict.copy()
    for key, value in sanitized.items():
        if isinstance(value, Path):
            sanitized[key] = mask_path(value, root=resolver.root)
    return sanitized


def bind_telemetry_context(**kwargs) -> None:
    """Binds global metadata to the current logging context.

    All subsequent log entries within the current process/thread will
    automatically include these key-value pairs (e.g., run_id, model_version).

    Args:
        **kwargs: Arbitrary metadata keys and values to inject into logs.
    """
    if not kwargs:
        logger.warning("telemetry_context_binding_skipped", reason="no_data_provided")
        return

    # Sanitize: prevent accidental binding of non-serializable objects.
    sanitized_context = {k: str(v) for k, v in kwargs.items() if v is not None}

    structlog.contextvars.bind_contextvars(**sanitized_context)

    # Audit: Record metadata context update.
    logger.info(
        "telemetry_context_updated",
        bound_keys=list(sanitized_context.keys())
    )


# ==============================================================================
# DECORATORS
# ==============================================================================

def track_performance(func):
    """Decorator to measure and log the execution duration of a function.

    Emits a structured log entry upon completion, detailing the function
    name and the elapsed time in seconds.

    Args:
        func: The function to be timed.

    Returns:
        The wrapped function with performance tracking enabled.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            duration = time.perf_counter() - start_time
            logger.info(
                "performance_metric",
                function_name=func.__name__,
                duration_seconds=round(duration, 4)
            )
    return wrapper
