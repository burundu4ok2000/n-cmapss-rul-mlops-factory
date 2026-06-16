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
COMPONENT: CLOUD_TRAINER
SUBCOMPONENT: FACTORY TELEMETRY

VERSION: 0.1.0
STATUS: CONSTRUCTION
AUTHOR: Stanislav Burundukov (@Stan_Buren)

COMPLIANCE:
    - ISO 8601 Temporal Anchoring (TimeStamper)
    - EU AI Act Machine-Readability (JSONRenderer)

DIAGNOSTIC LAYERS:
    - ContextVars: Global metadata "magnet" (e.g., run_id).
    - LogLevel: Severity categorization (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    - StackInfo: Runtime call hierarchy ("how we got here").
    - ExcInfo: Structured traceback serialization for failure analysis.

OUTPUT CHANNELS:
    - Production: Google Cloud Logging API (Structured Metadata).
    - Development: Human-friendly colorized console output.

RESILIENCE & FAILOVER:
    - Defensive Provisioning: Guarantees audit log directory existence before initialization.
    - API Fallback: Graceful degradation to binary file logging if Google Cloud Logging API is unreachable.

PERFORMANCE:
    - Caching: Caches logger instances on first use to minimize overhead.

CONFIGURATION:
    - LOG_LEVEL: Environment variable (DEBUG/INFO/WARNING/ERROR/CRITICAL). Defaults to INFO.

"""

# ==============================================================================
# DEPENDENCIES
# ==============================================================================

# 1. Standard Library Dependencies
import functools
import logging
import os
from pathlib import Path
import time


# 2. Third-Party Dependencies
import structlog

# ==============================================================================
# CONSTANTS & CONFIGURATION
# ==============================================================================

ENV_VAR_LOG_LEVEL = "LOG_LEVEL"
ENV_VAR_ENVIRONMENT = "ENVIRONMENT"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_ENV_NAME = "local"
PROD_ENV_NAME = "production"
LOG_FORMAT = "%(message)s"
TIMESTAMP_FORMAT = "iso"
FILE_APPEND_MODE = "ab"
CACHE_LOGGERS = True

# ==============================================================================
# SYSTEM SETUP: LOGGING & AUDIT
# ==============================================================================

logger = structlog.get_logger(__name__)

# ==============================================================================
# FUNCTIONS
# ==============================================================================

def setup_logging(log_file: Path):
    """Initializes the dual-stream audit logging infrastructure.

    Configures a hybrid logging pipeline that routes human-readable console
    output for local development and machine-parsable JSON metadata for
    production/cloud environments.

    Args:
        log_file: Path to the destination file where JSON audit logs will be
            persisted (production failover or local analysis).
    """
    is_production = os.getenv(ENV_VAR_ENVIRONMENT) == PROD_ENV_NAME
    
    log_sink = DEFAULT_ENV_NAME
    if is_production:
        try:
            from google.cloud import logging as cloud_logging
            client = cloud_logging.Client()
            client.setup_logging()
            logger_factory = structlog.stdlib.LoggerFactory()
            log_sink = "google_cloud"
        except (ImportError, Exception):
            log_file.parent.mkdir(parents=True, exist_ok=True)
            logger_factory = structlog.BytesLoggerFactory(open(log_file, FILE_APPEND_MODE))
            log_sink = "failover_file"
    else:
        logger_factory = structlog.PrintLoggerFactory()
        log_sink = "console"

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt=TIMESTAMP_FORMAT),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer() if is_production 
        else structlog.dev.ConsoleRenderer(colors=True)
    ]

    structlog.configure(
        processors=processors,
        logger_factory=logger_factory,
        cache_logger_on_first_use=CACHE_LOGGERS,
    )

    log_level_name = os.getenv(ENV_VAR_LOG_LEVEL, DEFAULT_LOG_LEVEL).upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    logging.basicConfig(format=LOG_FORMAT, level=log_level)
    
    # Confirmation: The first audit entry in the telemetry stream.
    logger.info(
        "factory_telemetry_system_ready", 
        env=os.getenv(ENV_VAR_ENVIRONMENT, DEFAULT_ENV_NAME), 
        level=log_level_name,
        log_sink=log_sink,
        backup_path=str(log_file) if is_production else None,
        is_production=is_production
    )


def bind_telemetry_context(**kwargs):
    """Binds global metadata to the current logging context.
    
    All subsequent log entries within the current process/thread will 
    automatically include these key-value pairs (e.g., run_id, model_version).

    Args:
        **kwargs: Arbitrary metadata keys and values to inject into logs.
    """
    if not kwargs:
        logger.warning("telemetry_context_binding_skipped", reason="no_data_provided")
        return

    # Sanitize: Ensure we don't bind complex non-serializable objects by accident.
    sanitized_context = {k: str(v) for k, v in kwargs.items() if v is not None}
    
    structlog.contextvars.bind_contextvars(**sanitized_context)
    
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
