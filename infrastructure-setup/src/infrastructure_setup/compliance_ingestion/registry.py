"""
Regulatory Compliance Registry for EU Legislative Documents.
Centrally manages CELEX identifiers and slugs for the ingestion pipeline.
"""

from typing import Dict, Optional, List

REGISTRY: Dict[str, str] = {
    "AI_ACT": "32024R1689",
    "DORA": "32022R2554",
    "NIS2": "32022L2555",
    "CRA": "32024R2847",  # Cyber Resilience Act
}

def get_celex_by_slug(slug: str) -> Optional[str]:
    """Returns the CELEX ID for a given regulatory slug."""
    return REGISTRY.get(slug.upper())

def list_all_slugs() -> List[str]:
    """Returns a list of all supported regulatory slugs."""
    return list(REGISTRY.keys())

def get_all_regulations() -> Dict[str, str]:
    """Returns the full registry of supported regulations."""
    return REGISTRY
