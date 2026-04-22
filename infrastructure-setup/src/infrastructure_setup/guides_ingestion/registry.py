"""
Guides Registry.
Centrally manages URLs and slugs for the internal engineering guards.
"""

from typing import Dict, List, Optional

REGISTRY: Dict[str, str] = {
    "GOOGLE_PYTHON_STYLE": "https://google.github.io/styleguide/pyguide.html",
}

def get_url_by_slug(slug: str) -> Optional[str]:
    """Returns the URL for a given guide slug."""
    return REGISTRY.get(slug.upper())

def list_all_slugs() -> List[str]:
    """Returns a list of all supported guide slugs."""
    return list(REGISTRY.keys())
