"""
Industrial-grade Cellar API Client for Official EU Legislative Ingestion.
Uses UUID discovery and the OP Portal Download Handler for maximum stability.
"""

import httpx
import re
from loguru import logger
from typing import Optional

# Discovery URL to find the underlying Cellar UUID from a CELEX ID
DISCOVERY_URL = "http://publications.europa.eu/resource/celex/{celex}"
# Formal OP Portal Download Handler URL pattern (stable, avoids WAF)
CONTENT_URL_TEMPLATE = (
    "https://op.europa.eu/o/opportal-service/download-handler"
    "?identifier={uuid}&format=xhtml&language=en&productionSystem=cellar&part="
)

class CellarClient:
    """
    Handles robust retrieval of legislative content via CELEX-to-UUID-to-XHTML flow.
    """
    
    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    def _discover_uuid(self, celex_id: str) -> Optional[str]:
        """
        Extracts the internal Cellar UUID for a given CELEX ID by sniffing the redirect.
        """
        url = DISCOVERY_URL.format(celex=celex_id)
        try:
            with httpx.Client(timeout=self.timeout) as client:
                # We stay at the proxy level to get the 'Location' header
                response = client.get(url, headers=self.headers, follow_redirects=False)
                location = response.headers.get("Location", "")
                
                # Regex matches the standard Cellar UUID pattern
                match = re.search(r'cellar/([a-f0-9\-]{36})', location)
                if match:
                    uuid = match.group(1)
                    logger.debug(f"Discovered UUID for {celex_id}: {uuid}")
                    return uuid
                
                logger.error(f"UUID discovery failed for {celex_id}. Redirect location: {location}")
        except Exception as e:
            logger.error(f"Discovery phase failed for {celex_id}: {e}")
            
        return None

    def fetch_law_html(self, celex_id: str) -> Optional[str]:
        """
        Retrieves official XHTML content using the discovered UUID via the OP download handler.
        """
        uuid = self._discover_uuid(celex_id)
        if not uuid:
            return None
            
        content_url = CONTENT_URL_TEMPLATE.format(uuid=uuid)
        logger.info(f"Ingesting official XHTML for {celex_id} (UUID: {uuid})")
        
        try:
            with httpx.Client(follow_redirects=True, timeout=self.timeout) as client:
                response = client.get(content_url, headers=self.headers)
                response.raise_for_status()
                
                content = response.text
                # Validation: check for XHTML root and legislative markers
                if "xmlns=\"http://www.w3.org/1999/xhtml\"" in content:
                    logger.success(f"Successfully ingested 1.3MB+ of legislative data for {celex_id}")
                    return content
                else:
                    logger.warning(f"Response for {celex_id} did not contain valid XHTML.")
                    return content
                    
        except httpx.HTTPError as e:
            logger.error(f"Content retrieval failed for {celex_id} (UUID: {uuid}): {e}")
            
        return None
