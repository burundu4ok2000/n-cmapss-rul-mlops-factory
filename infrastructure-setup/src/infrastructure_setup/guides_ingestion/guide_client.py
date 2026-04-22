"""
Client for Engineering Guides Ingestion.
Handles robust retrieval of web-based documentation.
"""

import httpx
from loguru import logger
from typing import Optional

class GuideClient:
    """
    Handles retrieval of public documentation and guides.
    """
    
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    def fetch_guide_html(self, url: str) -> Optional[str]:
        """
        Retrieves HTML content from the provided URL.
        """
        logger.info(f"Ingesting guide content from: {url}")
        
        try:
            with httpx.Client(follow_redirects=True, timeout=self.timeout) as client:
                response = client.get(url, headers=self.headers)
                response.raise_for_status()
                
                return response.text
        except httpx.HTTPError as e:
            logger.error(f"Failed to retrieve guide from {url}: {e}")
            
        return None
