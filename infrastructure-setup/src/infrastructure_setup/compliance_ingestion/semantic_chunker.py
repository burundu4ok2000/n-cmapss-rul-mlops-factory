"""
Semantic Chunker for EU Legislative Documents.
Converts official HTML to Markdown and splits it into article-based fragments for RAG.
"""

import re
from typing import List, Dict, Any
from markdownify import markdownify as md
from loguru import logger

class SemanticChunker:
    """
    Transforms legal HTML into semantically separated Markdown chunks.
    """
    
    def __init__(self, html_content: str):
        self.html = html_content
        # Multi-modal pattern: catches Article/Chapter/Section with any whitespace (including \xa0)
        # and optional markdown formatting or headers.
        self.article_pattern = re.compile(
            r'(?m)^((?:#+\s+)?(?:\*\*|__)?(?:Article|Chapter|Section)[\s\xa0]+\d+.*(?:\*\*|__)?)$'
        )

    def to_markdown(self) -> str:
        """Converts raw legal HTML to a structured Markdown representation."""
        logger.debug("Converting legal HTML to Markdown (ATX style)...")
        return md(self.html, heading_style="ATX", strip=['script', 'style', 'img'])

    def chunk_by_articles(self) -> List[Dict[str, Any]]:
        """
        Splits the Markdown text into chunks based on Article headers.
        Returns a list of dictionaries with metadata and content.
        """
        markdown_text = self.to_markdown()
        
        # Split text while keeping the headers (delimiters)
        # We split by the pattern, then group pairs of (header, content)
        parts = self.article_pattern.split(markdown_text)
        
        # The first part is usually the preamble/pre-article text
        preamble = parts[0].strip()
        chunks = []
        
        if preamble:
            chunks.append({
                "metadata": {"section": "Preamble/Recitals", "type": "intro"},
                "text": preamble
            })
        
        # Subsequent parts come in pairs: [Header, Content, Header, Content, ...]
        for i in range(1, len(parts), 2):
            header = parts[i].strip()
            content = parts[i+1].strip() if i+1 < len(parts) else ""
            
            # Clean up the header to get a clean Article ID
            article_match = re.search(r'Article\s+(\d+\w?)', header)
            article_id = article_match.group(1) if article_match else header
            
            chunks.append({
                "metadata": {
                    "section": header.lstrip('#').strip(),
                    "article_id": article_id,
                    "type": "article"
                },
                "text": f"{header}\n\n{content}"
            })
            
        logger.info(f"Generated {len(chunks)} semantic chunks from legislative text.")
        return chunks
