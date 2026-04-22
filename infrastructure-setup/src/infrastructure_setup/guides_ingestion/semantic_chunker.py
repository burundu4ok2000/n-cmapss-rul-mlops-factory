"""
Semantic Chunker for Engineering Guides.
Converts HTML to Markdown and splits into section-based fragments.
"""

import re
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from loguru import logger

class SemanticChunker:
    """
    Transforms documentation HTML into semantically separated Markdown chunks 
    based on header hierarchy.
    """
    
    def __init__(self, html_content: str):
        self.html = html_content
        self.code_cache = {}

    def to_markdown(self) -> str:
        """
        Converts raw HTML to Markdown while protecting code blocks.
        """
        logger.debug("Protecting code blocks from markdownify...")
        soup = BeautifulSoup(self.html, 'html.parser')
        
        self.code_cache = {}
        for i, pre in enumerate(soup.find_all('pre')):
            placeholder = f"CODETOKEN{i}BLOCK"
            self.code_cache[placeholder] = pre.get_text()
            pre.replace_with(placeholder)
            
        logger.debug("Converting sanitized HTML to Markdown...")
        return md(str(soup), heading_style="ATX", strip=['script', 'style', 'nav', 'footer'])

    def _clean_text(self, text: str) -> List[str]:
        """
        Industrial sanitization:
        1. Removes JSON-LD and tech debris.
        2. Unwraps lines into continuous paragraphs.
        3. Separates code tokens from surrounding text (e.g. footnotes).
        """
        text = re.sub(r'\{"@context":.*\}', '', text)
        text = re.sub(r'\[([^\]]+)\]\(#[^\)]+\)', r'\1', text)

        blocks = text.split('\n\n')
        processed_blocks = []
        
        for block in blocks:
            lines = [l.strip() for l in block.splitlines() if l.strip()]
            if not lines:
                continue
            
            current_segment = []
            
            for line in lines:
                is_token = any(token in line for token in self.code_cache.keys())
                is_structural = line.startswith(('*', '-', '+', '>', '1.', '2.', '3.', 'Yes:', 'No:'))
                
                # Boundary detected: Token or List Start or Label Start
                if is_token or is_structural:
                    if current_segment:
                        processed_blocks.append(" ".join(current_segment))
                        current_segment = []
                    
                    if is_token:
                        processed_blocks.append(line)
                    else:
                        # Structural lines (lists/labels) start a new segment immediately
                        current_segment = [line]
                else:
                    # Regular line - append to current segment for unwrapping
                    current_segment.append(line)
            
            if current_segment:
                processed_blocks.append(" ".join(current_segment))

        return [b.strip() for b in processed_blocks if b.strip()]

    def chunk_by_headers(self) -> List[Dict[str, Any]]:
        """
        Splits Markdown text into chunks based on Headers (up to H5), then restores code.
        """
        markdown_text = self.to_markdown()
        lines = markdown_text.splitlines()
        
        chunks = []
        current_section = "Introduction"
        current_level = 0
        current_content = []
        
        header_regex = re.compile(r'^(#{1,5})\s+(.*)$')

        for line in lines:
            header_match = header_regex.match(line)
            
            if header_match:
                if current_content:
                    paragraphs = self._clean_text("\n".join(current_content))
                    if paragraphs:
                        # RESTORE CODE BLOCKS HERE, after all cleaning is done
                        final_paragraphs = []
                        for p in paragraphs:
                            for token, code in self.code_cache.items():
                                if token in p:
                                    p = p.replace(token, f"```\n{code}\n```")
                            final_paragraphs.append(p)

                        chunks.append({
                            "metadata": {
                                "section": current_section,
                                "level": current_level,
                                "type": "guide_section"
                            },
                            "text": final_paragraphs
                        })
                
                hashes, title = header_match.groups()
                current_level = len(hashes)
                section_parts = self._clean_text(title.strip())
                current_section = section_parts[0] if section_parts else title.strip()
                current_content = [line]
            else:
                current_content.append(line)

        if current_content:
            paragraphs = self._clean_text("\n".join(current_content))
            if paragraphs:
                final_paragraphs = []
                for p in paragraphs:
                    for token, code in self.code_cache.items():
                        if token in p:
                            p = p.replace(token, f"```\n{code}\n```")
                    final_paragraphs.append(p)

                chunks.append({
                    "metadata": {
                        "section": current_section,
                        "level": current_level,
                        "type": "guide_section"
                    },
                    "text": final_paragraphs
                })
        
        return chunks
