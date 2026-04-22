"""
Engineering Guide Processor - Ingestion Orchestrator.
Coordinates guide retrieval and semantic chunking.
"""

import json
import sys
import shutil
import argparse
from pathlib import Path
from loguru import logger
from .guide_client import GuideClient
from .semantic_chunker import SemanticChunker
from .registry import get_url_by_slug, list_all_slugs

class GuideProcessor:
    """
    Orchestrates the lifecycle of an engineering guide ingestion.
    """
    
    def __init__(self, project_root: Path):
        self.root = project_root
        self.client = GuideClient()
        
        # Buffer zone for raw materials (Restricted to .workspace)
        self.raw_dir = self.root / ".workspace" / "guides-data" / "raw-html"
        # Staging area for RAG-ready fragments (Restricted to .workspace)
        self.processed_dir = self.root / ".workspace" / "guides-data"
        
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def process_guide(self, slug: str) -> bool:
        """
        Runs the full ingestion pipeline for a specific guide slug.
        """
        url = get_url_by_slug(slug)
        if not url:
            logger.error(f"Guide slug {slug} not found in registry.")
            return False

        try:
            logger.info(f"Initiating Guide Ingestion for: {slug} (URL: {url})")
            
            # Ensure ephemeral buffer exists for THIS guide
            self.raw_dir.mkdir(parents=True, exist_ok=True)
            
            # 1. Download
            html_content = self.client.fetch_guide_html(url)
            if not html_content:
                return False
            
            # 2. Buffer raw content for audit (Ephemeral)
            raw_path = self.raw_dir / f"{slug.lower()}.html"
            with open(raw_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            # 3. Create Semantic Chunks
            chunker = SemanticChunker(html_content)
            chunks = chunker.chunk_by_headers()
            
            # 4. Save processed fragments to the Data Lake
            output_data = {
                "metadata": {
                    "slug": slug,
                    "url": url,
                    "source": "Official Style Guides (GitHub Pages)",
                    "chunk_count": len(chunks)
                },
                "fragments": chunks
            }
            
            output_path = self.processed_dir / f"{slug.lower()}_fragments.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.success(f"Ingestion successful. {len(chunks)} chunks saved at {output_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Pipeline failure for {slug}: {e}")
            return False
        finally:
            # ATOMIC CLEANUP: Ensure raw buffer remains ephemeral
            if self.raw_dir.exists():
                shutil.rmtree(self.raw_dir)
                logger.debug(f"Temporary staging buffer purged: {self.raw_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Engineering Guides Ingestion CLI")
    parser.add_argument("--slug", help="Guide slug (e.g., GOOGLE_PYTHON_STYLE)")
    args = parser.parse_args()

    # Dynamic root detection: src/infrastructure_setup/guides_ingestion/guide_processor.py
    # parents[4] points to n-cmapss-agentic-factory
    project_root = Path(__file__).resolve().parents[4]
    
    processor = GuideProcessor(project_root)

    if args.slug:
        success = processor.process_guide(args.slug)
        sys.exit(0 if success else 1)
    else:
        logger.info("Syncing full Guides Registry...")
        all_slugs = list_all_slugs()
        for s in all_slugs:
            processor.process_guide(s)
