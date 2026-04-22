"""
Regulatory Law Processor - Ingestion Orchestrator.
Coordinates official HTML retrieval and semantic chunking for the Agentic Factory.
ALIGNED WITH GOLDEN LAYER ARCHITECTURE (V1).
"""

import json
import shutil
from pathlib import Path
from loguru import logger
from .cellar_client import CellarClient
from .semantic_chunker import SemanticChunker
from .registry import get_celex_by_slug, list_all_slugs

class LawProcessor:
    """
    Orchestrates the lifecycle of a legislative document ingestion:
    Official Source -> Raw Buffer -> Semantic Chunks -> Data Lake (Staging).
    
    All storage is restricted to the .workspace/ directory to maintain root cleanliness.
    """
    
    def __init__(self, project_root: Path):
        self.root = project_root
        self.client = CellarClient()
        
        # Buffer zone for raw materials (EPHEMERAL - Restricted to .workspace)
        self.raw_dir = self.root / ".workspace" / "compliance-data" / ".raw-xml"
        
        # Staging area for RAG-ready fragments (Restricted to .workspace)
        self.processed_dir = self.root / ".workspace" / "compliance-data"
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def process_regulation(self, slug: str) -> bool:
        """
        Runs the full ingestion pipeline for a specific regulation slug.
        """
        celex_id = get_celex_by_slug(slug)
        if not celex_id:
            logger.error(f"Regulation slug {slug} not found in registry.")
            return False

        try:
            logger.info(f"Initiating Golden Layer Ingestion for: {slug} (CELEX: {celex_id})")
            
            # Ensure ephemeral buffer exists for THIS regulation
            self.raw_dir.mkdir(parents=True, exist_ok=True)
            
            # 1. Download official XHTML Manifestation
            html_content = self.client.fetch_law_html(celex_id)
            if not html_content:
                return False
            
            # 2. Buffer raw content for Audit (Ephemeral)
            raw_path = self.raw_dir / f"{celex_id}.xhtml"
            with open(raw_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            
            # 3. Create Semantic Chunks
            chunker = SemanticChunker(html_content)
            chunks = chunker.chunk_by_articles()
            
            # 4. Standardized Data Lake output (Staging in .workspace)
            output_data = {
                "metadata": {
                    "slug": slug,
                    "celex": celex_id,
                    "source": "Official Publications Office of the EU (Cellar)",
                    "chunk_count": len(chunks)
                },
                "fragments": chunks
            }
            
            lake_path = self.processed_dir / f"{slug.lower()}_fragments.json"
            with open(lake_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.success(f"Ingestion successful. {len(chunks)} chunks staged at {lake_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Pipeline failure for {slug}: {e}")
            return False
        finally:
            # ATOMIC CLEANUP: Purge audit buffer to maintain zero-residue state
            if self.raw_dir.exists():
                shutil.rmtree(self.raw_dir)
                logger.debug(f"Regulation buffer purged: {self.raw_dir}")

if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Industrial Regulatory Ingestion CLI (Golden Layer)")
    parser.add_argument("--slug", help="Regulatory slug (e.g., AI_ACT). If omitted, syncs full registry.")
    args = parser.parse_args()

    # Dynamic root detection: src/infrastructure_setup/compliance_ingestion/law_processor.py
    # parents[4] points to the repository root: n-cmapss-agentic-factory
    project_root = Path(__file__).resolve().parents[4]
    
    logger.debug(f"Project root identified as: {project_root}")
    processor = LawProcessor(project_root)

    if args.slug:
        success = processor.process_regulation(args.slug)
        sys.exit(0 if success else 1)
    else:
        logger.info("Executing full Regulatory Registry synchronization...")
        all_slugs = list_all_slugs()
        failures = 0
        for s in all_slugs:
            if not processor.process_regulation(s):
                failures += 1
        
        sys.exit(0 if failures == 0 else 1)
