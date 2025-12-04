from pathlib import Path
from typing import Dict, List
from .schemas import FileCategorizationResponse, CategoryCount
from modules.logging_config import logger
import logging


logger.info("Starting processing_service")

class ProcessingService:
    """
    Step 4: Process categorization results and prepare response
    """
    
    def prepare_response(
        self, 
        file_categories: Dict[str, List[str]], 
        extract_path: Path
    ) -> FileCategorizationResponse:
        """
        Prepare final response with categorized files.
        """
        logger.info("Preparing final file categorization response.")

        # Calculate totals
        total_files = sum(len(files) for files in file_categories.values())
        logger.debug(f"Total files counted: {total_files}")

        # Create category counts
        category_counts = {
            category: CategoryCount(
                count=len(files),
                files=files
            )
            for category, files in file_categories.items()
        }
        logger.debug(f"Category counts prepared: {list(category_counts.keys())}")

        response = FileCategorizationResponse(
            total_files=total_files,
            extraction_path=str(extract_path),
            categories=category_counts
        )

        logger.info("File categorization response prepared successfully.")
        return response