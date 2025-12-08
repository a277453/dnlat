from pathlib import Path
from typing import Dict, List
from .schemas import FileCategorizationResponse, CategoryCount
from modules.logging_config import logger
import logging


logger.info("Starting processing_service")

class ProcessingService:
    """
    FUNCTION:
        ProcessingService

    DESCRIPTION:
        Handles the processing of categorized file data and prepares
        the final structured API response for the caller.

    USAGE:
        service = ProcessingService()
        result = service.prepare_response(file_categories, extract_path)

    PARAMETERS:
        None

    RETURNS:
        None

    RAISES:
        None
    """
    
    def prepare_response(
        self, 
        file_categories: Dict[str, List[str]], 
        extract_path: Path
    ) -> FileCategorizationResponse:
        """
        FUNCTION:
            prepare_response

        DESCRIPTION:
            Processes categorized file data, calculates totals, converts them
            into CategoryCount objects, and returns a standardized response model.

        USAGE:
            result = self.prepare_response(file_categories, extract_path)

        PARAMETERS:
            file_categories (Dict[str, List[str]]) :
                Dictionary where key = category name,
                value = list of associated file names.
            extract_path (Path) :
                Path where files were extracted.

        RETURNS:
            FileCategorizationResponse :
                Includes total files, extraction path, and categorized file details.

        RAISES:
            None
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