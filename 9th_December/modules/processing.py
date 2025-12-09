from pathlib import Path
from typing import Dict, List
from .schemas import FileCategorizationResponse, CategoryCount


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
        
        Args:
            file_categories: Dictionary of categorized files
            extract_path: Path where files were extracted
            
        Returns:
            FileCategorizationResponse object
        """
        # Calculate totals
        total_files = sum(len(files) for files in file_categories.values())
        
        # Create category counts
        category_counts = {
            category: CategoryCount(
                count=len(files),
                files=files
            )
            for category, files in file_categories.items()
        }
        
        return FileCategorizationResponse(
            total_files=total_files,
            extraction_path=str(extract_path),
            categories=category_counts
        )