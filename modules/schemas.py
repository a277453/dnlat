from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from enum import Enum

class CategoryCount(BaseModel):
    """
    CLASS: CategoryCount

    DESCRIPTION:
    Represents the count of files within a specific category along with
    the list of file paths.

    USAGE:
    obj = CategoryCount(count=5, files=["a.log", "b.log"])

    PARAMETERS:
    count (int)       : Number of files in this category.
    files (list[str]) : List of file paths.

    RETURNS:
    CategoryCount instance

    RAISES:
    ValidationError : If fields do not match expected types.
    """
    count: int = Field(..., description="Number of files in this category")
    files: List[str] = Field(..., description="List of file paths")

class FileCategorizationResponse(BaseModel):
    """
    CLASS: FileCategorizationResponse

DESCRIPTION:
    Response model returned after file categorization. Includes total
    files processed, extraction directory, categorized file details, and
    optional ACU extraction logs.

USAGE:
    resp = FileCategorizationResponse(
               total_files=10,
               extraction_path="temp",
               categories={...}
           )

PARAMETERS:
    total_files (int)                       : Total number of files processed.
    extraction_path (str)                   : Directory where files were extracted.
    categories (dict[str, CategoryCount])   : Category-wise file details.
    acu_extraction_logs (list[str])         : ACU extraction log messages.

RETURNS:
    FileCategorizationResponse instance

RAISES:
    ValidationError : When invalid types are provided.
    """
    total_files: int = Field(..., description="Total number of files processed")
    extraction_path: str = Field(..., description="Path where files were extracted")
    categories: Dict[str, CategoryCount] = Field(
        ...,
        description="Categorized files by type"
    )
    acu_extraction_logs: List[str] = Field(default_factory=list, description="Detailed logs from ACU file extraction")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_files": 10,
                "extraction_path": "temp_extracted_files",
                "categories": {
                    "customer_journals": {
                        "count": 2,
                        "files": ["temp_extracted_files/journal1.log"]
                    },
                    "ui_journals": {
                        "count": 1,
                        "files": ["temp_extracted_files/ui.log"]
                    },
                    "acu_extraction_logs": ["Log entry 1", "Log entry 2"]
                }
            }
        }


# NEW: Enum for file types
class FileTypeEnum(str, Enum):
    """
    CLASS: FileTypeEnum

    DESCRIPTION:
    Enumeration of supported file type categories.

    USAGE:
    FileTypeEnum.CUSTOMER_JOURNALS

    VALUES:
    CUSTOMER_JOURNALS
    UI_JOURNALS
    TRC_TRACE
    TRC_ERROR
    REGISTRY_FILES
    """
    CUSTOMER_JOURNALS = "customer_journals"
    UI_JOURNALS = "ui_journals"
    TRC_TRACE = "trc_trace"
    TRC_ERROR = "trc_error"
    REGISTRY_FILES = "registry_files"


# NEW: Response for available file types
class AvailableFileTypesResponse(BaseModel):
    """
    
    CLASS: AvailableFileTypesResponse

    DESCRIPTION:
    Response model listing available file types and their details.

U   SAGE:
    resp = AvailableFileTypesResponse(
              available_types=[...],
              type_details={...}
           )

    PARAMETERS:
    available_types (list[str])               : List of file types found.
    type_details (dict[str, CategoryCount])   : Count and file list per type.

    RETURNS:
    AvailableFileTypesResponse instance

    RAISES:
    ValidationError : For invalid input formats.
    """
    available_types: List[str] = Field(..., description="List of available file types")
    type_details: Dict[str, CategoryCount] = Field(..., description="Details for each type")
    
    class Config:
        json_schema_extra = {
            "example": {
                "available_types": ["customer_journals", "ui_journals"],
                "type_details": {
                    "customer_journals": {
                        "count": 2,
                        "files": ["file1.jrn", "file2.jrn"]
                    },
                    "ui_journals": {
                        "count": 1,
                        "files": ["ui.jrn"]
                    }
                }
            }
        }


# NEW: Request for selecting file type(s) and operation
class FileTypeSelectionRequest(BaseModel):
    """
    CLASS: FileTypeSelectionRequest

    DESCRIPTION:
    Request model for selecting one or more file types for further
    operations.

    USAGE:
    req = FileTypeSelectionRequest(
              file_types=[FileTypeEnum.CUSTOMER_JOURNALS]
          )

    PARAMETERS:
    file_types (list[FileTypeEnum]) : The selected file types.

    RETURNS:
    FileTypeSelectionRequest instance

    RAISES:
    ValidationError
    """
    file_types: List[FileTypeEnum] = Field(..., description="Selected file type(s)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "file_types": ["customer_journals", "ui_journals"]
            }
        }


# NEW: Details for a single file type
class FileTypeDetail(BaseModel):
    """
    CLASS: FileTypeDetail

DESCRIPTION:
    Represents metadata for a single file type, including its file count,
    file list, and operations supported for that type.

USAGE:
    d = FileTypeDetail(file_count=3, files=[...], available_operations=[...])

PARAMETERS:
    file_count (int)                : Number of files.
    files (list[str])               : List of file paths.
    available_operations (list[str]): Available operations for this type.

RETURNS:
    FileTypeDetail instance

RAISES:
    ValidationError
    """
    file_count: int = Field(..., description="Number of files of this type")
    files: List[str] = Field(..., description="List of files")
    available_operations: List[str] = Field(..., description="Operations available for this file type")


# NEW: Response for file type selection (supports multiple types)
class FileTypeSelectionResponse(BaseModel):
    """
    CLASS: FileTypeSelectionResponse

DESCRIPTION:
    Response model for selection of one or more file types. Provides
    details of selected types and combined operations possible.

USAGE:
    resp = FileTypeSelectionResponse(
              selected_types=[...],
              type_details={...},
              combined_operations=[...]
           )

PARAMETERS:
    selected_types (list[str])                   : File types chosen.
    type_details (dict[str, FileTypeDetail])     : Details per selected type.
    combined_operations (list[str])              : Operations supported together.

RETURNS:
    FileTypeSelectionResponse instance

RAISES:
    ValidationError
    """
    selected_types: List[str] = Field(..., description="The selected file types")
    type_details: Dict[str, FileTypeDetail] = Field(..., description="Details for each selected type")
    combined_operations: List[str] = Field(..., description="Operations available when combining these types")
    
    class Config:
        json_schema_extra = {
            "example": {
                "selected_types": ["customer_journals", "ui_journals"],
                "type_details": {
                    "customer_journals": {
                        "file_count": 2,
                        "files": ["file1.jrn", "file2.jrn"],
                        "available_operations": ["parse", "analyze_transactions"]
                    },
                    "ui_journals": {
                        "file_count": 1,
                        "files": ["ui.jrn"],
                        "available_operations": ["parse_ui_events"]
                    }
                },
                "combined_operations": ["map_transactions_to_ui", "generate_combined_report"]
            }
        }


# NEW: Request for visualizing a single transaction
class TransactionVisualizationRequest(BaseModel):
    """
    CLASS: TransactionVisualizationRequest

DESCRIPTION:
    Request model for visualizing an individual transaction.

USAGE:
    req = TransactionVisualizationRequest(transaction_id="T123")

PARAMETERS:
    transaction_id (str) : ID of the transaction.

RETURNS:
    TransactionVisualizationRequest instance

RAISES:
    ValidationError
    """
    transaction_id: str = Field(..., description="The ID of the transaction to visualize")
class ParseFilesRequest(BaseModel):
    """
    CLASS: ParseFilesRequest

DESCRIPTION:
    Request model containing ACU file data to be parsed.

USAGE:
    req = ParseFilesRequest(filename="test.acu", xml_content="...")

PARAMETERS:
    filename (str)       : Name of the ACU file.
    xml_content (str)    : XML content extracted from the file.
    xsd_content (str?)   : Optional XSD schema content.

RETURNS:
    ParseFilesRequest instance

RAISES:
    ValidationError
    """
    filename: str
    xml_content: str
    xsd_content: Optional[str] = None
class PathRequest(BaseModel):
    """
    CLASS: PathRequest

DESCRIPTION:
    Request model containing directory path and allowed prefixes for ACU
    file discovery.

USAGE:
    req = PathRequest(path="logs/")

PARAMETERS:
    path (str)          : Directory path to search.
    prefixes (list[str]): Filename prefixes used for matching.

RETURNS:
    PathRequest instance

RAISES:
    ValidationError
    """
    path: str = Field(..., description="The base directory path to search for ACU files.")
    prefixes: List[str] = Field(['jdd', 'x3', 'agilis_acuexport_', 'acuexport_', 'agilis_', 'acu_'], description="List of prefixes to search for in filenames.") # Default prefixes
