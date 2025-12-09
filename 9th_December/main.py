from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from modules.middleware import timing_middleware  # NEW LINE
from api import routes
from pathlib import Path

app = FastAPI(
    title="DN Diagnostics and Analysis Platform API",
    description="API for processing and analyzing Diebold Nixdorf (DN) log files, transaction journals, and registry data.",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Timing middleware - NEW LINES
app.middleware("http")(timing_middleware)

# Global variable to store the processed files directory
PROCESSED_FILES_DIR = None

def set_processed_files_dir(directory: str):
    """
    Set the directory where processed files are stored.
    This should be called from the ZIP processing endpoint.
    """
    global PROCESSED_FILES_DIR
    PROCESSED_FILES_DIR = directory
    print(f"✓ Processed files directory set to: {directory}")

def get_processed_files_dir() -> str:
    """Get the current processed files directory"""
    return PROCESSED_FILES_DIR

# Include existing routers
app.include_router(routes.router, prefix="/api/v1", tags=["analysis-engine"])

# ============================================
# REGISTRY FILE ENDPOINTS
# ============================================

@app.get("/api/v1/list-registry-files")
async def list_registry_files():
    """
    List all registry files found in the processed package.
    
    Returns:
        dict: Dictionary containing list of registry file paths and count
        
    Raises:
        HTTPException: If no files have been processed yet or directory not found
    """
    if not PROCESSED_FILES_DIR:
        raise HTTPException(
            status_code=404, 
            detail="No files have been processed yet. Please upload a ZIP file first."
        )
    
    processed_path = Path(PROCESSED_FILES_DIR)
    
    if not processed_path.exists():
        raise HTTPException(
            status_code=404, 
            detail="Processed files directory not found. Please re-upload your ZIP file."
        )
    
    # Look for registry files in the registry_files subdirectory
    registry_dir = processed_path / "registry_files"
    
    if not registry_dir.exists():
        return {
            "files": [], 
            "count": 0,
            "message": "No registry files directory found in the processed package"
        }
    
    # Find all registry files with common extensions
    registry_extensions = ['.reg', '.ini', '.txt', '.cfg', '.conf']
    registry_files = []
    
    for ext in registry_extensions:
        found_files = list(registry_dir.rglob(f'*{ext}'))
        for f in found_files:
            # Store path relative to processed_path for easier retrieval
            relative_path = str(f.relative_to(processed_path))
            registry_files.append(relative_path)
    
    if not registry_files:
        return {
            "files": [], 
            "count": 0,
            "message": "No registry files found with supported extensions (.reg, .ini, .txt, .cfg, .conf)"
        }
    
    return {
        "files": sorted(registry_files),
        "count": len(registry_files),
        "message": f"Found {len(registry_files)} registry file(s)"
    }


@app.get("/api/v1/get-registry-file")
async def get_registry_file(file_path: str):
    """
    Get the contents of a specific registry file.
    
    Args:
        file_path: Relative path to the registry file (e.g., "registry_files/config.ini")
        
    Returns:
        dict: Dictionary containing file content, encoding, size, and name
        
    Raises:
        HTTPException: If file not found, access denied, or unable to read
    """
    if not PROCESSED_FILES_DIR:
        raise HTTPException(
            status_code=404, 
            detail="No files have been processed yet. Please upload a ZIP file first."
        )
    
    processed_path = Path(PROCESSED_FILES_DIR)
    full_path = processed_path / file_path
    
    # Security check: ensure the path is within the processed directory
    # This prevents path traversal attacks (e.g., ../../etc/passwd)
    try:
        full_path = full_path.resolve()
        processed_path = processed_path.resolve()
        
        if not str(full_path).startswith(str(processed_path)):
            raise HTTPException(
                status_code=403, 
                detail="Access denied: Path traversal detected"
            )
    except Exception as e:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid file path: {str(e)}"
        )
    
    # Check if file exists
    if not full_path.exists():
        raise HTTPException(
            status_code=404, 
            detail=f"File not found: {file_path}"
        )
    
    # Check if it's actually a file (not a directory)
    if not full_path.is_file():
        raise HTTPException(
            status_code=400, 
            detail="The specified path is not a file"
        )
    
    # Try to read the file with multiple encoding attempts
    try:
        encodings = ['utf-8', 'utf-16', 'latin-1', 'cp1252', 'iso-8859-1']
        content = None
        encoding_used = None
        
        for encoding in encodings:
            try:
                with open(full_path, 'r', encoding=encoding) as f:
                    content = f.read()
                    encoding_used = encoding
                    break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            raise HTTPException(
                status_code=500, 
                detail="Unable to decode file. File may be binary or use an unsupported encoding."
            )
        
        return {
            "file_path": file_path,
            "content": content,
            "encoding": encoding_used,
            "size": full_path.stat().st_size,
            "name": full_path.name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error reading file: {str(e)}"
        )


# ============================================
# ROOT AND HEALTH CHECK ENDPOINTS
# ============================================

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "DN Diagnostics and Analysis Platform API",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "api": "/api/v1"
        }
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify API is running.
    Also indicates if files have been processed.
    """
    return {
        "status": "healthy",
        "version": "1.0.0",
        "processed_files_loaded": PROCESSED_FILES_DIR is not None,
        "processed_dir": PROCESSED_FILES_DIR if PROCESSED_FILES_DIR else "Not set"
    }


# ============================================
# STARTUP EVENT
# ============================================

@app.on_event("startup")
async def startup_event():
    """Actions to perform on application startup"""
    print("=" * 60)
    print("DN Diagnostics Platform API v1.0.0 - Ready")
    print("=" * 60)


# ============================================
# SHUTDOWN EVENT
# ============================================

@app.on_event("shutdown")
async def shutdown_event():
    """Actions to perform on application shutdown"""
    print("Shutting down...")


# Export the set_processed_files_dir function so routes.py can use it
__all__ = ['app', 'set_processed_files_dir', 'get_processed_files_dir']