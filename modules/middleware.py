from fastapi import Request
from time import time

async def timing_middleware(request: Request, call_next):
    """Middleware to time all API requests"""
    start_time = time()
    method = request.method
    url = request.url.path
    
    try:
        response = await call_next(request)
        duration = time() - start_time
        
        # Only log if duration > 0.5s or if it's an error
        if duration > 0.5 or response.status_code >= 400:
            status_icon = "✅" if response.status_code < 400 else "❌"
            print(f"{status_icon} {method} {url} - {duration:.2f}s [{response.status_code}]")
        
        response.headers["X-Process-Time"] = f"{duration:.3f}s"
        return response
        
    except Exception as e:
        duration = time() - start_time
        print(f"❌ {method} {url} - {duration:.2f}s - ERROR: {str(e)[:100]}")
        raise