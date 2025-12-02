"""
Session Service - Manages temporary storage of processed file data
In production, replace with Redis or database
"""

from typing import Dict, Any, Optional
from pathlib import Path

class SessionService:
    """
    Manages session data for uploaded and processed files
    """
    
    def __init__(self):
        # In-memory storage (use Redis/Database in production)
        self._sessions: Dict[str, Dict[str, Any]] = {}
    
    def create_session(self, session_id: str, file_categories: Dict[str, list], extraction_path: Path) -> None:
        """
        Create a new session with file categories
        
        Args:
            session_id: Unique session identifier
            file_categories: Dictionary of categorized files
            extraction_path: Path to extracted files
        """
        self._sessions[session_id] = {
            'file_categories': file_categories,
            'extraction_path': str(extraction_path),
            'selected_type': None,
            'processed_data': {}
        }
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve session data
        
        Args:
            session_id: Session identifier
            
        Returns:
            Session data or None if not found
        """
        return self._sessions.get(session_id)
    
    def update_session(self, session_id: str, key: str, value: Any) -> bool:
        """
        Update specific session data
        
        Args:
            session_id: Session identifier
            key: Data key to update
            value: New value
            
        Returns:
            True if successful, False if session not found
        """
        if session_id in self._sessions:
            self._sessions[session_id][key] = value
            return True
        return False
    
    def get_file_categories(self, session_id: str) -> Optional[Dict[str, list]]:
        """
        Get file categories for a session
        
        Args:
            session_id: Session identifier
            
        Returns:
            File categories dictionary or None
        """
        session = self.get_session(session_id)
        return session['file_categories'] if session else None
    
    def set_selected_type(self, session_id: str, file_type: str) -> bool:
        """
        Set the selected file type for a session
        
        Args:
            session_id: Session identifier
            file_type: Selected file type
            
        Returns:
            True if successful
        """
        return self.update_session(session_id, 'selected_type', file_type)
    
    def get_selected_type(self, session_id: str) -> Optional[str]:
        """
        Get the currently selected file type
        
        Args:
            session_id: Session identifier
            
        Returns:
            Selected file type or None
        """
        session = self.get_session(session_id)
        return session['selected_type'] if session else None
    
    def delete_session(self, session_id: str) -> bool:
        """
        Delete a session
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if deleted, False if not found
        """
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False
    
    def session_exists(self, session_id: str) -> bool:
        """
        Check if session exists
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if exists
        """
        return session_id in self._sessions


# Global session service instance
session_service = SessionService()