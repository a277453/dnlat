"""
Session Service - Manages temporary storage of processed file data
In production, replace with Redis or database
"""

from typing import Dict, Any, Optional
from pathlib import Path
from modules.logging_config import logger
import logging


logger.info("Starting session_service")



class SessionService:
    """
    Manages session data for uploaded and processed files
    """
    
    def __init__(self):
        # In-memory storage (use Redis/Database in production)
        self._sessions: Dict[str, Dict[str, Any]] = {}
        logger.info("SessionService initialized") 

    def create_session(self, session_id: str, file_categories: Dict[str, list] = None, extraction_path: Path = None) -> None:
        """
        Create a new session with file categories
        """
        self._sessions[session_id] = {
            'file_categories': file_categories or {},
            'extraction_path': str(extraction_path) if extraction_path else None,
            'selected_type': None,
            'processed_data': {}
        }
        logger.info(f"Session created: {session_id}")  
        logger.debug(f"Session data: {self._sessions[session_id]}")  

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve session data
        """
        session = self._sessions.get(session_id)
        if session:
            logger.debug(f"Session retrieved: {session_id}")  
        else:
            logger.debug(f"Session not found: {session_id}")  
        return session

    def get_session_data(self, session_id: str, key: str) -> Any:
        """
        Get a specific piece of data from a session
        """
        session = self.get_session(session_id)
        if session:
            value = session.get(key)
            logger.debug(f"Retrieved key '{key}' from session {session_id}: {value}")  
            return value
        logger.debug(f"Key '{key}' not found because session {session_id} does not exist")  
        return None

    def update_session(self, session_id: str, key: str = None, value: Any = None, data: Dict = None) -> bool:
        """
        Update specific session data
        """
        if session_id in self._sessions:
            if data:
                self._sessions[session_id].update(data)
                logger.debug(f"Session {session_id} updated with data: {data}")  
            elif key is not None:
                self._sessions[session_id][key] = value
                logger.debug(f"Session {session_id} updated key '{key}' with value: {value}")  
            logger.info(f"Session {session_id} updated successfully")  
            return True
        logger.error(f"Failed to update session {session_id}: session does not exist")  
        return False

    def get_file_categories(self, session_id: str) -> Optional[Dict[str, list]]:
        """
        Get file categories for a session
        """
        session = self.get_session(session_id)
        if session:
            logger.debug(f"File categories retrieved for session {session_id}")  
            return session.get('file_categories')
        logger.debug(f"File categories not found: session {session_id} does not exist")  
        return None

    def set_selected_type(self, session_id: str, file_type: str) -> bool:
        """
        Set the selected file type for a session
        """
        result = self.update_session(session_id, 'selected_type', file_type)
        if result:
            logger.info(f"Selected type '{file_type}' set for session {session_id}")  
        return result

    def get_selected_type(self, session_id: str) -> Optional[str]:
        """
        Get the currently selected file type
        """
        session = self.get_session(session_id)
        if session:
            selected_type = session.get('selected_type')
            logger.debug(f"Selected type retrieved for session {session_id}: {selected_type}")  
            return selected_type
        logger.debug(f"Selected type not found: session {session_id} does not exist") 
        return None

    def delete_session(self, session_id: str) -> bool:
        """
        Delete a session
        """
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.info(f"Session deleted: {session_id}") 
            return True
        logger.error(f"Failed to delete session {session_id}: session does not exist")  
        return False

    def session_exists(self, session_id: str) -> bool:
        """
        Check if session exists
        """
        exists = session_id in self._sessions
        logger.debug(f"Session exists check for {session_id}: {exists}")  
        return exists


# Global session service instance
session_service = SessionService()