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
    CLASS: SessionService

DESCRIPTION:
    Manages lifecycle and data storage for sessions created during file
    processing. Supports creation, retrieval, updating, deletion, and
    checking of session existence.

USAGE:
    service = SessionService()
    service.create_session("123", file_categories={...})
    data = service.get_session("123")

    """
    
    def __init__(self):
        """
    FUNCTION: __init__

    DESCRIPTION:
        Initializes the in-memory session dictionary used to store all
        session-related data.

    USAGE:
        service = SessionService()

    PARAMETERS:
        None

    RETURNS:
        None

    RAISES:
        None
        """
        # In-memory storage (use Redis/Database in production)
        self._sessions: Dict[str, Dict[str, Any]] = {}
        logger.info("SessionService initialized") 

    def create_session(self, session_id: str, file_categories: Dict[str, list] = None, extraction_path: Path = None) -> None:
        """
        FUNCTION: create_session

DESCRIPTION:
    Creates a new session with file categories, extraction path,
    and initializes selected type and processed data.

USAGE:
    service.create_session("abc", file_categories, extraction_path)

PARAMETERS:
    session_id (str)         : Unique session identifier.
    file_categories (dict)   : Categories and associated file lists.
    extraction_path (Path?)  : Path where files were extracted.

RETURNS:
    None

RAISES:
    None
        """
        self._sessions[session_id] = {
            'file_categories': file_categories,
            'extraction_path': str(extraction_path),
            'selected_type': None,
            'processed_data': {}
        }
        logger.info(f"Session created: {session_id}")  
        logger.debug(f"Session data: {self._sessions[session_id]}")  

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        FUNCTION: get_session

DESCRIPTION:
    Retrieves complete session data for a given session ID.

USAGE:
    session = service.get_session("abc")

PARAMETERS:
    session_id (str) : Session identifier.

RETURNS:
    dict | None : Session data if found, otherwise None.

RAISES:
    None
        """
        session = self._sessions.get(session_id)
        if session:
            logger.debug(f"Session retrieved: {session_id}")  
        else:
            logger.debug(f"Session not found: {session_id}")  
        return session

    def get_session_data(self, session_id: str, key: str) -> Any:
        """
        FUNCTION: get_session_data

DESCRIPTION:
    Retrieves a specific key/value stored in a session.

USAGE:
    value = service.get_session_data("abc", "file_categories")

PARAMETERS:
    session_id (str) : Session identifier.
    key (str)        : Key to fetch from session data.

RETURNS:
    Any | None : Value of the key, or None if session/key doesn't exist.

RAISES:
    None
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
        FUNCTION: update_session

        DESCRIPTION:
            Updates session data. Supports updating a single key/value pair or
            merging an entire dictionary of updates.

        USAGE:
            service.update_session("abc", key="selected_type", value="ui_journals")
            OR
            service.update_session("abc", data={"processed_data": {...}})

        PARAMETERS:
            session_id (str) : Session identifier.
            key (str?)       : A single key to update.
            value (Any?)     : Value to set for the key.
            data (dict?)     : Dictionary of multiple keys/values to update.

        RETURNS:
            bool : True if updated, False if session does not exist.

        RAISES:
            None
        """
        if session_id in self._sessions:
            if data:
                self._sessions[session_id].update(data)
                logger.debug(f"Session {session_id} updated with data: {data}")  
            elif key is not None:
                self._sessions[session_id][key] = value
                #logger.debug(f"Session {session_id} updated key '{key}' with value: {value}")  
            logger.info(f"Session {session_id} updated successfully with {key}")  
            return True
        logger.error(f"Failed to update session {session_id}: session does not exist")  
        return False

    def get_file_categories(self, session_id: str) -> Optional[Dict[str, list]]:
        """
        FUNCTION: get_file_categories

DESCRIPTION:
    Returns file categories stored in a session.

USAGE:
    categories = service.get_file_categories("abc")

PARAMETERS:
    session_id (str) : Session identifier.

RETURNS:
    dict | None : File categories or None if session does not exist.

RAISES:
    None
        """
        session = self.get_session(session_id)
        if session:
            logger.debug(f"File categories retrieved for session {session_id}")  
            return session.get('file_categories')
        logger.debug(f"File categories not found: session {session_id} does not exist")  
        return None

    def set_selected_type(self, session_id: str, file_type: str) -> bool:
        """
        FUNCTION: set_selected_type

DESCRIPTION:
    Updates the selected file type for the session.

USAGE:
    service.set_selected_type("abc", "customer_journals")

PARAMETERS:
    session_id (str) : Session identifier.
    file_type (str)  : Selected file type value.

RETURNS:
    bool : True if updated, False otherwise.

RAISES:
    None
        """
        result = self.update_session(session_id, 'selected_type', file_type)
        if result:
            logger.info(f"Selected type '{file_type}' set for session {session_id}")  
        return result

    def get_selected_type(self, session_id: str) -> Optional[str]:
        """
        FUNCTION: get_selected_type

DESCRIPTION:
    Retrieves the selected file type for a session.

USAGE:
    t = service.get_selected_type("abc")

PARAMETERS:
    session_id (str) : Session identifier.

RETURNS:
    str | None : Selected type or None if not set or session missing.

RAISES:
    None
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
        FUNCTION: delete_session

    DESCRIPTION:
        Deletes the entire session from storage.

    USAGE:
        service.delete_session("abc")

    PARAMETERS:
        session_id (str) : Session identifier.

    RETURNS:
        bool : True if deleted, False otherwise.

    RAISES:
        None
        """
        if session_id in self._sessions:
            del self._sessions[session_id]
            logger.info(f"Session deleted: {session_id}") 
            return True
        logger.error(f"Failed to delete session {session_id}: session does not exist")  
        return False

    def session_exists(self, session_id: str) -> bool:
        """
        FUNCTION: session_exists

    DESCRIPTION:
        Checks whether a session with the given ID exists.

    USAGE:
        exists = service.session_exists("abc")

    PARAMETERS:
        session_id (str) : Session identifier.

    RETURNS:
        bool : True if session exists, otherwise False.

    RAISES:
        None
        """
        exists = session_id in self._sessions
        logger.debug(f"Session exists check for {session_id}: {exists}") 
        logger.info(f"hey saniya any session found")
        return exists

"""
    GLOBAL:
        session_service : Shared instance of SessionService.
    """
session_service = SessionService()