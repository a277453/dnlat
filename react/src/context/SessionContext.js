import React, { createContext, useState, useCallback } from 'react';

export const SessionContext = createContext();

// Valid session IDs are timestamp-based: 14 digits + "e" + employee code
// e.g. "20260515093611e1234"
// UUIDs (xxxxxxxx-xxxx-...) are stale and must be rejected.
const isValidSessionId = (id) => /^\d{14}e\w+$/.test(id);

export function SessionProvider({ children }) {
  const [sessionId, setSessionId] = useState(null);

  const setSession = useCallback((id) => {
    if (!id) return;
    if (!isValidSessionId(id)) {
      console.warn('[SessionContext] Rejected invalid session ID format:', id);
      localStorage.removeItem('sessionId');
      return;
    }
    setSessionId(id);
    localStorage.setItem('sessionId', id);
  }, []);

  const clearSession = useCallback(() => {
    setSessionId(null);
    localStorage.removeItem('sessionId');
  }, []);

  // On mount, restore from localStorage — but only if the format is valid.
  // This purges stale UUIDs left over from older app versions.
  React.useEffect(() => {
    const stored = localStorage.getItem('sessionId');
    if (stored && isValidSessionId(stored)) {
      setSessionId(stored);
    } else if (stored) {
      console.warn('[SessionContext] Cleared stale/invalid session ID from localStorage:', stored);
      localStorage.removeItem('sessionId');
    }
  }, []);

  return (
    <SessionContext.Provider value={{ sessionId, setSession, clearSession }}>
      {children}
    </SessionContext.Provider>
  );
}

export function useSession() {
  const ctx = React.useContext(SessionContext);
  if (!ctx) throw new Error('useSession must be used within SessionProvider');
  return ctx;
}