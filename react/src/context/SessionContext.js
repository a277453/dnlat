import React, { createContext, useState, useCallback } from 'react';

export const SessionContext = createContext();

export function SessionProvider({ children }) {
  const [sessionId, setSessionId] = useState(null);

  const setSession = useCallback((id) => {
    setSessionId(id);
    if (id) localStorage.setItem('sessionId', id);
  }, []);

  const clearSession = useCallback(() => {
    setSessionId(null);
    localStorage.removeItem('sessionId');
  }, []);

  // On mount, restore from localStorage
  React.useEffect(() => {
    const stored = localStorage.getItem('sessionId');
    if (stored) setSessionId(stored);
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
