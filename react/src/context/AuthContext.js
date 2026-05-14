import React, { createContext, useContext, useState, useCallback } from 'react';
import api from '../utils/api';

const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(() => {
    try {
      const stored = sessionStorage.getItem('dn_user');
      return stored ? JSON.parse(stored) : null;
    } catch { return null; }
  });
  const [theme, setTheme] = useState(() => localStorage.getItem('dn_theme') || 'dark');

  const login = useCallback((userData) => {
    setUser(userData);
    sessionStorage.setItem('dn_user', JSON.stringify(userData));
    api.defaults.headers.common['Authorization'] = `Bearer ${userData.session_token}`;
  }, []);

  const logout = useCallback(async () => {
    try { await api.post('/auth/logout'); } catch {}
    setUser(null);
    sessionStorage.removeItem('dn_user');
    delete api.defaults.headers.common['Authorization'];
  }, []);

  const toggleTheme = useCallback(() => {
    setTheme(t => {
      const next = t === 'dark' ? 'light' : 'dark';
      localStorage.setItem('dn_theme', next);
      return next;
    });
  }, []);

  // Restore token on mount
  React.useEffect(() => {
    if (user?.session_token) {
      api.defaults.headers.common['Authorization'] = `Bearer ${user.session_token}`;
    }
  }, [user]);

  return (
    <AuthContext.Provider value={{ user, login, logout, theme, toggleTheme }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used inside AuthProvider');
  return ctx;
};
