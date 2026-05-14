import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000/api/v1';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 300000,
});

// Request interceptor — inject session_id if available
api.interceptors.request.use(
  (config) => {
    const sessionId = localStorage.getItem('sessionId');
    if (sessionId) {
      if (!config.params) config.params = {};
      config.params.session_id = sessionId;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor — handle 401 globally
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      sessionStorage.removeItem('dn_user');
      localStorage.removeItem('sessionId');
      delete api.defaults.headers.common['Authorization'];
      window.dispatchEvent(new CustomEvent('dn:session-expired'));
    }
    return Promise.reject(error);
  }
);

export default api;
