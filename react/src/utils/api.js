import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000/api/v1';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 300000,
});

// Response interceptor — handle 401 globally
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      sessionStorage.removeItem('dn_user');
      delete api.defaults.headers.common['Authorization'];
      window.dispatchEvent(new CustomEvent('dn:session-expired'));
    }
    return Promise.reject(error);
  }
);

export default api;
