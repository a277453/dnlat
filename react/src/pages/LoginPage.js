import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import api from '../utils/api';

export default function LoginPage() {
  const { login, theme, toggleTheme } = useAuth();
  const navigate = useNavigate();
  const [form, setForm] = useState({ username: '', password: '' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const handleChange = e => setForm(f => ({ ...f, [e.target.name]: e.target.value }));

  const handleSubmit = async e => {
    e.preventDefault();
    setError(''); setSuccess('');
    if (!form.username.trim()) { setError('Please enter username and password.'); return; }
    setLoading(true);
    try {
      const res = await api.post('/auth/login', form);
      const user = res.data;
      login(user);
      setSuccess(`Welcome, ${user.username}!`);
      setTimeout(() => navigate('/'), 600);
    } catch (err) {
      const status = err.response?.status;
      if (status === 403) setError(`${form.username} is pending admin approval.`);
      else setError('Invalid username or password.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg-main)', display: 'flex', flexDirection: 'column' }}>
      {/* Theme toggle top-right */}
      <div style={{ display: 'flex', justifyContent: 'flex-end', padding: '16px 24px' }}>
        <label className="toggle-wrap" style={{ fontSize: 13, color: 'var(--text-muted)' }}>
          ☀ Light
          <span className="toggle-switch">
            <input type="checkbox" checked={theme === 'light'} onChange={toggleTheme} />
            <span className="toggle-slider" />
          </span>
        </label>
      </div>

      {/* Center card */}
      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px' }}>
        <div className="card fade-in" style={{ width: '100%', maxWidth: 420 }}>
          {/* Logo / title */}
          <div style={{ textAlign: 'center', marginBottom: 28 }}>
            <div style={{ fontSize: 36, marginBottom: 8 }}>⚙️</div>
            <h2>DN Diagnostics Login</h2>
            <p style={{ color: 'var(--text-muted)', fontSize: 13, marginTop: 4 }}>
              Diebold Nixdorf Analysis Platform
            </p>
          </div>

          {error   && <div className="alert alert-error   mb-16">{error}</div>}
          {success && <div className="alert alert-success mb-16">{success}</div>}

          <form onSubmit={handleSubmit}>
            <div className="mb-16">
              <label style={{ display: 'block', marginBottom: 6, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>Username</label>
              <input name="username" value={form.username} onChange={handleChange} placeholder="Enter your username" autoComplete="username" />
            </div>
            <div className="mb-20">
              <label style={{ display: 'block', marginBottom: 6, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>Password</label>
              <input name="password" type="password" value={form.password} onChange={handleChange} placeholder="Enter your password" autoComplete="current-password" />
            </div>
            <button className="btn btn-primary btn-full" type="submit" disabled={loading}>
              {loading ? <><span className="spinner" /> Authenticating…</> : 'Login'}
            </button>
          </form>

          <hr className="divider" />

          <div className="grid-2" style={{ gap: 10 }}>
            <Link className="btn btn-secondary" to="/register" style={{ justifyContent: 'center', textDecoration: 'none' }}>
              Register New User
            </Link>
            <Link className="btn btn-ghost" to="/forgot-password" style={{ justifyContent: 'center', textDecoration: 'none' }}>
              Forgot Password?
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
