import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import api from '../utils/api';

export function ForgotPasswordPage() {
  const { theme, toggleTheme } = useAuth();
  const [form, setForm] = useState({ username: '', employee_code: '' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const handleChange = e => setForm(f => ({ ...f, [e.target.name]: e.target.value }));

  const handleSubmit = async e => {
    e.preventDefault();
    setError(''); setSuccess('');
    if (!form.username.trim() || !form.employee_code.trim()) {
      setError('Please enter both Username and Employee Code.'); return;
    }
    if (!/^\d{8}$/.test(form.employee_code.trim())) {
      setError('Employee Code must be exactly 8 digits.'); return;
    }
    setLoading(true);
    try {
      const baseUrl = `${window.location.protocol}//${window.location.host}`;
      await api.post('/forgot-password', {
        username: form.username.trim(),
        employee_code: form.employee_code.trim(),
        base_url: baseUrl,
      });
      setSuccess('A password reset link has been sent to your email. Please check your inbox within 30 minutes.');
    } catch (err) {
      setError(err.response?.data?.detail || 'Unable to process request.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg-main)', display: 'flex', flexDirection: 'column' }}>
      <div style={{ display: 'flex', justifyContent: 'flex-end', padding: '16px 24px' }}>
        <label className="toggle-wrap" style={{ fontSize: 13, color: 'var(--text-muted)' }}>
          ☀ Light
          <span className="toggle-switch">
            <input type="checkbox" checked={theme === 'light'} onChange={toggleTheme} />
            <span className="toggle-slider" />
          </span>
        </label>
      </div>
      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 }}>
        <div className="card fade-in" style={{ width: '100%', maxWidth: 420 }}>
          <div style={{ textAlign: 'center', marginBottom: 24 }}>
            <div style={{ fontSize: 32, marginBottom: 6 }}>🔑</div>
            <h2>Forgot Password</h2>
            <p style={{ color: 'var(--text-muted)', fontSize: 13, marginTop: 4 }}>
              Enter your username and employee code to verify your identity.
            </p>
          </div>

          {error   && <div className="alert alert-error   mb-16">{error}</div>}
          {success && <div className="alert alert-success mb-16">{success}</div>}

          <form onSubmit={handleSubmit}>
            <div className="mb-12">
              <label style={{ display: 'block', marginBottom: 5, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>Username</label>
              <input name="username" value={form.username} onChange={handleChange} placeholder="Enter your registered username" />
            </div>
            <div className="mb-20">
              <label style={{ display: 'block', marginBottom: 5, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>Employee Code</label>
              <input name="employee_code" value={form.employee_code} onChange={handleChange} placeholder="Enter your 8-digit employee code" maxLength={8} />
            </div>
            <button className="btn btn-primary btn-full" type="submit" disabled={loading}>
              {loading ? <><span className="spinner" /> Verifying…</> : 'Verify Identity'}
            </button>
          </form>

          <hr className="divider" />
          <Link className="btn btn-ghost btn-full" to="/login" style={{ justifyContent: 'center', textDecoration: 'none' }}>
            ← Back to Login
          </Link>
        </div>
      </div>
    </div>
  );
}

export default ForgotPasswordPage;
