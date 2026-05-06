import React, { useState, useEffect } from 'react';
import { Link, useSearchParams, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import api from '../utils/api';

const PWD_RE = /^(?=.*[A-Z])(?=.*[a-z])(?=(?:.*\d){2,})(?=.*[!@#$%^&*()_+\-=[\]{};':"\\|,.<>/?]).{8,}$/;

export default function ResetPasswordPage() {
  const { theme, toggleTheme } = useAuth();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const token = searchParams.get('reset_token');

  const [username, setUsername] = useState('');
  const [tokenValid, setTokenValid] = useState(null); // null=loading, true, false
  const [tokenError, setTokenError] = useState('');
  const [form, setForm] = useState({ new_password: '', confirm_password: '' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  useEffect(() => {
    if (!token) { setTokenValid(false); setTokenError('No reset token found.'); return; }
    api.get('/validate-reset-token', { params: { token } })
      .then(res => { setUsername(res.data.username); setTokenValid(true); })
      .catch(err => { setTokenValid(false); setTokenError(err.response?.data?.detail || 'Invalid or expired token.'); });
  }, [token]);

  const handleChange = e => setForm(f => ({ ...f, [e.target.name]: e.target.value }));

  const handleSubmit = async e => {
    e.preventDefault();
    setError('');
    const { new_password, confirm_password } = form;
    if (!new_password || !confirm_password) { setError('Both fields are required.'); return; }
    if (new_password !== confirm_password) { setError('Passwords do not match.'); return; }
    if (!PWD_RE.test(new_password)) { setError('Password must be 8 chars with uppercase, lowercase, 2+ digits, and special character.'); return; }
    setLoading(true);
    try {
      await api.post('/reset-password', { token, new_password, confirm_password });
      setSuccess('Password reset successful! Redirecting to login…');
      setTimeout(() => navigate('/login'), 2000);
    } catch (err) {
      setError(err.response?.data?.detail || 'Reset failed.');
    } finally { setLoading(false); }
  };

  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg-main)', display: 'flex', flexDirection: 'column' }}>
      <div style={{ display: 'flex', justifyContent: 'flex-end', padding: '16px 24px' }}>
        <label className="toggle-wrap" style={{ fontSize: 13, color: 'var(--text-muted)' }}>
           Light
          <span className="toggle-switch">
            <input type="checkbox" checked={theme === 'light'} onChange={toggleTheme} />
            <span className="toggle-slider" />
          </span>
        </label>
      </div>
      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 }}>
        <div className="card fade-in" style={{ width: '100%', maxWidth: 420 }}>
          <div style={{ textAlign: 'center', marginBottom: 24 }}>
            <div style={{ fontSize: 32, marginBottom: 6 }}></div>
            <h2>Reset Your Password</h2>
          </div>

          {tokenValid === null && <div style={{ textAlign: 'center' }}><span className="spinner spinner-lg" /></div>}

          {tokenValid === false && (
            <>
              <div className="alert alert-error mb-16">{tokenError}</div>
              <div className="grid-2" style={{ gap: 10 }}>
                <Link className="btn btn-secondary" to="/forgot-password" style={{ justifyContent: 'center', textDecoration: 'none' }}>Request New Reset</Link>
                <Link className="btn btn-ghost" to="/login" style={{ justifyContent: 'center', textDecoration: 'none' }}>Back to Login</Link>
              </div>
            </>
          )}

          {tokenValid === true && (
            <>
              <p style={{ textAlign: 'center', color: 'var(--text-muted)', fontSize: 13, marginBottom: 20 }}>
                Setting new password for <strong style={{ color: 'var(--text-primary)' }}>{username}</strong>
              </p>

              {error   && <div className="alert alert-error   mb-16">{error}</div>}
              {success && <div className="alert alert-success mb-16">{success}</div>}

              <form onSubmit={handleSubmit}>
                <div className="mb-12">
                  <label style={{ display: 'block', marginBottom: 5, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>New Password</label>
                  <input name="new_password" type="password" value={form.new_password} onChange={handleChange} placeholder="Enter your new password" />
                </div>
                <div className="mb-20">
                  <label style={{ display: 'block', marginBottom: 5, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>Confirm New Password</label>
                  <input name="confirm_password" type="password" value={form.confirm_password} onChange={handleChange} placeholder="Re-enter your new password" />
                </div>
                <button className="btn btn-primary btn-full" type="submit" disabled={loading}>
                  {loading ? <><span className="spinner" /> Updating…</> : 'Reset Password'}
                </button>
              </form>
              <hr className="divider" />
              <Link className="btn btn-ghost btn-full" to="/login" style={{ justifyContent: 'center', textDecoration: 'none' }}> Back to Login</Link>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
