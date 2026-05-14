import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import api from '../utils/api';

const EMAIL_RE = /^[a-zA-Z]+\.[a-zA-Z]+@dieboldnixdorf\.com$/;
const NAME_RE  = /^[A-Za-z ]+$/;
const PWD_RE   = /^(?=.*[A-Z])(?=.*[a-z])(?=(?:.*\d){2,})(?=.*[!@#$%^&*()_+\-=[\]{};':"\\|,.<>/?]).{8,}$/;

function isInvalidEmpCode(code) {
  if (code.length !== 8 || !/^\d{8}$/.test(code)) return true;
  if (new Set(code).size === 1) return true;
  if ('01234567890123456789'.includes(code)) return true;
  if ('98765432109876543210'.includes(code)) return true;
  const pairs = [code.slice(0,2), code.slice(2,4), code.slice(4,6), code.slice(6,8)];
  if (pairs.every(p => p[0] === p[1])) return true;
  return false;
}

export default function RegisterPage() {
  const { theme, toggleTheme } = useAuth();
  const navigate = useNavigate();
  const [form, setForm] = useState({ email:'', name:'', password:'', confirm_password:'', employee_code:'' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const handleChange = e => setForm(f => ({ ...f, [e.target.name]: e.target.value }));

  const validate = () => {
    const { email, name, password, confirm_password, employee_code } = form;
    if (!email || !name || !password || !confirm_password || !employee_code)
      return 'All fields are required.';
    if (!EMAIL_RE.test(email.trim().toLowerCase()))
      return 'Please use your official Diebold Nixdorf email ID.';
    if (!NAME_RE.test(name.trim()))
      return 'Name must contain only letters and spaces.';
    if (!PWD_RE.test(password))
      return 'Password must be 8 chars with uppercase, lowercase, 2+ digits, and a special character.';
    if (password !== confirm_password)
      return 'Passwords do not match.';
    if (isInvalidEmpCode(employee_code.trim()))
      return 'Please enter a valid 8-digit employee code.';
    return null;
  };

  const handleSubmit = async e => {
    e.preventDefault();
    setError(''); setSuccess('');
    const err = validate();
    if (err) { setError(err); return; }
    setLoading(true);
    try {
      await api.post('/auth/register', {
        email: form.email.trim().toLowerCase(),
        name: form.name.trim(),
        password: form.password,
        employee_code: form.employee_code.trim(),
        role: 'USER',
      });
      setSuccess('Registration successful! Awaiting admin activation.');
      setTimeout(() => navigate('/login'), 2500);
    } catch (err) {
      setError(err.response?.data?.detail || 'Registration failed. Please try again.');
    } finally {
      setLoading(false);
    }
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

      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px' }}>
        <div className="card fade-in" style={{ width: '100%', maxWidth: 460 }}>
          <div style={{ textAlign: 'center', marginBottom: 24 }}>
            <div style={{ fontSize: 32, marginBottom: 6 }}></div>
            <h2>DN Diagnostics Register</h2>
          </div>

          {error   && <div className="alert alert-error   mb-16">{error}</div>}
          {success && <div className="alert alert-success mb-16">{success}</div>}

          <form onSubmit={handleSubmit}>
            {[
              { label: 'Email ID', name: 'email', type: 'email', placeholder: 'Enter DN Official Email' },
              { label: 'Name', name: 'name', type: 'text', placeholder: 'Enter your name' },
              { label: 'Password', name: 'password', type: 'password', placeholder: 'Enter your password' },
              { label: 'Confirm Password', name: 'confirm_password', type: 'password', placeholder: 'Re-enter your password' },
              { label: 'Employee Code', name: 'employee_code', type: 'text', placeholder: 'Enter 8-digit employee code', maxLength: 8 },
            ].map(f => (
              <div key={f.name} className="mb-12">
                <label style={{ display: 'block', marginBottom: 5, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>{f.label}</label>
                <input {...f} value={form[f.name]} onChange={handleChange} />
              </div>
            ))}
            <div className="mb-12">
              <label style={{ display: 'block', marginBottom: 5, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>Role Type</label>
              <input value="USER" disabled style={{ opacity: 0.5 }} readOnly />
            </div>
            <button className="btn btn-primary btn-full mt-8" type="submit" disabled={loading}>
              {loading ? <><span className="spinner" /> Registering…</> : 'Register'}
            </button>
          </form>

          <hr className="divider" />
          <Link className="btn btn-ghost btn-full" to="/login" style={{ justifyContent: 'center', textDecoration: 'none' }}>
             Back to Login
          </Link>
        </div>
      </div>
    </div>
  );
}
