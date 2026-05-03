import React from 'react';
import { useAuth } from '../context/AuthContext';

export default function Sidebar({ selected, onSelect, functionalities, availableTypes }) {
  const { user, logout, theme, toggleTheme } = useAuth();

  const groups = [
    {
      label: 'Transaction Analysis',
      items: ['transaction_stats','individual_transaction','ui_flow_individual','consolidated_flow','transaction_comparison'],
    },
    {
      label: 'Registry',
      items: ['registry_single','registry_compare'],
    },
    {
      label: 'Hardware & Config',
      items: ['counters_analysis','acu_single_parse','acu_compare'],
    },
  ];

  const iconMap = {
    transaction_stats:       '📊',
    individual_transaction:  '🔍',
    ui_flow_individual:      '🖥️',
    consolidated_flow:       '🔀',
    transaction_comparison:  '↔️',
    registry_single:         '📋',
    registry_compare:        '🔄',
    counters_analysis:       '🔢',
    acu_single_parse:        '⚡',
    acu_compare:             '⚖️',
  };

  return (
    <aside className="sidebar">
      {/* Logo */}
      <div style={{ padding: '20px 16px 16px', borderBottom: '1px solid var(--border)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: 22 }}>⚙️</span>
          <div>
            <div style={{ fontFamily: 'var(--font-display)', fontWeight: 800, fontSize: 14, color: 'var(--text-primary)' }}>DN Diagnostics</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Analysis Platform</div>
          </div>
        </div>
      </div>

      {/* User */}
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border)', fontSize: 12 }}>
        <div style={{ color: 'var(--text-muted)', marginBottom: 2 }}>Logged in as</div>
        <div style={{ color: 'var(--text-primary)', fontWeight: 600 }}>{user?.name || user?.username}</div>
        <span className={`badge ${user?.role === 'ADMIN' ? 'badge-purple' : user?.role === 'DEV_MODE' ? 'badge-yellow' : 'badge-blue'}`} style={{ marginTop: 4 }}>
          {user?.role}
        </span>
      </div>

      {/* Nav groups */}
      <nav style={{ flex: 1, overflowY: 'auto', padding: '8px 8px' }}>
        {groups.map(g => {
          const visibleItems = g.items.filter(id => functionalities[id]);
          if (!visibleItems.length) return null;
          return (
            <div key={g.label} style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '1px', color: 'var(--text-muted)', padding: '4px 8px 6px' }}>
                {g.label}
              </div>
              {visibleItems.map(id => {
                const fn = functionalities[id];
                const reqsMet = fn.requires.every(r => availableTypes.includes(r));
                const isActive = selected === id;
                return (
                  <button
                    key={id}
                    onClick={() => reqsMet && onSelect(id)}
                    title={!reqsMet ? `Requires: ${fn.requires.join(', ')}` : fn.description}
                    style={{
                      width: '100%', textAlign: 'left', display: 'flex', alignItems: 'center', gap: 8,
                      padding: '8px 10px', borderRadius: 'var(--radius-sm)', border: 'none',
                      background: isActive ? 'var(--accent-glow)' : 'transparent',
                      color: isActive ? 'var(--accent)' : reqsMet ? 'var(--text-secondary)' : 'var(--text-muted)',
                      cursor: reqsMet ? 'pointer' : 'not-allowed',
                      fontSize: 12.5, fontFamily: 'var(--font-body)',
                      transition: 'all .15s',
                      borderLeft: isActive ? '2px solid var(--accent)' : '2px solid transparent',
                      opacity: reqsMet ? 1 : 0.45,
                    }}
                    onMouseEnter={e => { if (reqsMet && !isActive) e.currentTarget.style.background = 'var(--bg-deep)'; }}
                    onMouseLeave={e => { if (!isActive) e.currentTarget.style.background = 'transparent'; }}
                  >
                    <span style={{ fontSize: 14, flexShrink: 0 }}>{iconMap[id]}</span>
                    <span style={{ lineHeight: 1.3 }}>{fn.shortName || fn.name.replace(/^[ \S]+\s/, '')}</span>
                  </button>
                );
              })}
            </div>
          );
        })}
      </nav>

      {/* Bottom controls */}
      <div style={{ padding: '12px 14px', borderTop: '1px solid var(--border)', display: 'flex', flexDirection: 'column', gap: 8 }}>
        <label className="toggle-wrap" style={{ fontSize: 12, color: 'var(--text-muted)', cursor: 'pointer' }}>
          {theme === 'dark' ? '🌙 Dark Mode' : '☀️ Light Mode'}
          <span className="toggle-switch" style={{ marginLeft: 'auto' }}>
            <input type="checkbox" checked={theme === 'light'} onChange={toggleTheme} />
            <span className="toggle-slider" />
          </span>
        </label>
        <button className="btn btn-ghost btn-sm" onClick={logout} style={{ justifyContent: 'center' }}>
          🚪 Logout
        </button>
      </div>
    </aside>
  );
}
