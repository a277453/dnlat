import React, { useState, useRef, useCallback } from 'react';

// ── Metric card ──────────────────────────────────────────────────────────────
export const MetricCard = ({ label, value, accent }) => (
  <div className="metric-card">
    <div className="metric-label">{label}</div>
    <div className="metric-value" style={accent ? { color: accent } : {}}>{value ?? '—'}</div>
  </div>
);

// ── Themed HTML table ────────────────────────────────────────────────────────
export const ThemedTable = ({ data = [], height = 380 }) => {
  if (!data || data.length === 0)
    return <p style={{ color: 'var(--text-muted)', fontSize: 13 }}>No data available.</p>;

  const cols = Object.keys(data[0]);
  return (
    <div className="themed-table-wrapper">
      <div className="themed-table-scroll" style={{ maxHeight: height }}>
        <table className="themed-table">
          <thead>
            <tr>{cols.map(c => <th key={c}>{c}</th>)}</tr>
          </thead>
          <tbody>
            {data.map((row, i) => (
              <tr key={i}>
                {cols.map(c => <td key={c}>{String(row[c] ?? '')}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ── Log block ────────────────────────────────────────────────────────────────
export const LogBlock = ({ text, maxHeight = 400 }) => {
  const lines = String(text || '').split('\n');
  return (
    <div className="log-block" style={{ maxHeight }}>
      {lines.map((line, i) => (
        <div key={i} className="log-line">
          <span className="log-line-num">{i + 1}</span>
          <span className="log-line-text">{line}</span>
        </div>
      ))}
    </div>
  );
};

// ── Diff viewer ──────────────────────────────────────────────────────────────
function detectDiff(a, b) {
  if (a === b) return 'identical';
  const aS = a.trim(), bS = b.trim();
  if ((!aS && bS) || (aS && !bS)) return 'whitespace';
  if (a.replace(/\s/g,'') === b.replace(/\s/g,'')) return 'whitespace';
  return 'content';
}

export const DiffViewer = ({ content1, content2, filename1, filename2 }) => {
  const [hideIdentical, setHideIdentical] = useState(true);
  const lines1 = String(content1 || '').split('\n');
  const lines2 = String(content2 || '').split('\n');
  const maxLines = Math.max(lines1.length, lines2.length);

  const diffs = Array.from({ length: maxLines }, (_, i) => ({
    l1: lines1[i] ?? '',
    l2: lines2[i] ?? '',
    type: detectDiff(lines1[i] ?? '', lines2[i] ?? ''),
  }));

  const diffCount = diffs.filter(d => d.type !== 'identical').length;
  const visible = hideIdentical ? diffs.filter((d, i) => {
    if (!d.l1.trim() && !d.l2.trim()) return false;
    return d.type !== 'identical';
  }) : diffs.filter(d => d.l1.trim() || d.l2.trim());

  const cssClass = t => t === 'content' ? 'diff-line--content' : t === 'whitespace' ? 'diff-line--whitespace' : 'diff-line--identical';

  return (
    <div>
      {/* Legend + toggle */}
      <div className="flex items-center gap-16 mb-12" style={{ flexWrap: 'wrap' }}>
        <div className="flex items-center gap-8">
          <div style={{ width: 16, height: 16, background: 'rgba(239,68,68,.4)', border: '2px solid var(--danger)', borderRadius: 3 }} />
          <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>Content Changes</span>
        </div>
        <div className="flex items-center gap-8">
          <div style={{ width: 16, height: 16, background: 'rgba(168,85,247,.3)', border: '2px solid var(--purple)', borderRadius: 3 }} />
          <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>Whitespace Only</span>
        </div>
        <label className="toggle-wrap" style={{ fontSize: 12, color: 'var(--text-muted)', marginLeft: 'auto' }}>
          Show Diff Lines Only
          <span className="toggle-switch">
            <input type="checkbox" checked={hideIdentical} onChange={e => setHideIdentical(e.target.checked)} />
            <span className="toggle-slider" />
          </span>
        </label>
      </div>

      {hideIdentical && diffCount === 0 && (
        <div className="alert alert-success">✓ No differences found — the two files are identical.</div>
      )}
      {hideIdentical && diffCount > 0 && (
        <div className="alert alert-info mb-12">Showing {diffCount} differing line(s).</div>
      )}

      <div className="diff-wrapper">
        {/* Left pane */}
        <div className="diff-pane">
          <div className="diff-pane-header">📄 Source A: {filename1}</div>
          {visible.map((d, i) => (
            <div key={i} className={`diff-line ${cssClass(d.type)}`}>
              <span className="diff-line-num">{diffs.indexOf(d) + 1}</span>
              {d.l1.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
            </div>
          ))}
        </div>
        {/* Right pane */}
        <div className="diff-pane">
          <div className="diff-pane-header">📄 Source B: {filename2}</div>
          {visible.map((d, i) => (
            <div key={i} className={`diff-line ${cssClass(d.type)}`}>
              <span className="diff-line-num">{diffs.indexOf(d) + 1}</span>
              {d.l2.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

// ── Tabs ─────────────────────────────────────────────────────────────────────
export const Tabs = ({ tabs, activeTab, onTabChange, children }) => (
  <div>
    <div className="tabs">
      {tabs.map(t => (
        <button key={t.id} className={`tab${activeTab === t.id ? ' active' : ''}`} onClick={() => onTabChange(t.id)}>
          {t.label}
        </button>
      ))}
    </div>
    {children}
  </div>
);

// ── Expander ─────────────────────────────────────────────────────────────────
export const Expander = ({ title, children, defaultOpen = false }) => {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="expander">
      <div className="expander-header" onClick={() => setOpen(o => !o)}>
        <span>{title}</span>
        <span style={{ fontSize: 18, color: 'var(--text-muted)', transition: 'transform .2s', transform: open ? 'rotate(180deg)' : 'none' }}>▾</span>
      </div>
      {open && <div className="expander-body fade-in">{children}</div>}
    </div>
  );
};

// ── File drop zone ────────────────────────────────────────────────────────────
export const FileDropZone = ({ onFile, accept = '.zip', label = 'Drop ZIP file here or click to browse', info = '' }) => {
  const [drag, setDrag] = useState(false);
  const ref = useRef();

  const handleDrop = useCallback(e => {
    e.preventDefault(); setDrag(false);
    const file = e.dataTransfer.files[0];
    if (file) onFile(file);
  }, [onFile]);

  return (
    <div
      className={`file-drop${drag ? ' drag-over' : ''}`}
      onClick={() => ref.current.click()}
      onDragOver={e => { e.preventDefault(); setDrag(true); }}
      onDragLeave={() => setDrag(false)}
      onDrop={handleDrop}
    >
      <input ref={ref} type="file" accept={accept} style={{ display: 'none' }} onChange={e => { if (e.target.files[0]) onFile(e.target.files[0]); }} />
      <div className="file-drop-icon">📦</div>
      <p style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: 4 }}>{label}</p>
      {info && <p style={{ color: 'var(--text-muted)', fontSize: 12 }}>{info}</p>}
    </div>
  );
};

// ── Select wrapper ────────────────────────────────────────────────────────────
export const SelectField = ({ label, value, onChange, options, placeholder = 'Select…', disabled = false }) => (
  <div>
    {label && <label style={{ display: 'block', marginBottom: 5, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>{label}</label>}
    <div className="select-wrapper">
      <select value={value} onChange={e => onChange(e.target.value)} disabled={disabled}>
        {placeholder && <option value="">{placeholder}</option>}
        {options.map(o => typeof o === 'string'
          ? <option key={o} value={o}>{o}</option>
          : <option key={o.value} value={o.value}>{o.label}</option>
        )}
      </select>
      <span className="select-arrow">▾</span>
    </div>
  </div>
);

// ── Multi-select ──────────────────────────────────────────────────────────────
export const MultiSelect = ({ label, options, selected, onChange }) => {
  const toggle = val => {
    if (selected.includes(val)) onChange(selected.filter(v => v !== val));
    else onChange([...selected, val]);
  };
  const allSelected = options.length > 0 && selected.length === options.length;
  return (
    <div>
      {label && <label style={{ display: 'block', marginBottom: 8, fontWeight: 600, color: 'var(--text-primary)', fontSize: 13 }}>{label}</label>}
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 8 }}>
        <button className="btn btn-sm btn-secondary" onClick={() => onChange(allSelected ? [] : [...options])}>
          {allSelected ? 'Deselect All' : 'Select All'}
        </button>
        {selected.length > 0 && <span className="badge badge-blue">{selected.length} selected</span>}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4, maxHeight: 200, overflowY: 'auto', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', padding: '8px 12px', background: 'var(--bg-input)' }}>
        {options.map(opt => (
          <label key={opt} style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer', fontSize: 13, color: 'var(--text-secondary)', padding: '3px 0' }}>
            <input type="checkbox" checked={selected.includes(opt)} onChange={() => toggle(opt)} style={{ width: 'auto', accent: 'var(--accent)' }} />
            <span style={{ wordBreak: 'break-all' }}>{opt}</span>
          </label>
        ))}
      </div>
    </div>
  );
};

// ── Badge helper ─────────────────────────────────────────────────────────────
export const StatusBadge = ({ value }) => {
  const v = String(value || '').toLowerCase();
  const cls = v === 'successful' ? 'badge-green' : v === 'unsuccessful' ? 'badge-red' : 'badge-blue';
  return <span className={`badge ${cls}`}>{value}</span>;
};

// ── Flow chart (individual transaction) ──────────────────────────────────────
export const FlowChart = ({ screens = [] }) => (
  <div className="flow-chart">
    {screens.map((s, i) => {
      const name = typeof s === 'object' ? s.screen : s;
      const dur  = typeof s === 'object' && s.duration != null ? ` (${Number(s.duration).toFixed(2)}s)` : '';
      return (
        <React.Fragment key={i}>
          <div className="flow-node flow-node--match" style={{ background: 'rgba(59,130,246,.2)', border: '1.5px solid var(--accent)' }}>
            <span style={{ fontWeight: 600, fontSize: 12, color: 'var(--text-muted)', marginRight: 6 }}>{i + 1}.</span>
            {name}{dur}
          </div>
          {i < screens.length - 1 && <div className="flow-arrow">↓</div>}
        </React.Fragment>
      );
    })}
  </div>
);

// ── Comparison flow (two columns) ─────────────────────────────────────────────
export const ComparisonFlow = ({ flow1, flow2, matches1, matches2 }) => (
  <div className="grid-2" style={{ gap: 20 }}>
    {[{flow: flow1, matches: matches1, label: 'Transaction 1'}, {flow: flow2, matches: matches2, label: 'Transaction 2'}].map(({flow, matches, label}) => (
      <div key={label}>
        <h4 style={{ marginBottom: 12, color: 'var(--text-primary)' }}>{label}</h4>
        <div className="flow-chart">
          {(flow || []).map((s, i) => {
            const name = typeof s === 'object' ? s.screen : s;
            const dur  = typeof s === 'object' && s.duration != null ? ` (${Number(s.duration).toFixed(2)}s)` : '';
            const isMatch = matches?.[i];
            return (
              <React.Fragment key={i}>
                <div className={`flow-node ${isMatch ? 'flow-node--match' : 'flow-node--no-match'}`}>
                  <span style={{ fontWeight: 600, fontSize: 12, color: 'var(--text-muted)', marginRight: 6 }}>{i + 1}.</span>
                  {name}{dur}
                </div>
                {i < flow.length - 1 && <div className="flow-arrow">↓</div>}
              </React.Fragment>
            );
          })}
        </div>
      </div>
    ))}
  </div>
);

// ── Loading overlay ───────────────────────────────────────────────────────────
export const Loading = ({ text = 'Loading…' }) => (
  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 12, padding: 40, color: 'var(--text-muted)' }}>
    <span className="spinner spinner-lg" />
    <span style={{ fontSize: 13 }}>{text}</span>
  </div>
);

// ── Access denied ─────────────────────────────────────────────────────────────
export const AccessDenied = () => (
  <div className="alert alert-error">
    🚫 Access Denied — your role does not have permission to use this feature.
  </div>
);
