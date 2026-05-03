import React, { useState, useEffect } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from 'utils/api';
import { Loading, AccessDenied, SelectField, MetricCard, ThemedTable } from '../UIComponents';

/* Parse a .reg file text → array of { Path, Key, Value } */
function parseRegistry(text) {
  const rows = [];
  let section = null;
  const secRe = /^\s*\[(.+?)]\s*$/;
  const kvRe  = /^\s*(@|".+?"|[^=]+?)\s*=\s*(.+?)\s*$/;

  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line) continue;
    const sm = secRe.exec(line);
    if (sm) { section = sm[1].trim(); continue; }
    if (section) {
      const mv = kvRe.exec(line);
      if (mv) {
        const [, kRaw, val] = mv;
        const key = kRaw === '@' ? '@' : kRaw.replace(/^"|"$/g,'');
        rows.push({ Path: section, Key: key, Value: val.trim() });
      }
    }
  }
  return rows;
}

function safeBase64ToText(b64) {
  try {
    const binary = atob(b64);
    const bytes  = Uint8Array.from(binary, c => c.charCodeAt(0));
    // Try UTF-8 first
    try { return new TextDecoder('utf-8', { fatal: true }).decode(bytes); } catch {}
    // Fall back to latin-1
    return new TextDecoder('latin-1').decode(bytes);
  } catch { return ''; }
}

export default function RegistrySingle() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <RegistrySingleInner />;
}

function RegistrySingleInner() {
  const [files, setFiles]       = useState({});
  const [loading, setLoading]   = useState(true);
  const [error, setError]       = useState('');
  const [selFile, setSelFile]   = useState('');
  const [parsed, setParsed]     = useState([]);
  const [search, setSearch]     = useState('');

  useEffect(() => {
    api.get('/get-registry-contents', { params: { session_id:'current_session' }, timeout: 30000 })
      .then(r => setFiles(r.data.registry_contents || {}))
      .catch(e => setError(e.response?.data?.detail || 'Failed to load registry files.'))
      .finally(() => setLoading(false));
  }, []);

  const handleSelect = (fname) => {
    setSelFile(fname); setSearch('');
    if (!fname || !files[fname]) { setParsed([]); return; }
    const text = safeBase64ToText(files[fname]);
    setParsed(parseRegistry(text));
  };

  const displayData = search.trim()
    ? parsed.filter(r =>
        r.Path.toLowerCase().includes(search.toLowerCase()) ||
        r.Key.toLowerCase().includes(search.toLowerCase())  ||
        r.Value.toLowerCase().includes(search.toLowerCase())
      )
    : parsed;

  const downloadCsv = () => {
    const cols = ['Path','Key','Value'];
    const rows = displayData.map(r => cols.map(c => `"${String(r[c]).replace(/"/g,'""')}"`).join(','));
    const csv  = [cols.join(','), ...rows].join('\n');
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type:'text/csv' }));
    a.download = `${selFile?.split('.')[0] || 'registry'}.csv`;
    a.click();
  };

  if (loading) return <Loading text="Loading registry files…" />;
  if (error)   return <div className="alert alert-error">{error}</div>;
  if (!Object.keys(files).length) return <div className="alert alert-warning">No registry files found in the uploaded package.</div>;

  return (
    <div>
      <div className="card mb-16">
        <div className="section-heading mb-12">Registry File Viewer</div>
        <SelectField
          label="Select Registry File"
          value={selFile}
          onChange={handleSelect}
          options={Object.keys(files)}
          placeholder="— Select a file —"
        />
      </div>

      {selFile && parsed.length > 0 && (
        <div className="fade-in">
          <div className="grid-3 mb-16" style={{ gap:10 }}>
            <MetricCard label="Total Entries" value={parsed.length} />
            <MetricCard label="Unique Paths"  value={new Set(parsed.map(r=>r.Path)).size} />
            <MetricCard label="Unique Keys"   value={new Set(parsed.map(r=>r.Key)).size} />
          </div>

          <div className="flex items-center gap-12 mb-12">
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search in path, key, or value…"
              style={{ flex:1 }}
            />
            {search && (
              <span style={{ fontSize:12, color:'var(--text-muted)', flexShrink:0 }}>
                {displayData.length} of {parsed.length}
              </span>
            )}
            <button className="btn btn-secondary btn-sm" onClick={downloadCsv}>⬇ CSV</button>
          </div>

          <ThemedTable data={displayData} height={420} />
        </div>
      )}

      {selFile && parsed.length === 0 && (
        <div className="alert alert-warning">No entries found in the selected registry file.</div>
      )}
    </div>
  );
}
