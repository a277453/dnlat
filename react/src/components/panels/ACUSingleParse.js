import React, { useState, useEffect } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from 'utils/api';
import { Loading, AccessDenied, SelectField, ThemedTable, Expander } from '../UIComponents';

export default function ACUSingleParse() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <ACUSingleInner />;
}

function ACUSingleInner() {
  const [files, setFiles]         = useState([]);
  const [loading, setLoading]     = useState(true);
  const [error, setError]         = useState('');
  const [selFile, setSelFile]     = useState('');
  const [parsing, setParsing]     = useState(false);
  const [parseErr, setParseErr]   = useState('');
  const [parsed, setParsed]       = useState(null); // { data: [...], docsCount }
  const [search, setSearch]       = useState('');
  const [selParam, setSelParam]   = useState('');

  useEffect(() => {
    api.get('/get-acu-files', { timeout:30000 })
      .then(r => {
        const f = r.data.acu_files || [];
        if (!f.length) setError('No ACU files found in the uploaded ZIP.');
        setFiles(f);
      })
      .catch(e => setError(e.response?.data?.detail || 'Failed to load ACU files.'))
      .finally(() => setLoading(false));
  }, []);

  const handleParse = async () => {
    if (!selFile) return;
    setParsing(true); setParseErr(''); setParsed(null); setSearch(''); setSelParam('');
    try {
      const r = await api.post('/parse-acu-files', [{ filename: selFile }], { timeout:120000 });
      const records   = r.data.data || [];
      const docsCount = records.filter(rec => rec.Details).length;
      setParsed({ records, docsCount });
    } catch (e) { setParseErr(e.response?.data?.detail || 'Parsing failed.'); }
    finally { setParsing(false); }
  };

  const displayData = parsed
    ? (search.trim()
        ? parsed.records.filter(r =>
            r.Parameter?.toLowerCase().includes(search.toLowerCase()) ||
            r.Value?.toLowerCase().includes(search.toLowerCase())
          )
        : parsed.records
      )
    : [];

  const selParamData = displayData.find(r => r.Parameter === selParam);

  const downloadCsv = () => {
    const rows = displayData.map(r => `"${String(r.Parameter).replace(/"/g,'""')}","${String(r.Value).replace(/"/g,'""')}"`);
    const csv  = ['Parameter,Value', ...rows].join('\n');
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
    a.download = 'acu_config_export.csv'; a.click();
  };

  if (loading) return <Loading text="Loading ACU files…" />;
  if (error)   return <div className="alert alert-error">{error}</div>;
  if (!files.length) return <div className="alert alert-warning">No ACU XML files available.</div>;

  return (
    <div>
      <div className="alert alert-info mb-16">
        Extract, parse, and analyze ACU configuration files with XSD documentation support.
      </div>

      <div className="card mb-16">
        <div className="section-heading mb-12">Select and Parse File</div>
        <div className="flex items-center gap-12">
          <div style={{ flex:1 }}>
            <SelectField value={selFile} onChange={setSelFile} options={files} placeholder="— Choose a file to parse —" />
          </div>
          <button className="btn btn-primary" onClick={handleParse} disabled={!selFile || parsing}>
            {parsing ? <><span className="spinner" /> Parsing…</> : '⚡ Parse Selected File'}
          </button>
        </div>
        {parseErr && <div className="alert alert-error mt-8">{parseErr}</div>}
      </div>

      {parsed && !parsing && (
        <div className="fade-in">
          <div className="alert alert-success mb-16">
            ✓ Parsed {parsed.records.length} parameters ({parsed.docsCount} with documentation).
          </div>

          <div className="card">
            <div className="section-heading mb-12">Parsed Parameters</div>
            <div className="flex items-center gap-12 mb-12">
              <input
                value={search}
                onChange={e => { setSearch(e.target.value); setSelParam(''); }}
                placeholder="Search parameters or values…"
                style={{ flex:1 }}
              />
              <span style={{ fontSize:12, color:'var(--text-muted)', flexShrink:0 }}>
                {displayData.length} of {parsed.records.length}
              </span>
              <button className="btn btn-secondary btn-sm" onClick={downloadCsv}>⬇ CSV</button>
            </div>

            <ThemedTable data={displayData.map(r => ({ Parameter: r.Parameter, Value: r.Value }))} height={340} />

            {/* Parameter documentation selector */}
            <div className="mt-12">
              <SelectField
                label="Select a parameter to view documentation"
                value={selParam}
                onChange={setSelParam}
                options={displayData.map(r => r.Parameter)}
                placeholder="— Select parameter —"
              />
            </div>

            {selParam && selParamData?.Details && (
              <div className="mt-12 fade-in" style={{ background:'var(--bg-deep)', padding:'14px 16px', borderRadius:'var(--radius-sm)', border:'1px solid var(--border)' }}>
                <div className="section-heading mb-8">Documentation: <code style={{ fontFamily:'var(--font-mono)', fontSize:13 }}>{selParam}</code></div>
                <div style={{ fontSize:13, color:'var(--text-secondary)', lineHeight:1.7, whiteSpace:'pre-wrap' }}>
                  {selParamData.Details}
                </div>
              </div>
            )}
            {selParam && !selParamData?.Details && (
              <div className="alert alert-info mt-8">No documentation available for this parameter.</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
