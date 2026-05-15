import React, { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from '../../utils/api';
import { Loading, AccessDenied, SelectField, DiffViewer, FileDropZone } from '../UIComponents';

function safeBase64ToText(b64) {
  try {
    const binary = atob(b64);
    const bytes  = Uint8Array.from(binary, c => c.charCodeAt(0));
    try { return new TextDecoder('utf-8', { fatal:true }).decode(bytes); } catch {}
    return new TextDecoder('latin-1').decode(bytes);
  } catch { return ''; }
}

export default function RegistryCompare() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <RegistryCompareInner />;
}

function RegistryCompareInner() {
  const [filesA, setFilesA]       = useState({});
  const [filesB, setFilesB]       = useState({});
  const [loadingA, setLoadingA]   = useState(true);
  const [loadingB, setLoadingB]   = useState(false);
  const [errorA, setErrorA]       = useState('');
  const [errorB, setErrorB]       = useState('');

  const [selFile, setSelFile]     = useState('');
  const [comparing, setComparing] = useState(false);
  const [compResult, setCompResult] = useState(null); // { textA, textB, fname }

  // Load Package A on mount
  useEffect(() => {
    api.get('/get-registry-contents', { params:{ session_id:'current_session' }, timeout:30000 })
      .then(r => setFilesA(r.data.registry_contents || {}))
      .catch(e => setErrorA(e.response?.data?.detail || 'Failed to load package A.'))
      .finally(() => setLoadingA(false));
  }, []);

  const handleZipB = useCallback(async (file) => {
    setLoadingB(true); setErrorB(''); setFilesB({});
    const fd = new FormData();
    fd.append('file', file, file.name);
    try {
      const r = await api.post('/extract-registry-from-zip', fd, { timeout:120000 });
      setFilesB(r.data.registry_contents || {});
    } catch (e) {
      setErrorB(e.response?.data?.detail || 'Failed to extract package B.');
    } finally { setLoadingB(false); }
  }, []);

  const commonFiles = Object.keys(filesA).filter(f => Object.keys(filesB).includes(f));

  const handleCompare = () => {
    if (!selFile || !filesA[selFile] || !filesB[selFile]) return;
    const textA = safeBase64ToText(filesA[selFile]);
    const textB = safeBase64ToText(filesB[selFile]);
    setCompResult({ textA, textB, fname: selFile });
  };

  if (loadingA) return <Loading text="Loading registry files from main package…" />;
  if (errorA)   return <div className="alert alert-error">{errorA}</div>;

  return (
    <div>
      {/* Step 1 */}
      <div className="card mb-16">
        <div className="section-heading mb-8">Step 1: Package A (Main Upload)</div>
        <div className="alert alert-success">
          ✓ Loaded {Object.keys(filesA).length} registry file(s) from main package.
        </div>
      </div>

      {/* Step 2 */}
      <div className="card mb-16">
        <div className="section-heading mb-12">Step 2: Upload Second Package for Comparison</div>
        {Object.keys(filesB).length === 0 ? (
          <>
            <FileDropZone
              onFile={handleZipB}
              accept=".zip"
              label="Drop second ZIP archive here"
              info="Registry files will be extracted automatically"
            />
            {loadingB && <Loading text="Extracting registry from Package B…" />}
            {errorB   && <div className="alert alert-error mt-8">{errorB}</div>}
          </>
        ) : (
          <div>
            <div className="alert alert-success mb-8">
              ✓ Loaded {Object.keys(filesB).length} registry file(s) from Package B.
            </div>
            <button className="btn btn-ghost btn-sm" onClick={() => { setFilesB({}); setSelFile(''); setCompResult(null); }}>
              ✕ Remove Package B
            </button>
          </div>
        )}
      </div>

      {/* Step 3 */}
      {Object.keys(filesB).length > 0 && (
        <div className="card mb-16">
          <div className="section-heading mb-12">Step 3: Select File to Compare</div>
          {commonFiles.length === 0 ? (
            <div className="alert alert-warning">
              No files with matching names found in both packages.
              <div className="grid-2 mt-8" style={{ gap:8 }}>
                <div>
                  <strong>Package A:</strong>
                  {Object.keys(filesA).map(f => <div key={f} style={{ fontSize:12, color:'var(--text-muted)', marginTop:2 }}>• {f}</div>)}
                </div>
                <div>
                  <strong>Package B:</strong>
                  {Object.keys(filesB).map(f => <div key={f} style={{ fontSize:12, color:'var(--text-muted)', marginTop:2 }}>• {f}</div>)}
                </div>
              </div>
            </div>
          ) : (
            <>
              <div className="alert alert-info mb-12">✓ Found {commonFiles.length} file(s) with matching names.</div>
              <div className="flex items-center gap-12">
                <div style={{ flex:1 }}>
                  <SelectField
                    value={selFile}
                    onChange={v => { setSelFile(v); setCompResult(null); }}
                    options={commonFiles}
                    placeholder="— Select file to compare —"
                  />
                </div>
                <button className="btn btn-primary" onClick={handleCompare} disabled={!selFile}>
                  Compare Files
                </button>
              </div>
            </>
          )}
        </div>
      )}

      {/* Diff result */}
      {compResult && (
        <div className="card fade-in">
          <div className="section-heading mb-12">File Comparison: {compResult.fname}</div>
          <DiffViewer
            content1={compResult.textA}
            content2={compResult.textB}
            filename1={`Package A: ${compResult.fname}`}
            filename2={`Package B: ${compResult.fname}`}
          />
        </div>
      )}
    </div>
  );
}
