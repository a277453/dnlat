import React, { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from 'utils/api';
import { Loading, AccessDenied, SelectField, DiffViewer, FileDropZone } from '../UIComponents';

export default function ACUCompare() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <ACUCompareInner />;
}

function ACUCompareInner() {
  const [filesA, setFilesA]       = useState(null); // { filename: content_string }
  const [filesB, setFilesB]       = useState(null);
  const [loadingA, setLoadingA]   = useState(true);
  const [loadingB, setLoadingB]   = useState(false);
  const [errorA, setErrorA]       = useState('');
  const [errorB, setErrorB]       = useState('');
  const [zipBName, setZipBName]   = useState('');

  const [selFile, setSelFile]     = useState('');
  const [compResult, setCompResult] = useState(null);

  // Load Package A on mount
  useEffect(() => {
    api.get('/get-acu-files', { timeout:30000 })
      .then(r => {
        const all = r.data.acu_files_all || r.data.acu_files || {};
        // strip __xsd__ keys
        const filtered = Array.isArray(all)
          ? null
          : Object.fromEntries(Object.entries(all).filter(([k]) => !k.startsWith('__xsd__')));
        if (!filtered || !Object.keys(filtered).length) {
          setErrorA('No ACU XML files found in the main package.');
        } else {
          setFilesA(filtered);
        }
      })
      .catch(e => setErrorA(e.response?.data?.detail || 'Failed to load Package A ACU files.'))
      .finally(() => setLoadingA(false));
  }, []);

  const handleZipB = useCallback(async (file) => {
    setLoadingB(true); setErrorB(''); setFilesB(null); setZipBName('');
    const fd = new FormData();
    fd.append('file', file, file.name);
    try {
      const r = await api.post('/extract-files/', fd, { timeout:120000 });
      const all = r.data.files || {};
      const filtered = Object.fromEntries(Object.entries(all).filter(([k]) => !k.startsWith('__xsd__')));
      if (!Object.keys(filtered).length) {
        setErrorB('No ACU XML files found in the uploaded ZIP.');
      } else {
        setFilesB(filtered);
        setZipBName(file.name);
      }
    } catch (e) { setErrorB(e.response?.data?.detail || 'Failed to extract Package B.'); }
    finally { setLoadingB(false); }
  }, []);

  const basenames = (obj) => Object.keys(obj || {}).map(k => k.split('/').pop()).filter(Boolean);
  const commonFiles = filesA && filesB
    ? basenames(filesA).filter(f => basenames(filesB).includes(f))
    : [];

  const handleCompare = () => {
    if (!selFile || !filesA || !filesB) return;
    // Find full path keys
    const findKey = (obj, base) => Object.keys(obj).find(k => k.split('/').pop() === base) || base;
    const kA = findKey(filesA, selFile);
    const kB = findKey(filesB, selFile);

    const toText = (val) => {
      if (typeof val === 'string') return val;
      if (val && typeof val === 'object') return JSON.stringify(val, null, 2);
      return String(val || '');
    };

    setCompResult({
      textA: toText(filesA[kA]),
      textB: toText(filesB[kB]),
      fname: selFile,
    });
  };

  if (loadingA) return <Loading text="Loading ACU files from main package…" />;
  if (errorA)   return <div className="alert alert-error">{errorA}</div>;

  return (
    <div>
      <div className="alert alert-info mb-16">
        Compare ACU configuration XML files from two different ZIP archives.
      </div>

      {/* Source A */}
      <div className="card mb-16">
        <div className="section-heading mb-8">Source A — Main Package</div>
        <div className="alert alert-success">
           Loaded {filesA ? Object.keys(filesA).length : 0} ACU XML file(s).
        </div>
      </div>

      {/* Source B */}
      <div className="card mb-16">
        <div className="section-heading mb-12">Source B</div>
        {!filesB ? (
          <>
            <FileDropZone
              onFile={handleZipB}
              accept=".zip"
              label="Drop second ZIP archive here"
              info="ACU XML files will be extracted automatically"
            />
            {loadingB && <Loading text="Extracting ACU files from Source B…" />}
            {errorB   && <div className="alert alert-error mt-8">{errorB}</div>}
          </>
        ) : (
          <div>
            <div className="alert alert-success mb-8">
               {zipBName} — {Object.keys(filesB).length} ACU XML file(s) loaded.
            </div>
            <button className="btn btn-ghost btn-sm" onClick={() => { setFilesB(null); setZipBName(''); setSelFile(''); setCompResult(null); }}>
               Replace Source B
            </button>
          </div>
        )}
      </div>

      {/* File selector */}
      {filesA && filesB && (
        <div className="card mb-16">
          <div className="section-heading mb-12">Select Files to Compare</div>
          {commonFiles.length === 0 ? (
            <div>
              <div className="alert alert-warning mb-8">No files with matching names found.</div>
              <div className="grid-2" style={{ gap:8 }}>
                <div>
                  <strong style={{ fontSize:12 }}>Source A:</strong>
                  {basenames(filesA).map(f => <div key={f} style={{ fontSize:12, color:'var(--text-muted)', marginTop:2 }}>• {f}</div>)}
                </div>
                <div>
                  <strong style={{ fontSize:12 }}>Source B:</strong>
                  {basenames(filesB).map(f => <div key={f} style={{ fontSize:12, color:'var(--text-muted)', marginTop:2 }}>• {f}</div>)}
                </div>
              </div>
            </div>
          ) : (
            <>
              <div className="alert alert-info mb-12"> Found {commonFiles.length} matching file(s).</div>
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

      {/* Diff */}
      {compResult && (
        <div className="card fade-in">
          <div className="section-heading mb-12">Comparison: {compResult.fname}</div>
          <DiffViewer
            content1={compResult.textA}
            content2={compResult.textB}
            filename1={`Source A: ${compResult.fname}`}
            filename2={`Source B: ${compResult.fname}`}
          />
        </div>
      )}
    </div>
  );
}
