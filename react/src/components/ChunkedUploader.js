import React, { useState, useRef, useCallback } from 'react';
import api from '../utils/api';

const CHUNK_MB    = 50;
const CHUNK_BYTES = CHUNK_MB * 1024 * 1024;

function genId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

export default function ChunkedUploader({ onResult, onClear }) {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [status, setStatus] = useState('');
  const [error, setError] = useState('');
  const [done, setDone] = useState(false);
  const [drag, setDrag] = useState(false);
  const inputRef = useRef();

  const reset = useCallback(() => {
    setFile(null); setUploading(false); setProgress(0);
    setStatus(''); setError(''); setDone(false);
    if (inputRef.current) inputRef.current.value = '';
    onClear?.();
  }, [onClear]);

  const startUpload = useCallback(async (f) => {
    setError(''); setDone(false); setProgress(0);
    setUploading(true);
    setStatus('Preparing…');

    const totalChunks = Math.ceil(f.size / CHUNK_BYTES);
    const uploadId    = genId();
    let offset = 0;

    for (let i = 0; i < totalChunks; i++) {
      const chunk = f.slice(offset, offset + CHUNK_BYTES);
      offset += CHUNK_BYTES;

      const fd = new FormData();
      fd.append('upload_id',    uploadId);
      fd.append('chunk_index',  String(i));
      fd.append('total_chunks', String(totalChunks));
      fd.append('filename',     f.name);
      fd.append('chunk',        chunk, `chunk_${i}`);

      try {
        await api.post('/upload-chunk', fd, { timeout: 120000 });
      } catch (err) {
        await api.delete(`/cancel-upload/${uploadId}`).catch(() => {});
        setError(`Upload failed at chunk ${i + 1}: ${err.response?.data?.detail || err.message}`);
        setUploading(false);
        return;
      }

      setProgress(Math.round(90 * (i + 1) / totalChunks));
      setStatus(`Uploading… (${i + 1}/${totalChunks})`);
    }

    setStatus('Processing package…');
    setProgress(95);

    try {
      const fd = new FormData();
      fd.append('upload_id',    uploadId);
      fd.append('filename',     f.name);
      fd.append('total_chunks', String(totalChunks));

      const res = await api.post('/finalize-upload', fd, { timeout: 600000 });
      setProgress(100);
      setStatus('Done!');
      setDone(true);
      setUploading(false);
      onResult?.(res.data);
    } catch (err) {
      setError(`Finalize failed: ${err.response?.data?.detail || err.message}`);
      setUploading(false);
    }
  }, [onResult]);

  const handleFile = useCallback((f) => {
    if (!f.name.endsWith('.zip')) { setError('Please upload a .zip file.'); return; }
    setFile(f);
    startUpload(f);
  }, [startUpload]);

  const handleDrop = useCallback(e => {
    e.preventDefault(); setDrag(false);
    const f = e.dataTransfer.files[0];
    if (f) handleFile(f);
  }, [handleFile]);

  const fileMb = file ? (file.size / 1024 / 1024).toFixed(1) : null;

  return (
    <div>
      {!file && !done && (
        <div
          className={`file-drop${drag ? ' drag-over' : ''}`}
          onClick={() => inputRef.current?.click()}
          onDragOver={e => { e.preventDefault(); setDrag(true); }}
          onDragLeave={() => setDrag(false)}
          onDrop={handleDrop}
        >
          <input ref={inputRef} type="file" accept=".zip" style={{ display: 'none' }}
            onChange={e => { if (e.target.files[0]) handleFile(e.target.files[0]); }} />
          <div className="file-drop-icon">📦</div>
          <p style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: 4 }}>
            Drop ZIP archive here or click to browse
          </p>
          <p style={{ color: 'var(--text-muted)', fontSize: 12 }}>
            Supports any size — uploaded in {CHUNK_MB} MB chunks
          </p>
        </div>
      )}

      {file && (
        <div className="card" style={{ marginTop: 0 }}>
          <div className="flex items-center justify-between mb-12">
            <div>
              <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontSize: 14 }}>📦 {file.name}</div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 2 }}>{fileMb} MB</div>
            </div>
            {!uploading && (
              <button className="btn btn-ghost btn-sm" onClick={reset}>✕ Remove</button>
            )}
          </div>

          {uploading && (
            <>
              <div style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 8 }}>{status}</div>
              <div className="progress-bar">
                <div className="progress-bar-fill" style={{ width: `${progress}%` }} />
              </div>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 6 }}>{progress}%</div>
            </>
          )}

          {done && (
            <div className="flex items-center gap-8">
              <div className="alert alert-success" style={{ flex: 1 }}>✓ Package processed successfully.</div>
              <button className="btn btn-ghost btn-sm" onClick={reset}>Upload New</button>
            </div>
          )}
        </div>
      )}

      {error && <div className="alert alert-error mt-12">{error}</div>}
    </div>
  );
}
