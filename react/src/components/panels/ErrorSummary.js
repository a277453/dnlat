/**
 * ErrorSummary panel — v5 new feature
 *
 * Calls GET /error-summary which parses TRCERROR.PRN and TRCTRACE.PRN
 * from the session and returns classified error rows grouped by severity:
 *   P1 — Critical / Exception
 *   P2 — Error / Reboot
 *   P3 — Warning
 *   P5 — Verbose / Trace
 *
 * Requires: trc_error + trc_trace file types in session.
 * RBAC: Elevated only (USER role denied).
 */
import React, { useState, useCallback } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from 'utils/api';
import { AccessDenied, Loading, MetricCard } from '../UIComponents';

/* ── Severity colour tokens ───────────────────────────────────────────────── */
const SEV_META = {
  P1: {
    label:      'P1 — Critical / Exception',
    accent:     '#ff4b4b',
    rowDark:    '#2d0000',
    rowLight:   '#ffd5d5',
    textLight:  '#b30000',
    badgeCls:   'badge-red',
  },
  P2: {
    label:      'P2 — Error / Reboot',
    accent:     '#f97316',
    rowDark:    '#2d1800',
    rowLight:   '#ffe8cc',
    textLight:  '#a05000',
    badgeCls:   'badge-yellow',
  },
  P3: {
    label:      'P3 — Warning',
    accent:     '#eab308',
    rowDark:    '#2d2600',
    rowLight:   '#fff8cc',
    textLight:  '#806600',
    badgeCls:   'badge-yellow',
  },
  P5: {
    label:      'P5 — Verbose',
    accent:     '#9ca3af',
    rowDark:    '#1e1e1e',
    rowLight:   '#f0f0f0',
    textLight:  '#555555',
    badgeCls:   '',
  },
};

/* ── Severity legend pill ─────────────────────────────────────────────────── */
function SevPill({ sev, theme }) {
  const m    = SEV_META[sev];
  const dark = theme === 'dark';
  return (
    <div style={{
      background:  dark ? m.rowDark  : m.rowLight,
      borderLeft:  `4px solid ${m.accent}`,
      padding:     '8px 14px',
      borderRadius: 6,
      color:        dark ? m.accent : m.textLight,
      fontWeight:   600,
      fontSize:     13,
    }}>
      {m.label}
    </div>
  );
}

/* ── Main component ───────────────────────────────────────────────────────── */
export default function ErrorSummary() {
  const { user, theme } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <ErrorSummaryInner theme={theme} />;
}

function ErrorSummaryInner({ theme }) {
  const dark = theme === 'dark';

  /* state */
  const [data, setData]         = useState(null);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState('');
  const [skipP5, setSkipP5]     = useState(true);
  const [filterSev, setFilterSev] = useState(['P1','P2','P3']);
  const [expandType, setExpandType] = useState(false);

  /* fetch */
  const fetchData = useCallback(async (sp5 = skipP5) => {
    setLoading(true); setError(''); setData(null);
    try {
      const res = await api.get('/error-summary', {
        params: { skip_p5: sp5 },
        timeout: 120000,
      });
      setData(res.data);
    } catch (e) {
      const status = e.response?.status;
      if (status === 404) {
        setError('Session not found — please re-upload the ZIP file.');
      } else if (e.code === 'ECONNABORTED') {
        setError('Request timed out. TRC files may be very large — try enabling "Hide P5 verbose entries".');
      } else {
        setError(e.response?.data?.detail || e.message || 'Failed to fetch error summary.');
      }
    } finally {
      setLoading(false);
    }
  }, [skipP5]);

  /* on first render, auto-fetch */
  React.useEffect(() => { fetchData(skipP5); }, []); // eslint-disable-line

  /* toggle skip_p5 — refetch with new value */
  const handleSkipP5Change = (val) => {
    setSkipP5(val);
    fetchData(val);
  };

  /* severity filter toggle */
  const toggleSev = (sev) => {
    setFilterSev(prev =>
      prev.includes(sev) ? prev.filter(s => s !== sev) : [...prev, sev]
    );
  };

  /* derived filtered rows */
  const summary  = data?.summary || [];
  const sevCounts = data?.severity_counts || {};
  const total    = data?.total_unique_errors ?? 0;
  const filtered = filterSev.length
    ? summary.filter(r => filterSev.includes(r.severity))
    : summary;
  const typeErrorRows = filtered.filter(r => r.type_error?.trim());

  /* colour helper for table rows */
  const rowBg = (sev) => {
    const m = SEV_META[sev] || SEV_META.P5;
    return dark ? m.rowDark : m.rowLight;
  };
  const rowText = (sev) => {
    const m = SEV_META[sev] || SEV_META.P5;
    return dark ? m.accent : m.textLight;
  };

  return (
    <div>
      {/* Header */}
      <p style={{ color: 'var(--text-muted)', fontSize: 13, marginBottom: 16 }}>
        Errors classified from TRCERROR.PRN and TRCTRACE.PRN by severity level.
      </p>

      {/* Severity legend */}
      <div className="grid-4 mb-16" style={{ gap: 8 }}>
        {Object.keys(SEV_META).map(sev => (
          <SevPill key={sev} sev={sev} theme={theme} />
        ))}
      </div>

      {/* Controls */}
      <div className="card mb-16">
        <div className="flex items-center gap-16" style={{ flexWrap: 'wrap' }}>

          {/* Skip P5 toggle */}
          <label className="toggle-wrap" style={{ fontSize: 13, cursor: 'pointer', userSelect: 'none' }}>
            <span className="toggle-switch">
              <input
                type="checkbox"
                checked={skipP5}
                onChange={e => handleSkipP5Change(e.target.checked)}
              />
              <span className="toggle-slider" />
            </span>
            Hide P5 verbose entries
          </label>

          {/* Severity filter chips */}
          <div className="flex items-center gap-8" style={{ flexWrap: 'wrap' }}>
            <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Filter:</span>
            {Object.keys(SEV_META).map(sev => {
              const active = filterSev.includes(sev);
              const m      = SEV_META[sev];
              return (
                <button
                  key={sev}
                  onClick={() => toggleSev(sev)}
                  style={{
                    padding:      '4px 12px',
                    borderRadius: 99,
                    border:       `1.5px solid ${active ? m.accent : 'var(--border-input)'}`,
                    background:   active ? (dark ? m.rowDark : m.rowLight) : 'transparent',
                    color:        active ? (dark ? m.accent : m.textLight) : 'var(--text-muted)',
                    cursor:       'pointer',
                    fontWeight:   600,
                    fontSize:     12,
                    fontFamily:   'var(--font-body)',
                    transition:   'all .15s',
                  }}
                >
                  {sev}
                </button>
              );
            })}
          </div>

          {/* Refresh */}
          <button
            className="btn btn-secondary btn-sm"
            style={{ marginLeft: 'auto' }}
            onClick={() => fetchData(skipP5)}
            disabled={loading}
          >
            {loading ? <span className="spinner" /> : 'Refresh'}
          </button>
        </div>
      </div>

      {/* Error state */}
      {error && <div className="alert alert-error mb-16">{error}</div>}

      {/* Loading */}
      {loading && <Loading text="Analysing TRC log files…" />}

      {/* Results */}
      {data && !loading && (
        <div className="fade-in">
          {/* Severity count metrics */}
          <div className="section-heading mb-12">Severity Counts</div>
          <div className="grid-auto mb-20" style={{ gap: 10 }}>
            <MetricCard label="Total Unique Errors" value={total} />
            {Object.entries(SEV_META).map(([sev, m]) => (
              <MetricCard
                key={sev}
                label={m.label.split('—')[0].trim()}
                value={sevCounts[sev] ?? 0}
                accent={dark ? m.accent : m.textLight}
              />
            ))}
          </div>

          <hr className="divider" />

          {/* No results */}
          {!summary.length && (
            <div className="alert alert-info">
              {data.message || 'No errors found in the uploaded TRC files.'}
            </div>
          )}

          {summary.length > 0 && (
            <>
              <div className="flex items-center justify-between mb-8">
                <div className="section-heading">Error Table</div>
                <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>
                  {filtered.length} row{filtered.length !== 1 ? 's' : ''}
                  {filtered.length !== summary.length ? ` (of ${summary.length})` : ''}
                </span>
              </div>

              {!filtered.length ? (
                <div className="alert alert-info mb-16">No errors match the selected severity filters.</div>
              ) : (
                <div className="themed-table-wrapper mb-16">
                  <div className="themed-table-scroll" style={{ maxHeight: 520 }}>
                    <table className="themed-table">
                      <thead>
                        <tr>
                          {['No','Sev','Count','Trace / StCode','Source','Message','TypeError','File'].map(h => (
                            <th key={h}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {filtered.map((row, i) => {
                          const m     = SEV_META[row.severity] || SEV_META.P5;
                          const rBg   = rowBg(row.severity);
                          const rText = rowText(row.severity);
                          const msg   = row.type_error?.trim()
                            ? `⚡ ${row.message}`
                            : row.message;
                          const te    = row.type_error?.trim()
                            ? row.type_error.replace(/\n/g, ' | ')
                            : '—';
                          return (
                            <tr
                              key={i}
                              style={{
                                background:   rBg,
                                borderLeft:   `3px solid ${m.accent}`,
                                transition:   'background .15s',
                              }}
                              onMouseEnter={e => e.currentTarget.style.opacity = '.85'}
                              onMouseLeave={e => e.currentTarget.style.opacity = '1'}
                            >
                              <td style={{ color: 'var(--text-muted)', width: 40 }}>{row.no}</td>
                              <td>
                                <span style={{
                                  display:      'inline-block',
                                  padding:      '2px 8px',
                                  borderRadius: 99,
                                  background:   `${m.accent}22`,
                                  border:       `1px solid ${m.accent}`,
                                  color:        dark ? m.accent : m.textLight,
                                  fontWeight:   700,
                                  fontSize:     11,
                                }}>
                                  {row.severity}
                                </span>
                              </td>
                              <td style={{ color: rText, fontWeight: 600 }}>{row.count}</td>
                              <td style={{ fontFamily: 'var(--font-mono)', fontSize: 12, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }} title={row.trace}>{row.trace}</td>
                              <td style={{ maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={row.source}>{row.source}</td>
                              <td style={{ maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={row.message}>{msg}</td>
                              <td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-muted)', fontSize: 12 }} title={te}>{te}</td>
                              <td style={{ fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>{row.source_file}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* TypeError expandable section */}
              {typeErrorRows.length > 0 && (
                <div className="expander">
                  <div
                    className="expander-header"
                    onClick={() => setExpandType(o => !o)}
                  >
                    <span>JavaScript TypeErrors ({typeErrorRows.length} entries)</span>
                    <span style={{
                      fontSize: 18,
                      color: 'var(--text-muted)',
                      transform: expandType ? 'rotate(180deg)' : 'none',
                      transition: 'transform .2s',
                    }}>▾</span>
                  </div>
                  {expandType && (
                    <div className="expander-body fade-in">
                      {typeErrorRows.map((row, i) => {
                        const m    = SEV_META[row.severity] || SEV_META.P5;
                        const rBg  = rowBg(row.severity);
                        const rTxt = dark ? m.accent : m.textLight;
                        return (
                          <div
                            key={i}
                            style={{
                              background:   rBg,
                              borderLeft:   `3px solid ${m.accent}`,
                              padding:      '10px 14px',
                              borderRadius: 6,
                              marginBottom: 8,
                            }}
                          >
                            <div style={{ color: rTxt, fontWeight: 700, fontSize: 12, marginBottom: 4 }}>
                              {row.source} — {row.trace} (×{row.count})
                            </div>
                            <pre style={{
                              margin:     0,
                              color:      'var(--text-secondary)',
                              fontSize:   11,
                              fontFamily: 'var(--font-mono)',
                              whiteSpace: 'pre-wrap',
                              wordBreak:  'break-word',
                            }}>
                              {row.type_error}
                            </pre>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}
