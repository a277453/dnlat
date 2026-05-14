import React, { useState, useEffect } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from '../../utils/api';
import useTransactionData from '../../hooks/useTransactionData';
import { Loading, AccessDenied, SelectField, ThemedTable } from '../UIComponents';

function fmtDate(d) {
  if (!d || d.length !== 6) return d;
  try {
    const y = '20'+d.slice(0,2), m = d.slice(2,4), day = d.slice(4,6);
    return new Date(`${y}-${m}-${day}`).toLocaleDateString('en-GB',{day:'2-digit',month:'long',year:'numeric'});
  } catch { return d; }
}

const SUCCESS_COLOR = '#16a34a';
const FAIL_COLOR    = '#dc2626';

export default function CountersAnalysis() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <CountersInner />;
}

function CountersInner() {
  const { sources, allTransactions, loading, error } = useTransactionData();

  const [filteredSources, setFilteredSources] = useState([]);
  const [srcLoading, setSrcLoading]           = useState(true);
  const [selSource, setSelSource]             = useState('');
  const [selTxn, setSelTxn]                   = useState('');
  const [cntLoading, setCntLoading]           = useState(false);
  const [cntError, setCntError]               = useState('');
  const [cntData, setCntData]                 = useState(null);
  const [selPerTxn, setSelPerTxn]             = useState('');

  // v4 NEW: counter comparison state
  const [cmpMode, setCmpMode]       = useState('first'); // 'first' | 'previous'
  const [cmpLoading, setCmpLoading] = useState(false);
  const [cmpData, setCmpData]       = useState(null);
  const [cmpError, setCmpError]     = useState('');
  const [cmpTxnId, setCmpTxnId]     = useState(null); // which txn the comparison is for

  // Get TRC-matched sources
  useEffect(() => {
    api.get('/get-matching-sources-for-trc', { params: { session_id: 'current_session' }, timeout:30000 })
      .then(r => setFilteredSources(r.data.matching_sources || []))
      .catch(() => setFilteredSources(sources))
      .finally(() => setSrcLoading(false));
  }, [sources]);

  const srcTxns  = allTransactions.filter(t => t['Source File'] === selSource);
  const eligible = srcTxns.filter(t => ['Cash Deposit','Cash Withdrawal'].includes(t['Transaction Type']));

  const txnOptions = srcTxns.map(t => {
    const isCDW = ['Cash Deposit','Cash Withdrawal'].includes(t['Transaction Type']);
    const suffix = isCDW ? '' : ' (Not available)';
    return { value: `${t['Transaction ID']}__${t['Transaction Type']}__${t['End State']}__${t['Start Time']}${suffix}`, label: `${t['Transaction ID']} | ${t['Transaction Type']} | ${t['End State']} | ${t['Start Time']}${suffix}`, disabled: !isCDW };
  });

  const handleTxnSelect = (val) => {
    if (val.endsWith('(Not available)')) return;
    setSelTxn(val); setCntData(null); setCntError(''); setSelPerTxn('');
    // reset comparison when transaction changes
    setCmpData(null); setCmpError(''); setCmpTxnId(null);
  };

  const selectedId = selTxn.split('__')[0];

  const handleLoad = async () => {
    if (!selectedId || !selSource) return;
    setCntLoading(true); setCntError(''); setCntData(null);
    try {
      const r = await api.post('/get-counter-data', { transaction_id: selectedId, source_file: selSource }, { params: { session_id: 'current_session' }, timeout:60000 });
      setCntData(r.data);
    } catch (e) { setCntError(e.response?.data?.detail || 'Failed to load counter data.'); }
    finally { setCntLoading(false); }
  };

  if (loading || srcLoading) return <Loading text="Loading counter data sources…" />;
  if (error) return <div className="alert alert-error">{error}</div>;

  // Build per-transaction table
  const perTxnTable = (cntData?.counter_per_transaction || []).map(e => ({
    'Date Timestamp':                  e.date_timestamp,
    'Transaction ID':                  e.transaction_id,
    'Transaction Type':                e.transaction_type,
    'Transaction Summary with Result': e.transaction_summary,
    'Count':                           e.count,
    'Counter Summary':                 e.counter_summary,
    'Comment':                         e.comment,
  }));

  const perTxnOptions = perTxnTable
    .filter(r => r['Counter Summary'] === 'View Counters')
    .map(r => `${r['Date Timestamp']} | ${r['Transaction ID']} | ${r['Transaction Type']}`);

  return (
    <div>
      <div className="card mb-16">
        <div className="section-heading mb-12">Select Source & Transaction</div>
        <div className="mb-12">
          <SelectField
            label="Source File"
            value={selSource}
            onChange={v => { setSelSource(v); setSelTxn(''); setCntData(null); }}
            options={filteredSources}
            placeholder="— Select source file —"
          />
        </div>
        {selSource && (
          <>
            {eligible.length === 0 && (
              <div className="alert alert-info mb-12">Counter analysis is only available for Cash Deposit/Withdrawal transactions.</div>
            )}
            <SelectField
              label="Transaction (Cash Deposit/Withdrawal only)"
              value={selTxn}
              onChange={handleTxnSelect}
              options={txnOptions.map(o => o.value)}
              placeholder="— Select transaction —"
            />
            <button className="btn btn-primary mt-12" onClick={handleLoad} disabled={!selectedId || cntLoading}>
              {cntLoading ? <><span className="spinner" /> Loading…</> : ' Load Counter Data'}
            </button>
          </>
        )}
      </div>

      {cntError && <div className="alert alert-error mb-16">{cntError}</div>}

      {cntData && !cntLoading && (
        <div className="fade-in">
          {/* First / Start / Last counters */}
          {[
            { key:'start_counter',  title:'First Counter',           caption:'First transaction in the source file' },
            { key:'first_counter',  title:'Start Counter',           caption:'First transaction from the TRCTrace file based on selected Transaction' },
            { key:'last_counter',   title:'Last Counter',            caption:'Last transaction in the source file' },
          ].map(({ key, title, caption }) => {
            const ctr = cntData[key];
            if (!ctr) return null;
            return (
              <div key={key} className="card mb-16">
                <div className="section-heading mb-4">{title} — {fmtDate(ctr.date)} {ctr.timestamp}</div>
                <p style={{ color:'var(--text-muted)', fontSize:12, marginBottom:12 }}>{caption}</p>
                <ThemedTable data={ctr.counter_data || []} height={260} />
              </div>
            );
          })}

          {/* Per-transaction counter table */}
          {perTxnTable.length > 0 && (
            <div className="card mb-16">
              <div className="section-heading mb-12">Counter per Transaction</div>
              {/* Custom table with success/fail coloring */}
              <div className="themed-table-wrapper mb-12">
                <div className="themed-table-scroll" style={{ maxHeight:320 }}>
                  <table className="themed-table">
                    <thead>
                      <tr>{Object.keys(perTxnTable[0]).map(c => <th key={c}>{c}</th>)}</tr>
                    </thead>
                    <tbody>
                      {perTxnTable.map((row, i) => (
                        <tr key={i}>
                          {Object.entries(row).map(([c, v]) => {
                            const extra = c === 'Transaction Summary with Result'
                              ? { color: String(v).toLowerCase() === 'successful' ? SUCCESS_COLOR : String(v).toLowerCase() === 'unsuccessful' ? FAIL_COLOR : undefined, fontWeight: 600 }
                              : {};
                            return <td key={c} style={extra}>{String(v ?? '')}</td>;
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Select per-txn row to view logical counters */}
              {perTxnOptions.length > 0 && (
                <SelectField
                  label="Select transaction to view logical counters"
                  value={selPerTxn}
                  onChange={setSelPerTxn}
                  options={perTxnOptions}
                  placeholder="— Select transaction —"
                />
              )}
            </div>
          )}

          {/*  v4 NEW: Counter Comparison  */}
          {selPerTxn && selPerTxn !== '' && (() => {
            const perTxnType = selPerTxn.split(' | ')[2]?.trim();
            const perTxnId   = selPerTxn.split(' | ')[1]?.trim();
            const isEligible = ['Cash Withdrawal','Cash Deposit'].includes(perTxnType);
            if (!isEligible) return null;

            const handleCompare = async () => {
              if (cmpLoading) return;
              if (cmpTxnId === perTxnId && cmpData) return; // already loaded
              setCmpLoading(true); setCmpError(''); setCmpData(null); setCmpTxnId(perTxnId);
              try {
                const r = await api.post('/get-counter-comparison', {
                  transaction_id: perTxnId,
                  source_file:    selSource,
                  compare_mode:   cmpMode,
                }, { params: { session_id: 'current_session' }, timeout: 60000 });
                setCmpData(r.data);
              } catch (e) {
                setCmpError(e.response?.data?.detail || 'Comparison failed.');
              } finally { setCmpLoading(false); }
            };

            // Re-fetch when mode changes
            const handleModeChange = (newMode) => {
              setCmpMode(newMode);
              setCmpData(null); setCmpError(''); setCmpTxnId(null);
            };

            return (
              <div className="card fade-in mb-16">
                <div className="section-heading mb-12">Counter Comparison</div>

                {/* Mode toggle */}
                <div className="flex items-center gap-16 mb-12" style={{ flexWrap:'wrap' }}>
                  {[
                    { value:'first',    label:'Compare from First Transaction' },
                    { value:'previous', label:'Compare from Previous Transaction' },
                  ].map(opt => (
                    <label key={opt.value} className="toggle-wrap" style={{ fontSize:13, cursor:'pointer', userSelect:'none' }}>
                      <input
                        type="radio"
                        name="cmpMode"
                        value={opt.value}
                        checked={cmpMode === opt.value}
                        onChange={() => handleModeChange(opt.value)}
                        style={{ width:'auto', marginRight:4 }}
                      />
                      {opt.label}
                    </label>
                  ))}
                </div>

                <button className="btn btn-primary btn-sm mb-12" onClick={handleCompare} disabled={cmpLoading}>
                  {cmpLoading ? <><span className="spinner" /> Computing…</> : 'Compute Comparison'}
                </button>

                {cmpError && <div className="alert alert-error mb-12">{cmpError}</div>}

                {cmpData && !cmpLoading && (
                  <div className="fade-in">
                    {cmpData.no_counter_available ? (
                      <div className="alert alert-warning">{cmpData.no_counter_reason || 'No counter data available for this transaction.'}</div>
                    ) : cmpData.rows?.length > 0 ? (
                      (() => {
                        const INC = '#16a34a', DEC = '#dc2626';
                        const tableData = (cmpData.rows || []).map(r => {
                          const d = r.delta ?? 0;
                          return {
                            'No':           r.No    ?? '',
                            'Ty':           r.Ty    ?? '',
                            'ID':           r.ID    ?? '',
                            'Cur':          r.Cur   ?? '',
                            'Value':        r.Val   ?? '',
                            'Ini':          r.Ini   ?? '',
                            'PName':        r.PName ?? '',
                            'Original Cnt': r.baseline_cnt ?? '',
                            'Current Cnt':  r.second_cnt   ?? '',
                            'Change':       d > 0 ? `+${d}` : d < 0 ? `${d}` : '—',
                            '_delta':       d,
                          };
                        });
                        const cols = ['No','Ty','ID','Cur','Value','Ini','PName','Original Cnt','Current Cnt','Change'];
                        return (
                          <div className="themed-table-wrapper">
                            <div className="themed-table-scroll" style={{ maxHeight:340 }}>
                              <table className="themed-table">
                                <thead><tr>{cols.map(c => <th key={c}>{c}</th>)}</tr></thead>
                                <tbody>
                                  {tableData.map((row, i) => (
                                    <tr key={i}>
                                      {cols.map(c => {
                                        const extra = c === 'Change'
                                          ? { color: row._delta > 0 ? INC : row._delta < 0 ? DEC : 'var(--text-muted)', fontWeight: row._delta !== 0 ? 700 : 400 }
                                          : {};
                                        return <td key={c} style={extra}>{String(row[c] ?? '')}</td>;
                                      })}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        );
                      })()
                    ) : (
                      <div className="alert alert-info">
                        No counter change detected.{cmpData.no_counter_reason ? ` ${cmpData.no_counter_reason}` : ' No Cnt values changed between the baseline and the next block.'}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      )}
    </div>
  );
}
