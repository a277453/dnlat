import React, { useState, useEffect } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from 'utils/api';
import useTransactionData from '../../hooks/useTransactionData';
import { Loading, AccessDenied, MultiSelect, SelectField, MetricCard, ComparisonFlow, LogBlock, Tabs, Expander } from '../UIComponents';

export default function TransactionComparison() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <ComparisonInner />;
}

function ComparisonInner() {
  const { sources, allTransactions, loading, error } = useTransactionData();

  const [selSources, setSelSources]   = useState([]);
  const [filterType, setFilterType]   = useState('All Types');
  const [filterState, setFilterState] = useState('All States');
  const [search1, setSearch1]         = useState('');
  const [search2, setSearch2]         = useState('');
  const [selTxn1, setSelTxn1]         = useState('');
  const [selTxn2, setSelTxn2]         = useState('');
  const [compLoading, setCompLoading] = useState(false);
  const [compError, setCompError]     = useState('');
  const [compData, setCompData]       = useState(null);
  const [activeTab, setActiveTab]     = useState('flow');

  useEffect(() => { if (sources.length) setSelSources([...sources]); }, [sources]);

  const base     = allTransactions.filter(t => selSources.includes(t['Source File']));
  const byType   = filterType  !== 'All Types'  ? base.filter(t => t['Transaction Type'] === filterType)  : base;
  const byState  = filterState !== 'All States' ? byType.filter(t => t['End State'] === filterState)       : byType;
  const uniqueTypes  = [...new Set(base.map(t => t['Transaction Type']).filter(Boolean))].sort();
  const uniqueStates = [...new Set(byType.map(t => t['End State']).filter(Boolean))].sort();

  const filtered1 = search1.trim()
    ? byState.filter(t => String(t['Transaction ID']).toLowerCase().includes(search1.toLowerCase()))
    : byState;
  const filtered2 = search2.trim()
    ? byState.filter(t => String(t['Transaction ID']).toLowerCase().includes(search2.toLowerCase()))
    : byState;

  const id1 = selTxn1.split(' - ')[0];
  const id2 = selTxn2.split(' - ')[0];

  const opts1 = filtered1.map(t => `${t['Transaction ID']} - ${t['Transaction Type']} | ${t['End State']} | ${t['Source File']}`);
  const opts2 = filtered2.filter(t => t['Transaction ID'] !== id1)
    .map(t => `${t['Transaction ID']} - ${t['Transaction Type']} | ${t['End State']} | ${t['Source File']}`);

  const txnData1 = base.find(t => t['Transaction ID'] === id1);
  const txnData2 = base.find(t => t['Transaction ID'] === id2);

  const handleCompare = async () => {
    if (!id1 || !id2) return;
    setCompLoading(true); setCompError(''); setCompData(null);
    try {
      const res = await api.post('/compare-transactions-flow', { txn1_id: id1, txn2_id: id2 }, { timeout: 30000 });
      setCompData(res.data);
    } catch (e) { setCompError(e.response?.data?.detail || 'Comparison failed.'); }
    finally { setCompLoading(false); }
  };

  if (loading) return <Loading text="Loading transactions…" />;
  if (error)   return <div className="alert alert-error">{error}</div>;

  const t1Screens = (compData?.txn1_flow || []).map(s => typeof s === 'object' ? s.screen : s);
  const t2Screens = (compData?.txn2_flow || []).map(s => typeof s === 'object' ? s.screen : s);
  const common    = new Set([...t1Screens].filter(s => t2Screens.includes(s)));
  const totalU    = new Set([...t1Screens, ...t2Screens]).size;
  const similarity = totalU > 0 ? ((common.size / totalU) * 100).toFixed(1) : '0';

  return (
    <div>
      {/* Filters */}
      <div className="card mb-16">
        <div className="section-heading mb-12">Filter Transactions</div>
        <div style={{ marginBottom:12 }}>
          <MultiSelect label="Source Files" options={sources} selected={selSources} onChange={setSelSources} />
        </div>
        <div className="grid-2" style={{ gap:12 }}>
          <SelectField label="Transaction Type" value={filterType}  onChange={setFilterType}  options={['All Types', ...uniqueTypes]}  placeholder={null} />
          <SelectField label="End State"        value={filterState} onChange={setFilterState} options={['All States',...uniqueStates]} placeholder={null} />
        </div>
      </div>

      {/* Transaction selectors */}
      <div className="grid-2 mb-16" style={{ gap:16 }}>
        {[
          { label:'Transaction 1', search:search1, setSearch:setSearch1, opts:opts1, sel:selTxn1, setSel:setSelTxn1, data:txnData1 },
          { label:'Transaction 2', search:search2, setSearch:setSearch2, opts:opts2, sel:selTxn2, setSel:setSelTxn2, data:txnData2 },
        ].map(({ label, search, setSearch, opts, sel, setSel, data }) => (
          <div key={label} className="card">
            <div className="section-heading mb-12">{label}</div>
            <div className="mb-8">
              <label style={{ display:'block', marginBottom:5, fontWeight:600, color:'var(--text-primary)', fontSize:13 }}>🔍 Search ID</label>
              <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Filter by Transaction ID…" />
            </div>
            <SelectField value={sel} onChange={setSel} options={opts} placeholder="— Select transaction —" />
            {data && (
              <div style={{ marginTop:12, fontSize:13, color:'var(--text-secondary)', background:'var(--bg-deep)', padding:'10px 14px', borderRadius:'var(--radius-sm)' }}>
                <div><strong>ID:</strong> {data['Transaction ID']}</div>
                <div><strong>Type:</strong> {data['Transaction Type']}</div>
                <div><strong>State:</strong> {data['End State']}</div>
                <div><strong>Duration:</strong> {data['Duration (seconds)']}s</div>
              </div>
            )}
          </div>
        ))}
      </div>

      <button className="btn btn-primary mb-16" onClick={handleCompare} disabled={!id1 || !id2 || compLoading}>
        {compLoading ? <><span className="spinner" /> Comparing…</> : '↔️ Compare Transactions'}
      </button>
      {compError && <div className="alert alert-error mb-16">{compError}</div>}

      {/* Results */}
      {compData && !compLoading && (
        <div className="fade-in">
          <Tabs
            tabs={[
              { id:'flow',     label:'🖥️ Side-by-Side Flow' },
              { id:'logs',     label:'📜 Transaction Logs' },
              { id:'analysis', label:'📊 Detailed Analysis' },
            ]}
            activeTab={activeTab}
            onTabChange={setActiveTab}
          >
            {activeTab === 'flow' && (
              <div>
                <ComparisonFlow
                  flow1={compData.txn1_flow}   flow2={compData.txn2_flow}
                  matches1={compData.txn1_matches} matches2={compData.txn2_matches}
                />
                <hr className="divider" />
                {/* Legend */}
                <div className="flex gap-16 mb-16" style={{ flexWrap:'wrap' }}>
                  <div className="flex items-center gap-8">
                    <div style={{ width:14, height:14, borderRadius:3, background:'rgba(59,130,246,.25)', border:'1.5px solid var(--accent)' }} />
                    <span style={{ fontSize:12 }}>Screen appears in both transactions</span>
                  </div>
                  <div className="flex items-center gap-8">
                    <div style={{ width:14, height:14, borderRadius:3, background:'rgba(245,158,11,.2)', border:'1.5px solid var(--warning)' }} />
                    <span style={{ fontSize:12 }}>Screen unique to this transaction</span>
                  </div>
                </div>
                {/* Similarity metrics */}
                <div className="grid-4" style={{ gap:10 }}>
                  <MetricCard label="Common Screens"   value={common.size} />
                  <MetricCard label="Different Screens" value={totalU - common.size} accent="var(--warning)" />
                  <MetricCard label="Total Unique"     value={totalU} />
                  <MetricCard label="Similarity"       value={`${similarity}%`} accent="var(--accent)" />
                </div>
              </div>
            )}

            {activeTab === 'logs' && (
              <div className="grid-2" style={{ gap:16 }}>
                <div>
                  <div className="section-heading mb-8">Transaction 1: {id1}</div>
                  <LogBlock text={compData.txn1_log || 'No log available'} maxHeight={480} />
                </div>
                <div>
                  <div className="section-heading mb-8">Transaction 2: {id2}</div>
                  <LogBlock text={compData.txn2_log || 'No log available'} maxHeight={480} />
                </div>
              </div>
            )}

            {activeTab === 'analysis' && (
              <div>
                {/* Duration comparison */}
                {txnData1 && txnData2 && (
                  <>
                    <div className="section-heading mb-12">Detailed Metrics</div>
                    <div className="grid-3 mb-16" style={{ gap:10 }}>
                      <MetricCard label="TXN1 Duration" value={`${txnData1['Duration (seconds)']}s`} />
                      <MetricCard label="TXN2 Duration" value={`${txnData2['Duration (seconds)']}s`} />
                      <MetricCard
                        label="Duration Difference"
                        value={`${Math.abs((txnData2['Duration (seconds)'] || 0) - (txnData1['Duration (seconds)'] || 0))}s`}
                        accent="var(--warning)"
                      />
                    </div>
                    <div className="grid-2 mb-16" style={{ gap:12 }}>
                      <div style={{ background:'var(--bg-deep)', padding:'10px 14px', borderRadius:'var(--radius-sm)', fontSize:13 }}>
                        <strong>TXN1 Source:</strong><br />{txnData1['Source File']}
                      </div>
                      <div style={{ background:'var(--bg-deep)', padding:'10px 14px', borderRadius:'var(--radius-sm)', fontSize:13 }}>
                        <strong>TXN2 Source:</strong><br />{txnData2['Source File']}
                      </div>
                    </div>
                    {txnData1['Source File'] === txnData2['Source File']
                      ? <div className="alert alert-success mb-16">Both transactions are from the same source file.</div>
                      : <div className="alert alert-warning mb-16">Transactions are from different source files.</div>
                    }
                  </>
                )}

                {/* Screen breakdown */}
                <div className="section-heading mb-12">Screen-by-Screen Breakdown</div>
                <div style={{ display:'flex', flexDirection:'column', gap:8 }}>
                  {t1Screens.filter(s => !t2Screens.includes(s)).length > 0 && (
                    <Expander title={`Screens unique to ${id1} (${t1Screens.filter(s=>!t2Screens.includes(s)).length})`}>
                      {t1Screens.filter(s => !t2Screens.includes(s)).map(s => <div key={s} style={{ fontSize:13, color:'var(--text-secondary)', marginBottom:4 }}>• {s}</div>)}
                    </Expander>
                  )}
                  {t2Screens.filter(s => !t1Screens.includes(s)).length > 0 && (
                    <Expander title={`Screens unique to ${id2} (${t2Screens.filter(s=>!t1Screens.includes(s)).length})`}>
                      {t2Screens.filter(s => !t1Screens.includes(s)).map(s => <div key={s} style={{ fontSize:13, color:'var(--text-secondary)', marginBottom:4 }}>• {s}</div>)}
                    </Expander>
                  )}
                  {common.size > 0 && (
                    <Expander title={`Common screens (${common.size})`} defaultOpen={true}>
                      {[...common].map(s => <div key={s} style={{ fontSize:13, color:'var(--text-secondary)', marginBottom:4 }}>• {s}</div>)}
                    </Expander>
                  )}
                </div>
              </div>
            )}
          </Tabs>
        </div>
      )}
    </div>
  );
}
