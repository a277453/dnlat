import React, { useState, useEffect } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from 'utils/api';
import useTransactionData from '../../hooks/useTransactionData';
import { Loading, AccessDenied, SelectField, MetricCard, Expander } from '../UIComponents';

export default function ConsolidatedFlow() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <ConsolidatedFlowInner />;
}

function ConsolidatedFlowInner() {
  const { sources, allTransactions, loading, error } = useTransactionData();
  const [selSource, setSelSource]   = useState('');
  const [selType, setSelType]       = useState('');
  const [generating, setGenerating] = useState(false);
  const [genError, setGenError]     = useState('');
  const [flowData, setFlowData]     = useState(null);

  const sourceTxns  = allTransactions.filter(t => t['Source File'] === selSource);
  const uniqueTypes = [...new Set(sourceTxns.map(t => t['Transaction Type']).filter(Boolean))].sort();
  const typeTxns    = sourceTxns.filter(t => !selType || t['Transaction Type'] === selType);
  const successCt   = typeTxns.filter(t => t['End State'] === 'Successful').length;
  const failCt      = typeTxns.filter(t => t['End State'] === 'Unsuccessful').length;

  const handleGenerate = async () => {
    if (!selSource || !selType) return;
    setGenerating(true); setGenError(''); setFlowData(null);
    try {
      const res = await api.post('/generate-consolidated-flow', { source_file: selSource, transaction_type: selType }, { timeout: 60000 });
      setFlowData(res.data);
    } catch (e) {
      setGenError(e.response?.data?.detail || 'Failed to generate flow.');
    } finally { setGenerating(false); }
  };

  if (loading) return <Loading text="Loading transactions…" />;
  if (error)   return <div className="alert alert-error">{error}</div>;

  return (
    <div>
      <div className="card mb-16">
        <div className="section-heading mb-12">Select Source & Type</div>
        <div className="grid-2 mb-12" style={{ gap:12 }}>
          <SelectField label="Source File"       value={selSource} onChange={v => { setSelSource(v); setSelType(''); setFlowData(null); }} options={sources} placeholder="— Select source —" />
          <SelectField label="Transaction Type"  value={selType}   onChange={v => { setSelType(v); setFlowData(null); }}                  options={uniqueTypes} placeholder="— Select type —" disabled={!selSource} />
        </div>

        {selType && (
          <div className="grid-3 mb-12" style={{ gap:10 }}>
            <MetricCard label="Total"        value={typeTxns.length} />
            <MetricCard label="Successful"   value={successCt} accent="var(--success)" />
            <MetricCard label="Unsuccessful" value={failCt}    accent="var(--danger)" />
          </div>
        )}

        <button className="btn btn-primary" onClick={handleGenerate} disabled={!selSource || !selType || generating}>
          {generating ? <><span className="spinner" /> Generating…</> : ' Generate Consolidated Flow'}
        </button>
      </div>

      {genError && <div className="alert alert-error mb-16">{genError}</div>}

      {flowData && !generating && (
        <div className="fade-in">
          <div className="alert alert-info mb-16">
            Hover over screen nodes to see transaction IDs. Arrows show flow direction with counts.
          </div>

          {/* Screens + transitions grid */}
          <div className="card mb-16">
            <div className="section-heading mb-12">Screens in Consolidated Flow</div>
            <div style={{ display:'flex', flexWrap:'wrap', gap:8 }}>
              {(flowData.screens || []).map(screen => {
                const txnList = flowData.screen_transactions?.[screen] || [];
                return (
                  <div
                    key={screen}
                    title={`${txnList.length} transaction(s): ${txnList.slice(0,5).map(t=>t.txn_id).join(', ')}${txnList.length > 5 ? '…' : ''}`}
                    style={{
                      padding:'8px 14px', borderRadius:'var(--radius-sm)',
                      background:'rgba(59,130,246,.15)', border:'1.5px solid var(--accent)',
                      color:'var(--text-primary)', fontSize:13, fontWeight:500, cursor:'default',
                    }}
                  >
                    {screen}
                    <span style={{ marginLeft:8, fontSize:11, color:'var(--text-muted)' }}>×{txnList.length}</span>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Transitions */}
          {flowData.transitions?.length > 0 && (
            <div className="card mb-16">
              <div className="section-heading mb-12">Screen Transitions</div>
              <div style={{ display:'flex', flexDirection:'column', gap:4 }}>
                {flowData.transitions.map((tr, i) => (
                  <div key={i} className="flex items-center gap-8" style={{ fontSize:13, color:'var(--text-secondary)' }}>
                    <span style={{ color:'var(--accent)', fontWeight:600, minWidth:120 }}>{tr.from}</span>
                    <span style={{ color:'var(--text-muted)' }}></span>
                    <span style={{ color:'var(--text-primary)', fontWeight:600, minWidth:120 }}>{tr.to}</span>
                    <span className="badge badge-blue">×{tr.count}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Individual flows detail */}
          <Expander title=" Individual Transaction Flow Details" defaultOpen={false}>
            <p style={{ color:'var(--text-muted)', fontSize:13, marginBottom:12 }}>
              Transactions with UI flow data: {flowData.transactions_with_flow}/{flowData.total_transactions}
            </p>
            <div style={{ display:'flex', flexDirection:'column', gap:8 }}>
              {Object.entries(flowData.transaction_flows || {}).map(([id, info]) => (
                <div key={id} style={{ padding:'8px 12px', background:'var(--bg-deep)', borderRadius:'var(--radius-sm)', fontSize:12.5 }}>
                  <span style={{ fontWeight:600, color:'var(--text-primary)' }}>{id}</span>
                  <span style={{ marginLeft:8 }} className={`badge ${info.state === 'Successful' ? 'badge-green' : 'badge-red'}`}>{info.state}</span>
                  <span style={{ marginLeft:8, color:'var(--text-muted)' }}>[{info.start_time} – {info.end_time}]</span>
                  <div style={{ marginTop:4, color:'var(--text-secondary)' }}>{(info.screens || []).join('  ')}</div>
                </div>
              ))}
            </div>
          </Expander>
        </div>
      )}
    </div>
  );
}
