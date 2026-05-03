import React, { useState, useEffect } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from 'utils/api';
import useTransactionData from '../../hooks/useTransactionData';
import { Loading, AccessDenied, SelectField, MultiSelect, FlowChart, LogBlock, MetricCard, Expander } from '../UIComponents';

export default function UIFlowIndividual() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;
  return <UIFlowInner />;
}

function UIFlowInner() {
  const { sources, allTransactions, loading, error } = useTransactionData();

  const [selSources, setSelSources]   = useState([]);
  const [filterType, setFilterType]   = useState('All');
  const [filterState, setFilterState] = useState('All');
  const [searchId, setSearchId]       = useState('');
  const [selTxn, setSelTxn]           = useState('');

  const [vizLoading, setVizLoading]   = useState(false);
  const [vizError, setVizError]       = useState('');
  const [vizData, setVizData]         = useState(null);

  useEffect(() => { if (sources.length) setSelSources(sources); }, [sources]);

  const base     = allTransactions.filter(t => selSources.includes(t['Source File']));
  const byType   = filterType  !== 'All' ? base.filter(t => t['Transaction Type'] === filterType)  : base;
  const byState  = filterState !== 'All' ? byType.filter(t => t['End State'] === filterState)       : byType;
  const searched = searchId.trim()
    ? byState.filter(t => String(t['Transaction ID']).toLowerCase().includes(searchId.toLowerCase()))
    : byState;

  const uniqueTypes  = [...new Set(base.map(t => t['Transaction Type']).filter(Boolean))].sort();
  const uniqueStates = [...new Set(byType.map(t => t['End State']).filter(Boolean))].sort();

  const txnOptions = searched.map(t =>
    `${t['Transaction ID']} | ${t['Transaction Type']} | ${t['End State']} | ${t['Source File']} | ${t['Start Time']}`
  );

  const handleSelect = async (val) => {
    setSelTxn(val); setVizData(null); setVizError('');
    if (!val) return;
    const id = val.split(' | ')[0];
    setVizLoading(true);
    try {
      const res = await api.post('/visualize-individual-transaction-flow', { transaction_id: id }, { timeout: 60000 });
      setVizData(res.data);
    } catch (e) {
      setVizError(e.response?.data?.detail || 'Failed to load UI flow.');
    } finally { setVizLoading(false); }
  };

  if (loading) return <Loading text="Loading transactions…" />;
  if (error)   return <div className="alert alert-error">{error}</div>;

  return (
    <div>
      {/* Filters */}
      <div className="card mb-16">
        <div className="section-heading mb-12">Filter & Select Transaction</div>
        <div style={{ marginBottom:12 }}>
          <MultiSelect label="Source Files" options={sources} selected={selSources} onChange={setSelSources} />
        </div>
        <div className="grid-2 mb-12" style={{ gap:12 }}>
          <SelectField label="Transaction Type" value={filterType}  onChange={setFilterType}  options={['All',...uniqueTypes]}  placeholder={null} />
          <SelectField label="End State"        value={filterState} onChange={setFilterState} options={['All',...uniqueStates]} placeholder={null} />
        </div>
        <div className="mb-12">
          <label style={{ display:'block', marginBottom:5, fontWeight:600, color:'var(--text-primary)', fontSize:13 }}>🔍 Search Transaction ID</label>
          <input value={searchId} onChange={e => setSearchId(e.target.value)} placeholder="Enter Transaction ID…" />
        </div>
        <SelectField
          label="Select Transaction to Visualize"
          value={selTxn}
          onChange={handleSelect}
          options={txnOptions}
          placeholder="— Select a transaction —"
        />
      </div>

      {/* Loading */}
      {vizLoading && <Loading text="Loading UI flow…" />}
      {vizError   && <div className="alert alert-error">{vizError}</div>}

      {/* Viz result */}
      {vizData && !vizLoading && (
        <div className="fade-in">
          {/* Metrics */}
          <div className="grid-3 mb-16" style={{ gap:10 }}>
            <MetricCard label="Type"       value={vizData.transaction_type} />
            <MetricCard label="State"      value={vizData.end_state} />
            <MetricCard label="UI Events"  value={vizData.num_events ?? 0} />
            <MetricCard label="Start Time" value={vizData.start_time} />
            <MetricCard label="End Time"   value={vizData.end_time} />
            <MetricCard label="Source File" value={vizData.source_file} />
          </div>

          {/* Flow */}
          <div className="card mb-16">
            <div className="section-heading mb-12">UI Flow Visualization — {vizData.transaction_id}</div>
            {vizData.has_flow && vizData.ui_flow?.length > 0
              ? <FlowChart screens={vizData.ui_flow} />
              : <div className="alert alert-warning">No UI flow data available for this transaction.</div>
            }
          </div>

          {/* Log */}
          <Expander title="📜 View Full Transaction Log" defaultOpen={false}>
            <LogBlock text={vizData.transaction_log || 'No log available'} maxHeight={420} />
          </Expander>
        </div>
      )}
    </div>
  );
}
