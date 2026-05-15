import React, { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from '../../utils/api';
import useTransactionData from '../../hooks/useTransactionData';
import {
  ThemedTable, MetricCard, MultiSelect, SelectField,
  Loading, AccessDenied, StatusBadge,
} from '../UIComponents';

export default function TransactionStats() {
  const { user } = useAuth();
  if (user?.role === 'USER') return <AccessDenied />;

  return <TransactionStatsInner />;
}

function TransactionStatsInner() {
  const { sources, allTransactions, loading: txnLoading, error: txnError } = useTransactionData();

  const [stats, setStats]               = useState([]);
  const [statsLoading, setStatsLoading] = useState(true);
  const [statsError, setStatsError]     = useState('');

  // Filters
  const [selectedSources, setSelectedSources] = useState([]);
  const [filteredTxns, setFilteredTxns]        = useState([]);
  const [filterType, setFilterType]            = useState('All');
  const [filterState, setFilterState]          = useState('All');
  const [search, setSearch]                    = useState('');
  const [filterLoading, setFilterLoading]      = useState(false);

  // Load overall stats
  useEffect(() => {
    api.get('/transaction-statistics', { timeout: 30000 })
      .then(r => setStats(r.data.statistics || []))
      .catch(e => setStatsError(e.response?.data?.detail || 'Could not load statistics.'))
      .finally(() => setStatsLoading(false));
  }, []);

  // Fetch filtered transactions when sources change
  const fetchFiltered = useCallback(async (srcs) => {
    if (!srcs.length) { setFilteredTxns([]); return; }
    setFilterLoading(true);
    try {
      const res = await api.post('/filter-transactions-by-sources', { source_files: srcs }, { timeout: 30000 });
      setFilteredTxns(res.data.transactions || []);
    } catch { setFilteredTxns([]); }
    finally { setFilterLoading(false); }
  }, []);

  const handleSourceChange = (srcs) => { setSelectedSources(srcs); fetchFiltered(srcs); };

  // Derived display data with inline filters
  const uniqueTypes  = [...new Set(filteredTxns.map(t => t['Transaction Type']).filter(Boolean))].sort();
  const uniqueStates = [...new Set(filteredTxns.map(t => t['End State']).filter(Boolean))].sort();

  let display = filteredTxns;
  if (filterType  !== 'All') display = display.filter(t => t['Transaction Type'] === filterType);
  if (filterState !== 'All') display = display.filter(t => t['End State'] === filterState);
  if (search.trim()) {
    const q = search.toLowerCase();
    display = display.filter(t => String(t['Transaction ID'] || '').toLowerCase().includes(q));
  }

  const successCount = display.filter(t => t['End State'] === 'Successful').length;
  const failCount    = display.filter(t => t['End State'] === 'Unsuccessful').length;
  const successRate  = display.length > 0 ? ((successCount / display.length) * 100).toFixed(1) : '0';

  const tableData = display.map(t => ({
    'Transaction ID':  t['Transaction ID'],
    'Type':            t['Transaction Type'],
    'State':           t['End State'],
    'Duration (s)':    t['Duration (seconds)'] ?? '',
    'Source File':     t['Source File'],
    'Start Time':      t['Start Time'],
    'End Time':        t['End Time'],
  }));

  const downloadCsv = () => {
    if (!tableData.length) return;
    const cols = Object.keys(tableData[0]);
    const rows = tableData.map(r => cols.map(c => `"${String(r[c]).replace(/"/g,'""')}"`).join(','));
    const csv  = [cols.join(','), ...rows].join('\n');
    const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    a.download = `transactions_${selectedSources.length}_sources.csv`; a.click();
  };

  if (txnLoading || statsLoading) return <Loading text="Analyzing customer journals…" />;
  if (txnError) return <div className="alert alert-error">{txnError}</div>;
  if (statsError) return <div className="alert alert-error">{statsError}</div>;

  return (
    <div>
      {/* Overall stats table */}
      <div className="section-heading mb-12">Overall Transaction Statistics</div>
      <ThemedTable data={stats.map(r => ({
        'Transaction Type': r['Transaction Type'] || r.transaction_type || 'Unknown',
        'Count':            r['Count'] || r.count || 0,
        'Success Rate':     r['Success Rate'] || r.success_rate || '—',
        'Avg Duration':     r['Avg Duration'] || r.avg_duration || '—',
      }))} height={280} />

      <hr className="divider" />

      {/* Source filter */}
      <div className="section-heading mb-12">Filter by Source File</div>
      <MultiSelect
        label="Select source files to view their transactions"
        options={sources}
        selected={selectedSources}
        onChange={handleSourceChange}
      />

      {filterLoading && <Loading text="Filtering transactions…" />}

      {!filterLoading && selectedSources.length > 0 && (
        <div className="mt-16 fade-in">
          {/* Inline filters */}
          <div className="grid-2 mb-12" style={{ gap: 12 }}>
            <SelectField
              label="Transaction Type"
              value={filterType}
              onChange={setFilterType}
              options={['All', ...uniqueTypes]}
              placeholder={null}
            />
            <SelectField
              label="End State"
              value={filterState}
              onChange={setFilterState}
              options={['All', ...uniqueStates]}
              placeholder={null}
            />
          </div>

          {/* Summary metrics */}
          <div className="grid-4 mb-16" style={{ gap: 10 }}>
            <MetricCard label="Count"        value={display.length} />
            <MetricCard label="Successful"   value={successCount} accent="var(--success)" />
            <MetricCard label="Unsuccessful" value={failCount}     accent="var(--danger)" />
            <MetricCard label="Success Rate" value={`${successRate}%`} />
          </div>

          {/* Search + download row */}
          <div className="flex items-center gap-12 mb-12">
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search Transaction ID…"
              style={{ flex: 1 }}
            />
            <button className="btn btn-secondary btn-sm" onClick={downloadCsv} disabled={!display.length}>
              ⬇ CSV
            </button>
          </div>

          {display.length === 0
            ? <div className="alert alert-warning">No transactions match the current filters.</div>
            : <ThemedTable data={tableData} height={380} />
          }
        </div>
      )}
    </div>
  );
}
