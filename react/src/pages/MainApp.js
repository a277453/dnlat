import React, { useState, useCallback } from 'react';
import { useAuth } from '../context/AuthContext';
import Sidebar from '../components/Sidebar';
import ChunkedUploader from '../components/ChunkedUploader';
import { MetricCard } from '../components/UIComponents';
import api from '../utils/api';

// Analysis panels (lazy-ish — only rendered when selected)
import TransactionStats from '../components/panels/TransactionStats';
import IndividualTransactionAnalysis from '../components/panels/IndividualTransactionAnalysis';
import UIFlowIndividual from '../components/panels/UIFlowIndividual';
import ConsolidatedFlow from '../components/panels/ConsolidatedFlow';
import TransactionComparison from '../components/panels/TransactionComparison';
import RegistrySingle from '../components/panels/RegistrySingle';
import RegistryCompare from '../components/panels/RegistryCompare';
import CountersAnalysis from '../components/panels/CountersAnalysis';
import ACUSingleParse from '../components/panels/ACUSingleParse';
import ACUCompare from '../components/panels/ACUCompare';

/**
 * v4 NEW: Transaction Summary Bar
 * Automatically fetches and renders per-type transaction counts
 * as metric cards below the "Detected Files" section.
 * Mirrors the inline summary in streamlit_app.py:show_main_app().
 */
function TransactionSummaryBar() {
  const [stats, setStats]   = React.useState(null);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        // Trigger analysis (idempotent — backend is a no-op if already done)
        await api.post('/analyze-customer-journals', {}, { timeout: 120000 });
        const r = await api.get('/transaction-statistics', { timeout: 30000 });
        if (!cancelled) setStats(r.data?.statistics || []);
      } catch { /* silent — summary is non-critical */ }
      finally { if (!cancelled) setLoading(false); }
    };
    load();
    return () => { cancelled = true; };
  }, []);

  if (loading) return (
    <div style={{ fontSize:12, color:'var(--text-muted)', marginBottom:16 }}>
      <span className="spinner" style={{ marginRight:8 }} /> Building transaction summary…
    </div>
  );

  // Priority order: Cash Withdrawal + Cash Deposit first
  const PRIORITY = ['Cash Withdrawal', 'Cash Deposit'];
  const sorted = [
    ...(stats || []).filter(r => PRIORITY.includes(r['Transaction Type'] || r.transaction_type)),
    ...(stats || []).filter(r => !PRIORITY.includes(r['Transaction Type'] || r.transaction_type)),
  ].filter(r => {
    const name = (r['Transaction Type'] || r.transaction_type || '').trim().toLowerCase();
    return name && name !== 'unknown';
  });

  if (!sorted.length) return null;

  return (
    <div className="mb-20">
      <div className="section-heading mb-12">Transaction Summary</div>
      <div className="grid-auto">
        {sorted.map((r, i) => {
          const name = r['Transaction Type'] || r.transaction_type || '?';
          const cnt  = Number(r['Count'] || r.count || 0);
          return (
            <div key={i} className="metric-card">
              <div className="metric-label">{name}</div>
              <div className="metric-value">{cnt}</div>
            </div>
          );
        })}
      </div>
      <hr className="divider" />
    </div>
  );
}

const FUNCTIONALITIES = {
  transaction_stats: {
    name: '📊 Transaction Type Statistics',
    shortName: 'Transaction Statistics',
    description: 'View statistics for different transaction types',
    requires: ['customer_journals'],
  },
  individual_transaction: {
    name: '🔍 Individual Transaction Analysis',
    shortName: 'Individual Analysis',
    description: 'Analyze a specific transaction in detail with AI',
    requires: ['customer_journals'],
  },
  ui_flow_individual: {
    name: '🖥️ UI Flow of Individual Transaction',
    shortName: 'UI Flow (Single)',
    description: 'Visualize UI flow for a specific transaction',
    requires: ['customer_journals', 'ui_journals'],
  },
  consolidated_flow: {
    name: '🔀 Consolidated Transaction UI Flow',
    shortName: 'Consolidated Flow',
    description: 'View consolidated flow across multiple transactions',
    requires: ['customer_journals', 'ui_journals'],
  },
  transaction_comparison: {
    name: '↔️ Transaction Comparison Analysis',
    shortName: 'Transaction Comparison',
    description: 'Compare two transactions side by side',
    requires: ['customer_journals', 'ui_journals'],
  },
  registry_single: {
    name: '📋 Single View of Registry Files',
    shortName: 'Registry Viewer',
    description: 'View and analyze a single registry file',
    requires: ['registry_files'],
  },
  registry_compare: {
    name: '🔄 Compare Two Registry Files',
    shortName: 'Registry Compare',
    description: 'Compare differences between two registry files',
    requires: ['registry_files'],
  },
  counters_analysis: {
    name: '🔢 Counters Analysis',
    shortName: 'Counters Analysis',
    description: 'Analyze counter data from TRC Trace files',
    requires: ['customer_journals', 'trc_trace'],
  },
  acu_single_parse: {
    name: '⚡ ACU Parser — Single Archive',
    shortName: 'ACU Parser',
    description: 'Extract and parse ACU configuration files',
    requires: ['acu_files'],
  },
  acu_compare: {
    name: '⚖️ ACU Parser — Compare Archives',
    shortName: 'ACU Compare',
    description: 'Compare ACU configuration files from two ZIPs',
    requires: ['acu_files'],
  },
};

const CATEGORY_DISPLAY = {
  customer_journals: { label: 'Customer Journals', icon: '📋' },
  ui_journals:       { label: 'UI Journals',        icon: '🖥️' },
  trc_trace:         { label: 'TRC Trace',           icon: '📝' },
  trc_error:         { label: 'TRC Error',           icon: '⚠️' },
  registry_files:    { label: 'Registry Files',      icon: '📄' },
  acu_files:         { label: 'ACU XML Files',        icon: '⚡' },
};

const PANEL_COMPONENTS = {
  transaction_stats:       TransactionStats,
  individual_transaction:  IndividualTransactionAnalysis,
  ui_flow_individual:      UIFlowIndividual,
  consolidated_flow:       ConsolidatedFlow,
  transaction_comparison:  TransactionComparison,
  registry_single:         RegistrySingle,
  registry_compare:        RegistryCompare,
  counters_analysis:       CountersAnalysis,
  acu_single_parse:        ACUSingleParse,
  acu_compare:             ACUCompare,
};

export default function MainApp() {
  const { user } = useAuth();
  const [result, setResult] = useState(null);    // finalize-upload response
  const [selected, setSelected] = useState(null); // active function id

  const isUser = user?.role === 'USER';

  // RBAC: USER role only sees individual transaction analysis
  const visibleFunctions = isUser
    ? { individual_transaction: FUNCTIONALITIES.individual_transaction }
    : FUNCTIONALITIES;

  const categories    = result?.categories || {};
  const availableTypes = Object.entries(categories)
    .filter(([, v]) => (v?.count || 0) > 0)
    .map(([k]) => k);

  const handleResult = useCallback((r) => {
    setResult(r);
    setSelected(null);
  }, []);

  const handleClear = useCallback(() => {
    setResult(null);
    setSelected(null);
  }, []);

  const PanelComponent = selected ? PANEL_COMPONENTS[selected] : null;
  const canRenderPanel = selected && FUNCTIONALITIES[selected]?.requires.every(r => availableTypes.includes(r));

  // Transaction summary (from stats if available)
  const procTime = result?.processing_time_seconds ?? null;

  return (
    <div className="page-layout">
      <Sidebar
        selected={selected}
        onSelect={setSelected}
        functionalities={visibleFunctions}
        availableTypes={availableTypes}
      />

      <main className="main-content">
        {/* Header */}
        <div className="flex items-center justify-between mb-20" style={{ gap: 16 }}>
          <div>
            <h1 style={{ marginBottom: 2 }}>DN Diagnostics Platform</h1>
            <p style={{ color: 'var(--text-muted)', fontSize: 13 }}>
              Comprehensive analysis tool for Diebold Nixdorf diagnostic files
            </p>
          </div>
          <div style={{ fontSize: 13, color: 'var(--text-muted)', textAlign: 'right', flexShrink: 0 }}>
            <div style={{ fontWeight: 600, color: 'var(--text-primary)' }}>{user?.name || user?.username}</div>
            <span className={`badge ${user?.role === 'ADMIN' ? 'badge-purple' : user?.role === 'DEV_MODE' ? 'badge-yellow' : 'badge-blue'}`}>
              {user?.role}
            </span>
          </div>
        </div>

        {/* Upload section */}
        <div className="card mb-20">
          <div className="section-heading mb-16">📦 Upload ZIP Package (VCP Pro)</div>
          <ChunkedUploader onResult={handleResult} onClear={handleClear} />
        </div>

        {/* Detected files */}
        {result && (
          <div className="fade-in">
            <div className="section-heading mb-16">📁 Detected Files</div>
            <div className="grid-auto mb-20">
              {Object.entries(CATEGORY_DISPLAY).map(([key, { label }]) => (
                <MetricCard
                  key={key}
                  label={`${label}`}
                  value={categories[key]?.count ?? 0}
                  accent={(categories[key]?.count ?? 0) > 0 ? 'var(--accent)' : undefined}
                />
              ))}
              <MetricCard
                label="Process Time"
                value={procTime != null ? `${procTime}s` : '—'}
              />
            </div>

            <hr className="divider" />

            {/* ── v4 NEW: Transaction Summary (auto-loads for elevated roles) ─── */}
            {availableTypes.includes('customer_journals') && user?.role !== 'USER' && (
              <TransactionSummaryBar />
            )}

            {/* Function selector prompt when no sidebar item active */}
            {!selected && (
              <div style={{ textAlign: 'center', padding: '40px 24px', color: 'var(--text-muted)' }}>
                <div style={{ fontSize: 40, marginBottom: 12 }}>👈</div>
                <div style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: 6 }}>
                  Select an analysis function from the sidebar
                </div>
                <div style={{ fontSize: 13 }}>
                  {availableTypes.length === 0
                    ? 'No recognizable file types found in this package.'
                    : `Available: ${availableTypes.map(t => CATEGORY_DISPLAY[t]?.label || t).join(', ')}`}
                </div>
              </div>
            )}

            {/* Active panel */}
            {selected && !canRenderPanel && (
              <div className="alert alert-error fade-in">
                ❌ Cannot load <strong>{FUNCTIONALITIES[selected]?.name}</strong> — missing required files:{' '}
                {FUNCTIONALITIES[selected]?.requires.filter(r => !availableTypes.includes(r)).join(', ')}
              </div>
            )}

            {selected && canRenderPanel && PanelComponent && (
              <div className="fade-in">
                <div className="section-heading mb-20">{FUNCTIONALITIES[selected].name}</div>
                <PanelComponent />
              </div>
            )}
          </div>
        )}

        {/* Footer */}
        <div style={{ marginTop: 40, textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>
          © 2025–26 Diebold Nixdorf Analysis Tools
        </div>
      </main>
    </div>
  );
}
