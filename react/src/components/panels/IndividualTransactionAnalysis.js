import React, { useState, useEffect, useRef } from 'react';
import { useAuth } from '../../context/AuthContext';
import api from '../../utils/api';
import useTransactionData from '../../hooks/useTransactionData';
import {
  Loading, LogBlock, SelectField, MultiSelect, Expander, ThemedTable,
} from '../UIComponents';
import ChatPanel from '../ChatPanel';

/*  helpers  */
const ANOMALY_CATEGORIES = [
  'No alternative needed - AI analysis was correct',
  'Customer Timeout/Abandonment', 'Customer Cancellation',
  'Card Reading/Hardware Issues', 'PIN Authentication Problems',
  'Cash Dispenser Malfunction', 'Account/Balance Issues',
  'Network/Communication Errors', 'System/Software Errors',
  'Receipt Printer Problems', 'Security/Fraud Detection',
  'Database/Core Banking Issues', 'Environmental Factors (Power, etc.)',
  'User Interface/Display Problems', 'Other (please specify in comments)',
];

function renderAnalysisHtml(text) {
  let t = String(text || '').trim();
  if (t.startsWith('---')) t = t.slice(3).trimStart();
  if (t.endsWith('---'))   t = t.slice(0,-3).trimEnd();
  t = t
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/\n/g,'<br>')
    .replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>')
    .replace(/<br>- /g,'<br>• ')
    .replace(/---/g,'<hr style="border-color:#333;margin:10px 0">');
  return t;
}

/*  Main component  */
export default function IndividualTransactionAnalysis() {
  const { user }  = useAuth();
  const { sources, allTransactions, loading, error } = useTransactionData();

  // Filters
  const [selSources, setSelSources]   = useState([]);
  const [filterType, setFilterType]   = useState('All Types');
  const [filterState, setFilterState] = useState('All States');
  const [searchId, setSearchId]       = useState('');

  // Selection
  const [selTxn, setSelTxn]           = useState('');
  const [txnData, setTxnData]         = useState(null);

  // Analysis
  const [analysing, setAnalysing]     = useState(false);
  const [analysis, setAnalysis]       = useState(null);
  const [analysisErr, setAnalysisErr] = useState('');

  // Feedback
  const [fbRating, setFbRating]       = useState(3);
  const [fbCause, setFbCause]         = useState(ANOMALY_CATEGORIES[0]);
  const [fbComment, setFbComment]     = useState('');
  const [fbLoading, setFbLoading]     = useState(false);
  const [fbMsg, setFbMsg]             = useState('');

  // DB retrieval (ADMIN/DEV)
  const [dbTxnId, setDbTxnId]         = useState('');
  const [dbEmpCode, setDbEmpCode]     = useState(user?.employee_code || '');
  const [dbResult, setDbResult]       = useState(null);
  const [dbLoading, setDbLoading]     = useState(false);
  const [fbDbTxnId, setFbDbTxnId]     = useState('');
  const [fbDbResult, setFbDbResult]   = useState(null);
  const [fbDbLoading, setFbDbLoading] = useState(false);

  const canElevated = ['ADMIN','DEV_MODE'].includes(user?.role);
  const canFeedback = ['USER','DEV_MODE','ADMIN'].includes(user?.role);

  // Init source selection
  useEffect(() => { if (sources.length) setSelSources(sources); }, [sources]);

  // Filtered transactions
  const base = allTransactions.filter(t => selSources.includes(t['Source File']));
  const byType  = filterType  !== 'All Types'  ? base.filter(t => t['Transaction Type'] === filterType)  : base;
  const byState = filterState !== 'All States' ? byType.filter(t => t['End State'] === filterState)       : byType;
  const searched = searchId.trim()
    ? byState.filter(t => String(t['Transaction ID']).toLowerCase().includes(searchId.toLowerCase()))
    : byState;

  const uniqueTypes  = [...new Set(base.map(t => t['Transaction Type']).filter(Boolean))].sort();
  const uniqueStates = [...new Set(byType.map(t => t['End State']).filter(Boolean))].sort();

  const txnOptions = searched.map(t =>
    `${t['Transaction ID']} | ${t['Transaction Type']} | ${t['End State']} | ${t['Source File']} | ${t['Start Time']}`
  );

  // Sync txn data when selection changes
  useEffect(() => {
    if (!selTxn) { setTxnData(null); setAnalysis(null); return; }
    const id = selTxn.split(' | ')[0];
    const found = searched.find(t => t['Transaction ID'] === id) || null;
    setTxnData(found);
    setAnalysis(null);
    setAnalysisErr('');
  }, [selTxn]); // eslint-disable-line

  const handleAnalyse = async () => {
    if (!txnData) return;
    setAnalysing(true); setAnalysisErr(''); setAnalysis(null);
    try {
      const res = await api.post('/analyze-transaction-llm', {
        transaction_id: txnData['Transaction ID'],
        employee_code: user?.employee_code,
      }, { timeout: 300000 });
      setAnalysis(res.data);
    } catch (e) {
      setAnalysisErr(e.response?.data?.detail || 'Analysis failed.');
    } finally { setAnalysing(false); }
  };

  const handleFeedback = async () => {
    if (!analysis || !txnData) return;
    setFbLoading(true); setFbMsg('');
    try {
      await api.post('/submit-llm-feedback', {
        transaction_id:      txnData['Transaction ID'],
        rating:              fbRating,
        alternative_cause:   fbCause,
        comment:             fbComment,
        user_name:           user?.username,
        user_email:          user?.email || '',
        model_version:       analysis?.metadata?.model || 'unknown',
        original_llm_response: analysis?.analysis || '',
      }, { timeout: 30000 });
      setFbMsg(' Feedback submitted successfully.');
      setFbRating(3); setFbCause(ANOMALY_CATEGORIES[0]); setFbComment('');
    } catch (e) {
      setFbMsg(`Error: ${e.response?.data?.detail || 'Submission failed.'}`);
    } finally { setFbLoading(false); }
  };

  const handleDbRetrieve = async () => {
    if (!dbTxnId.trim()) return;
    setDbLoading(true); setDbResult(null);
    try {
      const r = await api.get('/get-analysis-records', { params: { transaction_id: dbTxnId.trim(), employee_code: dbEmpCode.trim() }, timeout: 30000 });
      setDbResult(r.data);
    } catch (e) { setDbResult({ error: e.response?.data?.detail || 'Not found.' }); }
    finally { setDbLoading(false); }
  };

  const handleFbRetrieve = async () => {
    if (!fbDbTxnId.trim()) return;
    setFbDbLoading(true); setFbDbResult(null);
    try {
      const r = await api.get('/get-feedback-records', { params: { transaction_id: fbDbTxnId.trim(), user_name: user?.username }, timeout: 30000 });
      setFbDbResult(r.data);
    } catch (e) { setFbDbResult({ error: e.response?.data?.detail || 'Not found.' }); }
    finally { setFbDbLoading(false); }
  };

  if (loading) return <Loading text="Loading transactions…" />;
  if (error)   return <div className="alert alert-error">{error}</div>;

  return (
    <div>
      {/*  Filters  */}
      <div className="card mb-16">
        <div className="section-heading mb-12">Select Transaction</div>
        <div className="grid-3 mb-12" style={{ gap: 12 }}>
          <div style={{ gridColumn: 'span 3' }}>
            <MultiSelect label="Source Files" options={sources} selected={selSources} onChange={setSelSources} />
          </div>
          <SelectField label="Transaction Type" value={filterType}  onChange={setFilterType}  options={['All Types',  ...uniqueTypes]}  placeholder={null} />
          <SelectField label="End State"        value={filterState} onChange={setFilterState} options={['All States', ...uniqueStates]} placeholder={null} />
          <div>
            <label style={{ display:'block', marginBottom:5, fontWeight:600, color:'var(--text-primary)', fontSize:13 }}> Search Transaction ID</label>
            <input value={searchId} onChange={e => setSearchId(e.target.value)} placeholder="Enter Transaction ID…" />
          </div>
        </div>

        <SelectField
          label="Transaction"
          value={selTxn}
          onChange={setSelTxn}
          options={txnOptions}
          placeholder="— Select a transaction to analyze —"
        />
      </div>

      {/*  Transaction details  */}
      {txnData && (
        <div className="fade-in">
          <div className="grid-2 mb-16" style={{ gap: 16 }}>
            <div className="card">
              <div className="section-heading mb-12">Transaction Details</div>
              {[
                ['ID',         txnData['Transaction ID']],
                ['Type',       txnData['Transaction Type']],
                ['State',      txnData['End State']],
                ['Start Time', txnData['Start Time']],
                ['End Time',   txnData['End Time']],
                ['Source',     txnData['Source File']],
              ].map(([k,v]) => (
                <div key={k} className="flex items-center gap-8 mb-8" style={{ flexWrap:'wrap' }}>
                  <span style={{ minWidth:80, color:'var(--text-muted)', fontSize:12, fontWeight:600, textTransform:'uppercase', letterSpacing:'.4px' }}>{k}</span>
                  <span style={{ color:'var(--text-primary)', fontSize:13 }}>{String(v ?? '—')}</span>
                </div>
              ))}
            </div>

            <div className="card">
              <div className="section-heading mb-12">Transaction Log</div>
              <LogBlock text={txnData['Transaction Log'] || 'No log available'} maxHeight={280} />
            </div>
          </div>

          {/*  LLM Analysis  */}
          <div className="card mb-16">
            <div className="section-heading mb-12">DN Transaction Analysis</div>
            <button className="btn btn-primary mb-12" onClick={handleAnalyse} disabled={analysing}>
              {analysing ? <><span className="spinner" /> Analysing…</> : ' Analyse Transaction'}
            </button>
            {analysisErr && <div className="alert alert-error mb-12">{analysisErr}</div>}

            {analysis && (
              <div className="fade-in">
                <div className="grid-3 mb-12" style={{ gap:10 }}>
                  <div className="metric-card"><div className="metric-label">Analysis Time</div><div className="metric-value" style={{fontSize:'1.1rem'}}>{analysis.metadata?.analysis_time_seconds ?? '—'}s</div></div>
                  <div className="metric-card"><div className="metric-label">Log Size</div><div className="metric-value" style={{fontSize:'1.1rem'}}>{analysis.metadata?.log_length ?? '—'} chars</div></div>
                  <div className="metric-card"><div className="metric-label">Analysed At</div><div className="metric-value" style={{fontSize:'1.1rem'}}>{analysis.timestamp ?? '—'}</div></div>
                </div>

                <div style={{ background:'var(--bg-deep)', padding:20, borderRadius:'var(--radius)', borderLeft:'4px solid var(--success)', fontFamily:'var(--font-body)', fontSize:14, lineHeight:1.8, color:'var(--text-secondary)' }}
                  dangerouslySetInnerHTML={{ __html: renderAnalysisHtml(analysis.analysis) }}
                />

                <Expander title="Analysis Metadata" defaultOpen={false}>
                  <pre style={{ fontFamily:'var(--font-mono)', fontSize:12, color:'var(--text-code)', whiteSpace:'pre-wrap' }}>
                    {JSON.stringify(analysis.metadata, null, 2)}
                  </pre>
                </Expander>

                {/*  v4 NEW: Chat panel  */}
                <div className="card mt-16">
                  <div className="section-heading mb-12">Ask About This Transaction</div>
                  <ChatPanel
                    transactionId={txnData?.['Transaction ID']}
                    transactionLog={txnData?.['Transaction Log'] || ''}
                    analysisText={analysis.analysis}
                    txnData={txnData}
                  />
                </div>

                {/*  Feedback  */}
                {canFeedback && (
                  <Expander title=" Provide Feedback on AI Analysis" defaultOpen={false}>
                    <p style={{ color:'var(--text-muted)', fontSize:13, marginBottom:16 }}>Help us improve our AI analysis by providing feedback on the results.</p>

                    {/* Q1: Rating */}
                    <div className="mb-16">
                      <label style={{ display:'block', marginBottom:6, fontWeight:600, color:'var(--text-primary)', fontSize:13 }}>1  Rate the Analysis Quality (0–5)</label>
                      <div className="flex items-center gap-12">
                        <input type="range" min={0} max={5} value={fbRating} onChange={e => setFbRating(Number(e.target.value))} style={{ flex:1, width:'auto', padding:0 }} />
                        <span className="badge badge-blue" style={{ minWidth:80, justifyContent:'center' }}>
                          {fbRating} — {['','Poor','Fair','Good','Very Good','Excellent'][fbRating] || 'Poor'}
                        </span>
                      </div>
                    </div>

                    {/* Q2: Alternative cause */}
                    <div className="mb-16">
                      <SelectField
                        label="2  Alternative Root Cause (if applicable)"
                        value={fbCause}
                        onChange={setFbCause}
                        options={ANOMALY_CATEGORIES}
                        placeholder={null}
                      />
                    </div>

                    {/* Q3: Comment */}
                    <div className="mb-16">
                      <label style={{ display:'block', marginBottom:6, fontWeight:600, color:'var(--text-primary)', fontSize:13 }}>3  Additional Comments</label>
                      <textarea value={fbComment} onChange={e => setFbComment(e.target.value)} rows={3} placeholder="e.g., 'The analysis missed…'" />
                    </div>

                    {fbMsg && <div className={`alert ${fbMsg.startsWith('Error') ? 'alert-error' : 'alert-success'} mb-12`}>{fbMsg}</div>}

                    <div className="flex gap-12">
                      <button className="btn btn-primary" onClick={handleFeedback} disabled={fbLoading}>
                        {fbLoading ? <><span className="spinner" /> Submitting…</> : 'Submit Feedback'}
                      </button>
                      <button className="btn btn-ghost" onClick={() => { setFbRating(3); setFbCause(ANOMALY_CATEGORIES[0]); setFbComment(''); setFbMsg(''); }}>
                        Clear
                      </button>
                    </div>
                  </Expander>
                )}
              </div>
            )}
          </div>
        </div>
      )}

      {/*  ADMIN/DEV_MODE — View from DB  */}
      {canElevated && (
        <>
          <hr className="divider" />
          <div className="card mb-16">
            <div className="section-heading mb-12"> View Old Analysis from DB</div>
            <p style={{ color:'var(--text-muted)', fontSize:13, marginBottom:12 }}>Enter a Transaction ID and employee code to retrieve a previously stored analysis.</p>
            <div className="grid-2 mb-12" style={{ gap:12 }}>
              <div>
                <label style={{ display:'block', marginBottom:5, fontWeight:600, color:'var(--text-primary)', fontSize:13 }}>Transaction ID</label>
                <input value={dbTxnId} onChange={e => setDbTxnId(e.target.value)} placeholder="e.g. 234XXXXXXXXXXXX" />
              </div>
              <div>
                <label style={{ display:'block', marginBottom:5, fontWeight:600, color:'var(--text-primary)', fontSize:13 }}>Employee Code</label>
                <input value={dbEmpCode} onChange={e => setDbEmpCode(e.target.value)} placeholder="8-digit code" />
              </div>
            </div>
            <button className="btn btn-secondary" onClick={handleDbRetrieve} disabled={dbLoading || !dbTxnId.trim()}>
              {dbLoading ? <><span className="spinner" /> Retrieving…</> : ' Retrieve from DB'}
            </button>
            {dbResult && (
              <div className="mt-12 fade-in">
                {dbResult.error
                  ? <div className="alert alert-warning">{dbResult.error}</div>
                  : <>
                    <div className="alert alert-success mb-8">Found {dbResult.count} record(s)</div>
                    <pre style={{ fontFamily:'var(--font-mono)', fontSize:12, color:'var(--text-code)', whiteSpace:'pre-wrap', background:'var(--bg-deep)', padding:12, borderRadius:'var(--radius-sm)', maxHeight:300, overflow:'auto' }}>
                      {JSON.stringify(dbResult.records, null, 2)}
                    </pre>
                  </>
                }
              </div>
            )}
          </div>

          <div className="card">
            <div className="section-heading mb-12"> View Feedback from DB</div>
            <div className="flex gap-12 mb-12" style={{ alignItems:'flex-end' }}>
              <div style={{ flex:1 }}>
                <label style={{ display:'block', marginBottom:5, fontWeight:600, color:'var(--text-primary)', fontSize:13 }}>Transaction ID</label>
                <input value={fbDbTxnId} onChange={e => setFbDbTxnId(e.target.value)} placeholder="e.g. 234XXXXXXXXXXXX" />
              </div>
              <button className="btn btn-secondary" onClick={handleFbRetrieve} disabled={fbDbLoading || !fbDbTxnId.trim()}>
                {fbDbLoading ? <><span className="spinner" /> Retrieving…</> : ' Retrieve Feedback'}
              </button>
            </div>
            {fbDbResult && (
              <div className="fade-in">
                {fbDbResult.error
                  ? <div className="alert alert-warning">{fbDbResult.error}</div>
                  : <>
                    <div className="alert alert-success mb-8">Found {fbDbResult.count} feedback record(s)</div>
                    <pre style={{ fontFamily:'var(--font-mono)', fontSize:12, color:'var(--text-code)', whiteSpace:'pre-wrap', background:'var(--bg-deep)', padding:12, borderRadius:'var(--radius-sm)', maxHeight:300, overflow:'auto' }}>
                      {JSON.stringify(fbDbResult.records, null, 2)}
                    </pre>
                  </>
                }
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}
