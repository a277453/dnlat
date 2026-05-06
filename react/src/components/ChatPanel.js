/**
 * ChatPanel — Follow-up Q&A about an analyzed transaction.
 * NEW in v4: calls POST /chat-transaction (two-layer scope guard on backend).
 * Streaming variant available at /chat-transaction-stream but we use the
 * non-streaming endpoint for simplicity; swap to SSE if needed.
 */
import React, { useState, useRef, useEffect } from 'react';
import api from '../../utils/api';

function ChatMessage({ role, content }) {
  const isUser = role === 'user';
  return (
    <div style={{
      display: 'flex',
      flexDirection: isUser ? 'row-reverse' : 'row',
      gap: 10,
      marginBottom: 12,
      alignItems: 'flex-start',
    }}>
      <div style={{
        width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
        background: isUser ? 'var(--accent)' : 'var(--bg-deep)',
        border: '1px solid var(--border)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontSize: 12, fontWeight: 700,
        color: isUser ? '#fff' : 'var(--text-muted)',
        marginTop: 2,
      }}>
        {isUser ? 'U' : 'AI'}
      </div>
      <div style={{
        maxWidth: '82%',
        padding: '10px 14px',
        borderRadius: isUser ? '12px 4px 12px 12px' : '4px 12px 12px 12px',
        background: isUser ? 'var(--accent-glow)' : 'var(--bg-card)',
        border: `1px solid ${isUser ? 'rgba(59,130,246,.3)' : 'var(--border)'}`,
        fontSize: 13,
        lineHeight: 1.65,
        color: 'var(--text-secondary)',
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
      }}>
        {content}
      </div>
    </div>
  );
}

export default function ChatPanel({ transactionId, transactionLog, analysisText, txnData }) {
  const [history, setHistory]   = useState([]);
  const [input, setInput]       = useState('');
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState('');
  const endRef                  = useRef(null);
  const inputRef                = useRef(null);

  // Reset chat when transaction changes
  useEffect(() => {
    setHistory([]);
    setInput('');
    setError('');
  }, [transactionId]);

  // Seed the first AI message from the existing analysis when it arrives
  useEffect(() => {
    if (analysisText && history.length === 0) {
      setHistory([
        { role: 'user',      content: `Analyze transaction: ${transactionId}` },
        { role: 'assistant', content: analysisText },
      ]);
    }
  // eslint-disable-next-line
  }, [analysisText]);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [history, loading]);

  const submit = async () => {
    const q = input.trim();
    if (!q || loading) return;

    const newHistory = [...history, { role: 'user', content: q }];
    setHistory(newHistory);
    setInput('');
    setLoading(true);
    setError('');

    try {
      const res = await api.post('/chat-transaction', {
        transaction_id:  transactionId,
        question:        q,
        history:         newHistory.slice(0, -1), // exclude the question we just added
        ej_content:      transactionLog || '',
        jrn_content:     '',
        analysis_result: analysisText  || '',
        txn_data:        txnData       || {},
      }, { timeout: 120000 });

      const reply = res.data?.reply || res.data?.response || 'No reply.';
      setHistory(h => [...h, { role: 'assistant', content: reply }]);
    } catch (e) {
      const msg = e.response?.data?.detail || e.message || 'Chat failed.';
      setError(msg);
      setHistory(h => h.slice(0, -1)); // remove the user turn on error
    } finally {
      setLoading(false);
      setTimeout(() => inputRef.current?.focus(), 50);
    }
  };

  const handleKey = e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submit(); }
  };

  return (
    <div>
      <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 12 }}>
        Ask follow-up questions about this transaction. Answers are based on the EJ log and the analysis above.
      </p>

      {/* Message list */}
      <div style={{
        minHeight: 160, maxHeight: 380, overflowY: 'auto',
        padding: '12px 4px',
        borderRadius: 'var(--radius)',
        marginBottom: 12,
      }}>
        {history.length === 0 && !loading && (
          <p style={{ color: 'var(--text-muted)', fontSize: 12, textAlign: 'center', padding: '20px 0' }}>
            Run the analysis above, then ask follow-up questions here.
          </p>
        )}
        {history.map((msg, i) => (
          <ChatMessage key={i} role={msg.role} content={msg.content} />
        ))}
        {loading && (
          <div className="flex items-center gap-8" style={{ padding: '8px 0', color: 'var(--text-muted)', fontSize: 12 }}>
            <span className="spinner" />
            <span>Thinking...</span>
          </div>
        )}
        {error && <div className="alert alert-error mt-8">{error}</div>}
        <div ref={endRef} />
      </div>

      {/* Input row */}
      <div className="flex items-center gap-8">
        <textarea
          ref={inputRef}
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKey}
          placeholder="Ask a follow-up question about this transaction... (Enter to send)"
          rows={2}
          disabled={loading || !analysisText}
          style={{ flex: 1, resize: 'vertical', minHeight: 44, fontSize: 13 }}
        />
        <button
          className="btn btn-primary"
          onClick={submit}
          disabled={loading || !input.trim() || !analysisText}
          style={{ flexShrink: 0, alignSelf: 'flex-end' }}
        >
          {loading ? <span className="spinner" /> : 'Send'}
        </button>
      </div>

      {!analysisText && (
        <p style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 6 }}>
          Run the analysis first to enable chat.
        </p>
      )}

      {history.length > 2 && (
        <button
          className="btn btn-ghost btn-sm mt-8"
          onClick={() => setHistory(history.slice(0, 2))}
          style={{ fontSize: 11 }}
        >
          Clear chat history
        </button>
      )}
    </div>
  );
}
