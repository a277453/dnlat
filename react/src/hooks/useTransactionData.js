/**
 * useTransactionData
 * Shared hook: ensures customer journals are analyzed,
 * fetches source files + all transactions, returns them.
 */
import { useState, useEffect, useCallback } from 'react';
import api from '../utils/api';

export default function useTransactionData() {
  const [sources, setSources]           = useState([]);
  const [allTransactions, setAllTxns]   = useState([]);
  const [loading, setLoading]           = useState(true);
  const [error, setError]               = useState('');

  const fetchData = useCallback(async () => {
    setLoading(true); setError('');
    try {
      // 1. Try getting sources directly
      let res = await api.get('/get-transactions-with-sources', { timeout: 60000 });

      // 2. If no sources yet, trigger analysis first
      if (!res.data.source_files?.length) {
        await api.post('/analyze-customer-journals', {}, { timeout: 300000 });
        res = await api.get('/get-transactions-with-sources', { timeout: 60000 });
      }

      setSources(res.data.source_files || []);
      setAllTxns(res.data.all_transactions || []);
    } catch (err) {
      const code = err.response?.status;
      if (code === 401 || code === 403) {
        setError('Access denied.');
      } else {
        setError(err.response?.data?.detail || err.message || 'Failed to load transaction data.');
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  return { sources, allTransactions, loading, error, refetch: fetchData };
}
