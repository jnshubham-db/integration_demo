import React, { useState, useEffect, useCallback } from 'react';

// ── Styles ────────────────────────────────────────────────────────────────────
const C = {
  dbRed:   '#FF3621',
  dbNavy:  '#1B3139',
  dbGreen: '#1B8040',
  dbGray:  '#6B7280',
  border:  '#E5E7EB',
  bg:      '#F9FAFB',
};

const S = {
  page:    { fontFamily: "'Inter', sans-serif", maxWidth: 1300, margin: '0 auto', padding: '24px 16px', background: '#fff', minHeight: '100vh' },
  header:  { borderBottom: `3px solid ${C.dbRed}`, paddingBottom: 16, marginBottom: 24 },
  h1:      { color: C.dbNavy, fontSize: 28, fontWeight: 700, margin: 0 },
  arch:    { background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: 16, marginBottom: 28, fontSize: 13 },
  archTitle: { fontWeight: 700, color: C.dbNavy, marginBottom: 10, fontSize: 14 },
  flow:    { display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' },
  box:     { background: '#fff', border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 12px', fontSize: 12, fontWeight: 600, lineHeight: 1.5 },
  arrow:   { color: C.dbRed, fontWeight: 700, fontSize: 16 },
  section: { marginBottom: 36 },
  sectionHead: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', borderBottom: `2px solid ${C.dbRed}`, paddingBottom: 8, marginBottom: 14 },
  h2:      { color: C.dbNavy, fontSize: 20, fontWeight: 700, margin: 0 },
  badge:   { fontSize: 11, borderRadius: 12, padding: '2px 10px', fontWeight: 600 },
  controls:{ display: 'flex', gap: 8, marginBottom: 12, flexWrap: 'wrap', alignItems: 'center' },
  input:   { padding: '7px 12px', borderRadius: 6, border: `1px solid ${C.border}`, fontSize: 13, outline: 'none', minWidth: 200 },
  btn:     { padding: '7px 14px', borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: 13, fontWeight: 600 },
  table:   { width: '100%', borderCollapse: 'collapse', fontSize: 13 },
  th:      { background: C.dbNavy, color: '#fff', padding: '9px 10px', textAlign: 'left', fontWeight: 600, whiteSpace: 'nowrap' },
  td:      { padding: '8px 10px', borderBottom: `1px solid ${C.border}`, verticalAlign: 'middle' },
  tdEven:  { padding: '8px 10px', borderBottom: `1px solid ${C.border}`, background: C.bg, verticalAlign: 'middle' },
  error:   { color: C.dbRed, fontSize: 13, marginBottom: 8, padding: '8px 12px', background: '#FEF2F2', borderRadius: 6 },
  success: { color: C.dbGreen, fontSize: 13, marginBottom: 8, padding: '8px 12px', background: '#F0FDF4', borderRadius: 6 },
  info:    { color: C.dbGray, fontSize: 13, margin: '8px 0' },
  form:    { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 10, background: C.bg, padding: 16, borderRadius: 8, marginBottom: 12, border: `1px solid ${C.border}` },
  label:   { display: 'flex', flexDirection: 'column', gap: 3, fontSize: 12 },
  labelTxt:{ color: C.dbGray, fontWeight: 600, textTransform: 'uppercase', fontSize: 11 },
  formInput:{ padding: '6px 10px', borderRadius: 5, border: `1px solid ${C.border}`, fontSize: 13 },
  inlineInput: { padding: '4px 8px', borderRadius: 4, border: `1px solid ${C.dbRed}`, fontSize: 12, width: 100 },
  pager:   { display: 'flex', gap: 8, alignItems: 'center', marginTop: 10, fontSize: 13, color: C.dbGray },
  stagingNote: { fontSize: 12, color: '#92400E', background: '#FFFBEB', border: '1px solid #FDE68A', borderRadius: 6, padding: '6px 12px', marginBottom: 10 },
};

const btn  = (extra = {}) => ({ ...S.btn, ...extra });
const BTN  = {
  primary: { background: C.dbRed, color: '#fff' },
  success: { background: C.dbGreen, color: '#fff' },
  danger:  { background: '#DC2626', color: '#fff' },
  neutral: { background: '#E5E7EB', color: '#374151' },
  outline: { background: '#fff', color: C.dbNavy, border: `1px solid ${C.dbNavy}` },
  amber:   { background: '#F59E0B', color: '#fff' },
};

// ── API helper ────────────────────────────────────────────────────────────────
async function api(path, opts = {}) {
  const res = await fetch(path, { headers: { 'Content-Type': 'application/json', ...opts.headers }, ...opts });
  if (!res.ok) { const t = await res.text().catch(() => res.statusText); throw new Error(t); }
  if (res.status === 204) return null;
  return res.json();
}

// ── Architecture banner ───────────────────────────────────────────────────────
function ArchBanner() {
  return (
    <div style={S.arch}>
      <div style={S.archTitle}>Architecture — Delta ↔ Lakebase Autoscaling</div>
      <div style={S.flow}>
        <div style={{ ...S.box, background: '#EFF6FF', borderColor: '#BFDBFE' }}>
          Delta Table<br/><small style={{color:C.dbGray}}>integration_demo.tpch.orders</small>
        </div>
        <span style={S.arrow}>⟶ Reverse ETL (Sync Table) ⟶</span>
        <div style={{ ...S.box, background: '#F0FDF4', borderColor: '#BBF7D0' }}>
          Lakebase (read-only)<br/><small style={{color:C.dbGray}}>tpch_sync_as.orders</small>
        </div>
        <span style={{ color: C.dbGray, fontSize: 12, padding: '0 4px' }}>← READ list here</span>
        <span style={{ color: C.dbGray, fontSize: 18 }}>|</span>
        <div style={{ ...S.box, background: '#FFFBEB', borderColor: '#FDE68A' }}>
          Lakebase (writable)<br/><small style={{color:C.dbGray}}>tpch.orders_staging</small>
        </div>
        <span style={{ color: C.dbGray, fontSize: 12, padding: '0 4px' }}>← WRITE changes here</span>
        <span style={S.arrow}>⟶ Forward ETL (15 min) ⟶</span>
        <div style={{ ...S.box, background: '#EFF6FF', borderColor: '#BFDBFE' }}>
          Delta Table<br/><small style={{color:C.dbGray}}>integration_demo.tpch.orders</small>
        </div>
      </div>
      <div style={{ marginTop: 10, fontSize: 12, color: C.dbGray, lineHeight: 1.7 }}>
        <b>📖 Read orders</b> — loaded from <code>tpch_sync_as.orders</code> (Lakebase synced table, mirrors Delta).&nbsp;&nbsp;
        <b>✏️ Edit / Create / Delete</b> — changes go to <code>tpch.orders_staging</code> (writable Postgres).&nbsp;&nbsp;
        <b>⚙️ Forward ETL</b> — Databricks Job runs every 15 min, MERGEs staging → Delta.&nbsp;&nbsp;
        <b>↺ Sync</b> — Delta changes flow back through the synced table to refresh the read view.
      </div>
    </div>
  );
}

// ── Empty order template ──────────────────────────────────────────────────────
const EMPTY = {
  o_orderkey: '', o_custkey: '', o_orderstatus: 'O', o_totalprice: '',
  o_orderdate: new Date().toISOString().slice(0, 10),
  o_orderpriority: '3-MEDIUM', o_clerk: 'Clerk#000000001',
  o_shippriority: '0', o_comment: '',
};

// ── Orders Section ────────────────────────────────────────────────────────────
function OrdersSection() {
  const [orders, setOrders]       = useState([]);
  const [total, setTotal]         = useState(0);
  const [staging, setStaging]     = useState([]);
  const [search, setSearch]       = useState('');
  const [offset, setOffset]       = useState(0);
  const [loading, setLoading]     = useState(false);
  const [seeding, setSeeding]     = useState(false);
  const [showStaging, setShowStaging] = useState(false);
  const [msg, setMsg]             = useState(null);
  const [showForm, setShowForm]   = useState(false);
  const [newOrder, setNewOrder]   = useState({ ...EMPTY });
  const [editKey, setEditKey]     = useState(null);
  const [editRow, setEditRow]     = useState({});
  const LIMIT = 50;

  const fetchOrders = useCallback(async () => {
    setLoading(true); setMsg(null);
    try {
      const params = new URLSearchParams({ limit: LIMIT, offset, ...(search ? { search } : {}) });
      const [rows, cnt] = await Promise.all([
        api(`/api/orders?${params}`),
        api(`/api/orders/count${search ? '?search=' + encodeURIComponent(search) : ''}`),
      ]);
      setOrders(rows);
      setTotal(cnt.count);
    } catch (e) { setMsg({ type: 'error', text: e.message }); }
    finally { setLoading(false); }
  }, [search, offset]);

  const fetchStaging = useCallback(async () => {
    try {
      const rows = await api('/api/orders/staging?limit=50');
      setStaging(rows);
    } catch (e) { /* staging table may be empty */ }
  }, []);

  useEffect(() => { fetchOrders(); }, [fetchOrders]);
  useEffect(() => { if (showStaging) fetchStaging(); }, [showStaging, fetchStaging]);

  // Seed into staging
  const seedFromDelta = async () => {
    setSeeding(true); setMsg(null);
    try {
      const r = await api('/api/seed', { method: 'POST' });
      setMsg({ type: 'success', text: `Seeded ${r.seeded} orders into staging (${r.attempted} attempted). Forward ETL will sync to Delta.` });
      fetchStaging();
    } catch (e) { setMsg({ type: 'error', text: 'Seed failed: ' + e.message }); }
    finally { setSeeding(false); }
  };

  // Create → writes to staging
  const createOrder = async (e) => {
    e.preventDefault();
    try {
      await api('/api/orders', { method: 'POST', body: JSON.stringify(newOrder) });
      setMsg({ type: 'success', text: `Order ${newOrder.o_orderkey} created in staging. Forward ETL will merge to Delta.` });
      setShowForm(false); setNewOrder({ ...EMPTY });
      fetchStaging();
    } catch (e) { setMsg({ type: 'error', text: e.message }); }
  };

  // Edit → writes to staging (copies from synced if not already in staging)
  const startEdit = (row) => { setEditKey(row.o_orderkey); setEditRow({ ...row }); };
  const cancelEdit = () => { setEditKey(null); setEditRow({}); };
  const saveEdit = async () => {
    try {
      await api(`/api/orders/${editKey}`, { method: 'PUT', body: JSON.stringify(editRow) });
      setMsg({ type: 'success', text: `Order ${editKey} saved to staging. Forward ETL will merge to Delta.` });
      cancelEdit();
      fetchOrders();
      fetchStaging();
    } catch (e) { setMsg({ type: 'error', text: e.message }); }
  };

  // Delete → removes from staging only
  const deleteOrder = async (key) => {
    if (!window.confirm(`Remove order ${key} from staging?\n\nNote: The order will remain in the Delta synced view until the next Forward ETL run removes it.`)) return;
    try {
      await api(`/api/orders/${key}`, { method: 'DELETE' });
      setMsg({ type: 'success', text: `Order ${key} removed from staging.` });
      fetchOrders();
      fetchStaging();
    } catch (e) { setMsg({ type: 'error', text: e.message }); }
  };

  const READ_COLS    = ['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority','o_comment'];
  const STAGING_COLS = ['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority','o_comment','last_modified'];

  return (
    <div style={S.section}>

      {/* ── Synced Orders (READ) ── */}
      <div style={S.sectionHead}>
        <div>
          <h2 style={S.h2}>Orders <span style={{ fontSize: 13, fontWeight: 400, color: C.dbGray }}>(Delta snapshot via Synced Table)</span></h2>
          <div style={{ fontSize: 12, color: C.dbGray, marginTop: 3 }}>
            Source: <code>tpch_sync_as.orders</code> — read-only · updated continuously from Delta
          </div>
        </div>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
          <span style={{ ...S.badge, background: '#EFF6FF', color: '#1D4ED8', border: '1px solid #BFDBFE' }}>
            Synced · {total} rows
          </span>
          <span style={{ ...S.badge, background: '#F0FDF4', color: C.dbGreen, border: '1px solid #BBF7D0' }}>
            Read-Only
          </span>
        </div>
      </div>

      {/* Controls */}
      <div style={S.controls}>
        <input style={S.input} placeholder="🔍 Search by comment…" value={search}
          onChange={e => { setSearch(e.target.value); setOffset(0); }} />
        <button style={btn(BTN.primary)} onClick={() => { setShowForm(v => !v); setMsg(null); }}>
          {showForm ? '✕ Cancel' : '+ New Order'}
        </button>
        <button style={btn(BTN.neutral)} onClick={fetchOrders}>↺ Refresh</button>
        <button style={btn({ ...BTN.outline, opacity: seeding ? 0.6 : 1 })} onClick={seedFromDelta} disabled={seeding}>
          {seeding ? 'Seeding…' : '⬇ Seed 100 from Delta'}
        </button>
        <button style={btn({ ...BTN.amber, opacity: showStaging ? 1 : 0.85 })}
          onClick={() => setShowStaging(v => !v)}>
          {showStaging ? '▲ Hide' : '▼ Show'} Pending Changes ({staging.length})
        </button>
      </div>

      {msg && <div style={msg.type === 'error' ? S.error : S.success}>{msg.text}</div>}

      {/* Create form */}
      {showForm && (
        <>
          <div style={S.stagingNote}>
            ✏️ New orders are written to <strong>tpch.orders_staging</strong> (Lakebase writable). The Forward ETL job will MERGE them into Delta every 15 min.
          </div>
          <form onSubmit={createOrder} style={S.form}>
            {Object.keys(EMPTY).map(k => (
              <label key={k} style={S.label}>
                <span style={S.labelTxt}>{k.replace('o_', '')}</span>
                <input style={S.formInput} value={newOrder[k]}
                  required={['o_orderkey','o_custkey','o_comment'].includes(k)}
                  onChange={e => setNewOrder(v => ({ ...v, [k]: e.target.value }))} />
              </label>
            ))}
            <div style={{ gridColumn: 'span 3' }}>
              <button type="submit" style={btn(BTN.success)}>✓ Save to Staging</button>
            </div>
          </form>
        </>
      )}

      {/* Synced orders table */}
      {loading
        ? <p style={S.info}>Loading from synced table…</p>
        : (
          <>
            {total === 0 && !loading && (
              <div style={{ ...S.info, padding: 16, background: C.bg, borderRadius: 8, textAlign: 'center', border: `1px solid ${C.border}` }}>
                Synced table is empty. Initial backfill from Delta may take 5–30 min after synced table setup.
              </div>
            )}
            {total > 0 && (
              <>
                <div style={S.info}>{total} total rows · showing {offset + 1}–{Math.min(offset + LIMIT, total)}</div>
                <div style={{ overflowX: 'auto', borderRadius: 8, border: `1px solid ${C.border}` }}>
                  <table style={S.table}>
                    <thead>
                      <tr>
                        {READ_COLS.map(c => <th key={c} style={S.th}>{c.replace('o_', '')}</th>)}
                        <th style={{ ...S.th, textAlign: 'center' }}>Actions → Staging</th>
                      </tr>
                    </thead>
                    <tbody>
                      {orders.map((row, i) => {
                        const isEdit = editKey === row.o_orderkey;
                        return (
                          <tr key={row.o_orderkey}>
                            {READ_COLS.map(c => (
                              <td key={c} style={i % 2 === 0 ? S.td : S.tdEven}>
                                {isEdit && c !== 'o_orderkey'
                                  ? <input style={S.inlineInput} value={editRow[c] ?? ''}
                                      onChange={e => setEditRow(v => ({ ...v, [c]: e.target.value }))} />
                                  : <span style={c === 'o_orderstatus' ? { fontWeight: 700, color: row[c] === 'O' ? C.dbGreen : row[c] === 'F' ? C.dbGray : C.dbRed } : {}}>
                                      {String(row[c] ?? '')}
                                    </span>}
                              </td>
                            ))}
                            <td style={{ ...(i % 2 === 0 ? S.td : S.tdEven), textAlign: 'center', whiteSpace: 'nowrap' }}>
                              {isEdit
                                ? <>
                                    <button style={btn({ ...BTN.success, marginRight: 4, padding: '4px 10px', fontSize: 12 })} onClick={saveEdit}>✓ Save to Staging</button>
                                    <button style={btn({ ...BTN.neutral, padding: '4px 10px', fontSize: 12 })} onClick={cancelEdit}>✕</button>
                                  </>
                                : <>
                                    <button style={btn({ ...BTN.outline, marginRight: 4, padding: '4px 10px', fontSize: 12 })} onClick={() => startEdit(row)}>✎ Edit</button>
                                    <button style={btn({ ...BTN.danger, padding: '4px 10px', fontSize: 12 })} onClick={() => deleteOrder(row.o_orderkey)}>✕ Del</button>
                                  </>}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
                <div style={S.pager}>
                  <button style={btn({ ...BTN.neutral, padding: '5px 12px' })} disabled={offset === 0}
                    onClick={() => setOffset(v => Math.max(0, v - LIMIT))}>← Prev</button>
                  <span>Page {Math.floor(offset / LIMIT) + 1} of {Math.ceil(total / LIMIT) || 1}</span>
                  <button style={btn({ ...BTN.neutral, padding: '5px 12px' })} disabled={offset + LIMIT >= total}
                    onClick={() => setOffset(v => v + LIMIT)}>Next →</button>
                </div>
              </>
            )}
          </>
        )}

      {/* ── Pending Changes (Staging) ── */}
      {showStaging && (
        <div style={{ marginTop: 24, border: '2px solid #FDE68A', borderRadius: 8, padding: 16, background: '#FFFBEB' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
            <div>
              <span style={{ fontWeight: 700, color: '#92400E', fontSize: 15 }}>⏳ Pending ETL Changes</span>
              <div style={{ fontSize: 12, color: '#B45309', marginTop: 2 }}>
                Source: <code>tpch.orders_staging</code> — changes queued for Forward ETL → Delta MERGE
              </div>
            </div>
            <button style={btn({ ...BTN.neutral, padding: '5px 10px', fontSize: 12 })} onClick={fetchStaging}>↺ Refresh</button>
          </div>
          {staging.length === 0
            ? <div style={{ color: '#B45309', fontSize: 13, textAlign: 'center', padding: 12 }}>No pending changes. Create or edit orders to see them here before the Forward ETL runs.</div>
            : (
              <div style={{ overflowX: 'auto', borderRadius: 6, border: '1px solid #FDE68A' }}>
                <table style={S.table}>
                  <thead>
                    <tr>{STAGING_COLS.map(c => <th key={c} style={{ ...S.th, background: '#92400E' }}>{c.replace('o_', '')}</th>)}</tr>
                  </thead>
                  <tbody>
                    {staging.map((row, i) => (
                      <tr key={row.o_orderkey}>
                        {STAGING_COLS.map(c => (
                          <td key={c} style={{ ...S.td, background: i % 2 === 0 ? '#fff' : '#FFFBEB' }}>
                            {String(row[c] ?? '')}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          <div style={{ fontSize: 12, color: '#B45309', marginTop: 8 }}>
            💡 Forward ETL job runs every 15 min and MERGEs these rows into <code>integration_demo.tpch.orders</code> (Delta). The synced table then picks up those changes.
          </div>
        </div>
      )}
    </div>
  );
}

// ── Customers Section ─────────────────────────────────────────────────────────
function CustomersSection() {
  const [customers, setCustomers] = useState([]);
  const [loading, setLoading]     = useState(false);
  const [error, setError]         = useState('');

  useEffect(() => {
    setLoading(true);
    api('/api/customers?limit=100')
      .then(setCustomers)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const COLS = ['c_custkey','c_name','c_nationkey','c_acctbal','c_mktsegment'];

  return (
    <div style={S.section}>
      <div style={S.sectionHead}>
        <div>
          <h2 style={S.h2}>Customers <span style={{ fontSize: 13, fontWeight: 400, color: C.dbGray }}>(Synced from Delta)</span></h2>
          <div style={{ fontSize: 12, color: C.dbGray, marginTop: 3 }}>
            Source: <code>tpch_sync_as.customer</code> — read-only · continuously synced from Delta
          </div>
        </div>
        <span style={{ ...S.badge, background: '#EFF6FF', color: '#1D4ED8', border: '1px solid #BFDBFE' }}>
          Read-Only · {customers.length} rows
        </span>
      </div>

      {error && <div style={S.error}>{error}</div>}

      {loading
        ? <p style={S.info}>Loading from synced table…</p>
        : customers.length === 0
          ? <div style={{ ...S.info, padding: 16, background: C.bg, borderRadius: 8, textAlign: 'center', border: `1px solid ${C.border}` }}>
              Synced table warming up — initial backfill from Delta may take 5–30 min.
            </div>
          : (
            <div style={{ overflowX: 'auto', borderRadius: 8, border: `1px solid ${C.border}` }}>
              <table style={S.table}>
                <thead>
                  <tr>{COLS.map(c => <th key={c} style={S.th}>{c.replace('c_', '')}</th>)}</tr>
                </thead>
                <tbody>
                  {customers.map((r, i) => (
                    <tr key={r.c_custkey}>
                      {COLS.map(c => (
                        <td key={c} style={i % 2 === 0 ? S.td : S.tdEven}>{String(r[c] ?? '')}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
    </div>
  );
}

// ── App root ──────────────────────────────────────────────────────────────────
export default function App() {
  return (
    <div style={S.page}>
      <div style={S.header}>
        <h1 style={S.h1}>Delta ↔ Lakebase Autoscaling Demo</h1>
        <p style={{ margin: '6px 0 0', color: C.dbGray, fontSize: 14 }}>
          Reads from synced Delta snapshot · Writes to Lakebase staging · Forward ETL every 15 min
        </p>
      </div>
      <ArchBanner />
      <OrdersSection />
      <CustomersSection />
    </div>
  );
}
