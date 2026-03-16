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
  page:    { fontFamily: "'Inter', sans-serif", maxWidth: 1200, margin: '0 auto', padding: '24px 16px', background: '#fff', minHeight: '100vh' },
  header:  { borderBottom: `3px solid ${C.dbRed}`, paddingBottom: 16, marginBottom: 24 },
  h1:      { color: C.dbNavy, fontSize: 28, fontWeight: 700, margin: 0 },
  arch:    { background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: 16, marginBottom: 24, fontSize: 13 },
  archTitle: { fontWeight: 700, color: C.dbNavy, marginBottom: 8, fontSize: 14 },
  flow:    { display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' },
  box:     { background: '#fff', border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 12px', fontSize: 12, fontWeight: 600 },
  arrow:   { color: C.dbRed, fontWeight: 700, fontSize: 16 },
  section: { marginBottom: 32 },
  sectionHead: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: `2px solid ${C.dbRed}`, paddingBottom: 6, marginBottom: 12 },
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
};

const btn  = (extra = {}) => ({ ...S.btn, ...extra });
const BTN  = { primary: { background: C.dbRed, color: '#fff' }, success: { background: C.dbGreen, color: '#fff' }, danger: { background: '#DC2626', color: '#fff' }, neutral: { background: '#E5E7EB', color: '#374151' }, outline: { background: '#fff', color: C.dbNavy, border: `1px solid ${C.dbNavy}` } };

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
      <div style={S.archTitle}>How this app works</div>
      <div style={S.flow}>
        <div style={{ ...S.box, background: '#EFF6FF', borderColor: '#BFDBFE' }}>Delta Table<br/><small style={{color:C.dbGray}}>integration_demo.tpch.orders</small></div>
        <span style={S.arrow}>⟶ Reverse ETL (Sync Table) ⟶</span>
        <div style={{ ...S.box, background: '#F0FDF4', borderColor: '#BBF7D0' }}>Lakebase (read-only)<br/><small style={{color:C.dbGray}}>tpch_sync.customer</small></div>
        <span style={{color:C.dbGray, fontSize:12}}>|</span>
        <div style={{ ...S.box, background: '#FFF7ED', borderColor: '#FED7AA' }}>Lakebase (writable)<br/><small style={{color:C.dbGray}}>tpch.orders_staging</small></div>
        <span style={S.arrow}>⟶ Forward ETL (15 min) ⟶</span>
        <div style={{ ...S.box, background: '#EFF6FF', borderColor: '#BFDBFE' }}>Delta Table<br/><small style={{color:C.dbGray}}>integration_demo.tpch.orders</small></div>
      </div>
      <div style={{ marginTop: 10, fontSize: 12, color: C.dbGray }}>
        <b>Read:</b> orders are loaded from <code>tpch.orders_staging</code> (Lakebase Postgres). &nbsp;
        <b>Write/Edit/Delete:</b> changes go directly to Lakebase Postgres. &nbsp;
        <b>Seed:</b> pulls rows from the Delta table into staging so you have data to work with. &nbsp;
        <b>Forward ETL:</b> a Databricks Job runs every 15 min to MERGE staging → Delta.
      </div>
    </div>
  );
}

// ── Empty order template ──────────────────────────────────────────────────────
const EMPTY = { o_orderkey:'', o_custkey:'', o_orderstatus:'O', o_totalprice:'', o_orderdate: new Date().toISOString().slice(0,10), o_orderpriority:'3-MEDIUM', o_clerk:'Clerk#000000001', o_shippriority:'0', o_comment:'' };

// ── Orders Section ────────────────────────────────────────────────────────────
function OrdersSection() {
  const [orders, setOrders]     = useState([]);
  const [total, setTotal]       = useState(0);
  const [search, setSearch]     = useState('');
  const [offset, setOffset]     = useState(0);
  const [loading, setLoading]   = useState(false);
  const [seeding, setSeeding]   = useState(false);
  const [msg, setMsg]           = useState(null); // {type:'error'|'success', text}
  const [showForm, setShowForm] = useState(false);
  const [newOrder, setNewOrder] = useState({ ...EMPTY });
  const [editKey, setEditKey]   = useState(null);
  const [editRow, setEditRow]   = useState({});
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

  useEffect(() => { fetchOrders(); }, [fetchOrders]);

  // Seed
  const seedFromDelta = async () => {
    setSeeding(true); setMsg(null);
    try {
      const r = await api('/api/seed', { method: 'POST' });
      setMsg({ type: 'success', text: `Seeded ${r.seeded} orders from Delta into staging (${r.attempted} attempted).` });
      fetchOrders();
    } catch (e) { setMsg({ type: 'error', text: 'Seed failed: ' + e.message }); }
    finally { setSeeding(false); }
  };

  // Create
  const createOrder = async (e) => {
    e.preventDefault();
    try {
      await api('/api/orders', { method: 'POST', body: JSON.stringify(newOrder) });
      setMsg({ type: 'success', text: `Order ${newOrder.o_orderkey} created in Lakebase.` });
      setShowForm(false); setNewOrder({ ...EMPTY }); fetchOrders();
    } catch (e) { setMsg({ type: 'error', text: e.message }); }
  };

  // Edit
  const startEdit = (row) => { setEditKey(row.o_orderkey); setEditRow({ ...row }); };
  const cancelEdit = () => { setEditKey(null); setEditRow({}); };
  const saveEdit = async () => {
    try {
      await api(`/api/orders/${editKey}`, { method: 'PUT', body: JSON.stringify(editRow) });
      setMsg({ type: 'success', text: `Order ${editKey} updated in Lakebase.` });
      cancelEdit(); fetchOrders();
    } catch (e) { setMsg({ type: 'error', text: e.message }); }
  };

  // Delete
  const deleteOrder = async (key) => {
    if (!window.confirm(`Delete order ${key} from Lakebase staging?`)) return;
    try {
      await api(`/api/orders/${key}`, { method: 'DELETE' });
      setMsg({ type: 'success', text: `Order ${key} deleted from Lakebase.` });
      fetchOrders();
    } catch (e) { setMsg({ type: 'error', text: e.message }); }
  };

  const COLS = ['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority','o_comment'];

  return (
    <div style={S.section}>
      {/* Section header */}
      <div style={S.sectionHead}>
        <div>
          <h2 style={S.h2}>Orders</h2>
          <div style={{ fontSize: 12, color: C.dbGray, marginTop: 2 }}>
            Source: <code>tpch.orders_staging</code> (Lakebase Postgres — writable)
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <span style={{ ...S.badge, background: '#FFF7ED', color: '#C2410C', border: '1px solid #FED7AA' }}>
            {total} rows
          </span>
          <span style={{ ...S.badge, background: '#F0FDF4', color: C.dbGreen, border: '1px solid #BBF7D0' }}>
            CRUD
          </span>
        </div>
      </div>

      {/* Controls */}
      <div style={S.controls}>
        <input style={S.input} placeholder="🔍  Search by comment…" value={search}
          onChange={e => { setSearch(e.target.value); setOffset(0); }} />
        <button style={btn(BTN.primary)} onClick={() => { setShowForm(v => !v); setMsg(null); }}>
          {showForm ? '✕ Cancel' : '+ New Order'}
        </button>
        <button style={btn(BTN.neutral)} onClick={fetchOrders}>↺ Refresh</button>
        <button style={btn({ ...BTN.outline, opacity: seeding ? 0.6 : 1 })} onClick={seedFromDelta} disabled={seeding}>
          {seeding ? 'Seeding…' : '⬇ Seed 100 from Delta'}
        </button>
      </div>

      {/* Message */}
      {msg && <div style={msg.type === 'error' ? S.error : S.success}>{msg.text}</div>}

      {/* Create form */}
      {showForm && (
        <form onSubmit={createOrder} style={S.form}>
          {Object.keys(EMPTY).map(k => (
            <label key={k} style={S.label}>
              <span style={S.labelTxt}>{k.replace('o_', '')}</span>
              <input style={S.formInput} value={newOrder[k]} required={['o_orderkey','o_custkey','o_comment'].includes(k)}
                onChange={e => setNewOrder(v => ({ ...v, [k]: e.target.value }))} />
            </label>
          ))}
          <div style={{ gridColumn: 'span 3' }}>
            <button type="submit" style={btn(BTN.success)}>✓ Save Order</button>
          </div>
        </form>
      )}

      {/* Table */}
      {loading
        ? <p style={S.info}>Loading from Lakebase…</p>
        : (
          <>
            <div style={S.info}>{total} total rows · showing {offset + 1}–{Math.min(offset + LIMIT, total)}</div>
            <div style={{ overflowX: 'auto', borderRadius: 8, border: `1px solid ${C.border}` }}>
              <table style={S.table}>
                <thead>
                  <tr>
                    {COLS.map(c => <th key={c} style={S.th}>{c.replace('o_','')}</th>)}
                    <th style={{ ...S.th, textAlign: 'center' }}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.length === 0 && (
                    <tr><td colSpan={COLS.length + 1} style={{ ...S.td, color: C.dbGray, textAlign: 'center', padding: 24 }}>
                      No orders found. Use "Seed 100 from Delta" to load sample data.
                    </td></tr>
                  )}
                  {orders.map((row, i) => {
                    const isEdit = editKey === row.o_orderkey;
                    return (
                      <tr key={row.o_orderkey}>
                        {COLS.map(c => (
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
                                <button style={btn({ ...BTN.success, marginRight: 4, padding: '4px 10px', fontSize: 12 })} onClick={saveEdit}>✓ Save</button>
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

            {/* Pagination */}
            <div style={S.pager}>
              <button style={btn({ ...BTN.neutral, padding: '5px 12px' })} disabled={offset === 0}
                onClick={() => setOffset(v => Math.max(0, v - LIMIT))}>← Prev</button>
              <span>Page {Math.floor(offset / LIMIT) + 1} of {Math.ceil(total / LIMIT) || 1}</span>
              <button style={btn({ ...BTN.neutral, padding: '5px 12px' })} disabled={offset + LIMIT >= total}
                onClick={() => setOffset(v => v + LIMIT)}>Next →</button>
            </div>
          </>
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
          <h2 style={S.h2}>Customers</h2>
          <div style={{ fontSize: 12, color: C.dbGray, marginTop: 2 }}>
            Source: <code>tpch_sync.customer</code> (Lakebase — synced read-only from Delta)
          </div>
        </div>
        <span style={{ ...S.badge, background: '#EFF6FF', color: '#1D4ED8', border: '1px solid #BFDBFE' }}>
          Read-Only · {customers.length} rows
        </span>
      </div>

      {error && <div style={S.error}>{error}</div>}

      {loading
        ? <p style={S.info}>Loading from Lakebase…</p>
        : customers.length === 0
          ? <div style={{ ...S.info, padding: '16px', background: C.bg, borderRadius: 8, textAlign: 'center', border: `1px solid ${C.border}` }}>
              Synced table is warming up — initial backfill from Delta may take 5–30 min.
            </div>
          : (
            <div style={{ overflowX: 'auto', borderRadius: 8, border: `1px solid ${C.border}` }}>
              <table style={S.table}>
                <thead>
                  <tr>{COLS.map(c => <th key={c} style={S.th}>{c.replace('c_','')}</th>)}</tr>
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
        <h1 style={S.h1}>Delta ↔ Lakebase Integration Demo</h1>
        <p style={{ margin: '6px 0 0', color: C.dbGray, fontSize: 14 }}>
          Full CRUD on Lakebase Postgres · Synced tables from Delta · Forward ETL every 15 min
        </p>
      </div>
      <ArchBanner />
      <OrdersSection />
      <CustomersSection />
    </div>
  );
}
