import React, { useState, useEffect, useCallback, useRef } from 'react';
import ReactMarkdown from 'react-markdown';

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

// ── Genie Chat Widget ─────────────────────────────────────────────────────────
const PREFILLED = [
  "What are the top 10 customers by total order value?",
  "Show revenue trend by month for the last year",
  "Which order priorities have the highest average total price?",
  "What are the top 5 nations by number of orders?",
  "Show me orders with status 'F' and their total value",
];

function GenieChat() {
  const [open, setOpen]           = useState(false);
  const [expanded, setExpanded]   = useState(false);
  const [messages, setMessages]   = useState([]);
  const [input, setInput]         = useState('');
  const [loading, setLoading]     = useState(false);
  const [convId, setConvId]       = useState(null);
  const bottomRef                 = useRef(null);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const send = useCallback(async (text) => {
    const q = text || input.trim();
    if (!q) return;
    setMessages(m => [...m, { role: 'user', text: q }]);
    setInput('');
    setLoading(true);
    try {
      const res = await api('/api/genie/message', {
        method: 'POST',
        body: JSON.stringify({ question: q, conversation_id: convId }),
      });
      setConvId(res.conversation_id);
      setMessages(m => [...m, { role: 'genie', text: res.answer }]);
    } catch (e) {
      setMessages(m => [...m, { role: 'error', text: e.message }]);
    } finally { setLoading(false); }
  }, [input, convId]);

  const panelW = expanded ? '100vw' : 420;
  const panelH = expanded ? '100vh' : 520;
  const panelStyle = {
    position: 'fixed', bottom: expanded ? 0 : 90, right: expanded ? 0 : 24,
    width: panelW, height: panelH, zIndex: 1000,
    background: '#fff', borderRadius: expanded ? 0 : 16,
    boxShadow: '0 8px 40px rgba(0,0,0,0.18)',
    display: 'flex', flexDirection: 'column',
    border: `1px solid ${C.border}`,
    overflow: 'hidden',
    transition: 'all 0.25s ease',
  };

  return (
    <>
      {/* Floating button */}
      {!open && (
        <button onClick={() => setOpen(true)} style={{
          position: 'fixed', bottom: 28, right: 28, zIndex: 999,
          background: 'linear-gradient(135deg, #FF3621 0%, #FF6B35 100%)',
          color: '#fff', border: 'none', borderRadius: 50,
          padding: '14px 22px', fontSize: 15, fontWeight: 700,
          cursor: 'pointer', boxShadow: '0 4px 20px rgba(255,54,33,0.45)',
          display: 'flex', alignItems: 'center', gap: 8,
          letterSpacing: 0.3,
        }}>
          <span style={{ fontSize: 20 }}>✦</span> Ask Genie
        </button>
      )}

      {/* Chat panel */}
      {open && (
        <div style={panelStyle}>
          {/* Header */}
          <div style={{
            background: 'linear-gradient(135deg, #1B3139 0%, #2D5A6B 100%)',
            padding: '14px 16px', display: 'flex', alignItems: 'center', gap: 10,
          }}>
            <span style={{ fontSize: 22 }}>✦</span>
            <div style={{ flex: 1 }}>
              <div style={{ color: '#fff', fontWeight: 700, fontSize: 15 }}>Ask Genie</div>
              <div style={{ color: '#94A3B8', fontSize: 11 }}>TPC-H Analytics · AI/BI Genie</div>
            </div>
            <button onClick={() => setExpanded(e => !e)} title={expanded ? 'Collapse' : 'Expand'} style={{
              background: 'rgba(255,255,255,0.12)', border: 'none', color: '#fff',
              borderRadius: 6, padding: '4px 8px', cursor: 'pointer', fontSize: 14,
            }}>{expanded ? '⊡' : '⛶'}</button>
            <button onClick={() => { setOpen(false); setExpanded(false); }} style={{
              background: 'rgba(255,255,255,0.12)', border: 'none', color: '#fff',
              borderRadius: 6, padding: '4px 8px', cursor: 'pointer', fontSize: 16,
            }}>✕</button>
          </div>

          {/* Messages */}
          <div style={{ flex: 1, overflowY: 'auto', padding: 14, display: 'flex', flexDirection: 'column', gap: 10 }}>
            {messages.length === 0 && (
              <div>
                <p style={{ color: C.dbGray, fontSize: 13, margin: '0 0 12px' }}>
                  Ask anything about TPC-H orders, customers, suppliers, or revenue.
                </p>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {PREFILLED.map((q, i) => (
                    <button key={i} onClick={() => send(q)} style={{
                      textAlign: 'left', background: C.bg, border: `1px solid ${C.border}`,
                      borderRadius: 8, padding: '8px 12px', fontSize: 12, cursor: 'pointer',
                      color: C.dbNavy, lineHeight: 1.4,
                    }}>{q}</button>
                  ))}
                </div>
              </div>
            )}
            {messages.map((m, i) => (
              <div key={i} style={{
                alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
                maxWidth: '85%',
              }}>
                <div style={{
                  background: m.role === 'user' ? '#1B3139' : m.role === 'error' ? '#FEF2F2' : C.bg,
                  color: m.role === 'user' ? '#fff' : m.role === 'error' ? C.dbRed : '#111',
                  borderRadius: m.role === 'user' ? '14px 14px 4px 14px' : '14px 14px 14px 4px',
                  padding: '9px 13px', fontSize: 13, lineHeight: 1.6,
                  border: m.role === 'genie' ? `1px solid ${C.border}` : 'none',
                }}>
                  {m.role === 'genie'
                    ? <ReactMarkdown components={{
                        p:      ({children}) => <p style={{margin:'4px 0'}}>{children}</p>,
                        strong: ({children}) => <strong style={{fontWeight:700}}>{children}</strong>,
                        ul:     ({children}) => <ul style={{margin:'4px 0',paddingLeft:18}}>{children}</ul>,
                        ol:     ({children}) => <ol style={{margin:'4px 0',paddingLeft:18}}>{children}</ol>,
                        li:     ({children}) => <li style={{margin:'2px 0'}}>{children}</li>,
                        table:  ({children}) => <table style={{borderCollapse:'collapse',width:'100%',margin:'6px 0',fontSize:12}}>{children}</table>,
                        th:     ({children}) => <th style={{background:'#1B3139',color:'#fff',padding:'5px 8px',textAlign:'left',fontWeight:600}}>{children}</th>,
                        td:     ({children}) => <td style={{padding:'4px 8px',borderBottom:`1px solid ${C.border}`}}>{children}</td>,
                        code:   ({children}) => <code style={{background:'#E5E7EB',borderRadius:3,padding:'1px 4px',fontSize:11,fontFamily:'monospace'}}>{children}</code>,
                        h1:     ({children}) => <h3 style={{margin:'6px 0 2px',fontSize:14,color:C.dbNavy}}>{children}</h3>,
                        h2:     ({children}) => <h4 style={{margin:'6px 0 2px',fontSize:13,color:C.dbNavy}}>{children}</h4>,
                        h3:     ({children}) => <h4 style={{margin:'4px 0 2px',fontSize:13,color:C.dbNavy}}>{children}</h4>,
                      }}>{m.text}</ReactMarkdown>
                    : m.text}
                </div>
              </div>
            ))}
            {loading && (
              <div style={{ alignSelf: 'flex-start', color: C.dbGray, fontSize: 13, padding: '4px 8px' }}>
                Genie is thinking…
              </div>
            )}
            <div ref={bottomRef} />
          </div>

          {/* Input */}
          <div style={{ borderTop: `1px solid ${C.border}`, padding: '10px 12px', display: 'flex', gap: 8 }}>
            <input
              value={input} onChange={e => setInput(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && !e.shiftKey && send()}
              placeholder="Ask about TPC-H data…"
              style={{ flex: 1, padding: '9px 12px', borderRadius: 8, border: `1px solid ${C.border}`, fontSize: 13, outline: 'none' }}
            />
            <button onClick={() => send()} disabled={loading} style={{
              background: loading ? '#E5E7EB' : 'linear-gradient(135deg, #FF3621, #FF6B35)',
              color: loading ? C.dbGray : '#fff', border: 'none', borderRadius: 8,
              padding: '9px 16px', cursor: loading ? 'not-allowed' : 'pointer', fontWeight: 700, fontSize: 13,
            }}>Send</button>
          </div>
        </div>
      )}
    </>
  );
}

// ── App root ──────────────────────────────────────────────────────────────────
export default function App() {
  return (
    <div style={S.page}>
      <div style={S.header}>
        <h1 style={S.h1}>LakeSync</h1>
        <p style={{ margin: '6px 0 0', color: C.dbGray, fontSize: 14 }}>
          Full CRUD on Lakebase Postgres · Synced tables from Delta · Forward ETL every 15 min
        </p>
      </div>
      <ArchBanner />
      <OrdersSection />
      <CustomersSection />
      <GenieChat />
    </div>
  );
}
