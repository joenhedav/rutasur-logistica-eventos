import { getDb } from '@/lib/mongo';
import { MOTIVO, fmtDia, franjaTxt } from '@/lib/fmt';
export const dynamic = 'force-dynamic';

const COLOR = { AUSENTE: '#0a5c7a', DIRECCION_INCORRECTA: '#c25e00', RECHAZADO: '#7a4fb3', ZONA_INACCESIBLE: '#6b7c86' };
const pct = (x) => (x == null ? '—' : `${(x * 100).toFixed(1)}%`);
const tinte = (tasa) => (tasa == null ? 'transparent' : `rgba(180, 35, 24, ${Math.min(0.55, Math.max(0, (1 - tasa) * 1.4))})`);

export default async function Analitica({ searchParams }) {
  const db = await getDb();
  const col = db.collection('metricas_diarias');
  const todos = await col.find({}).project({ fecha: 1, zonaId: 1, hubId: 1 }).toArray();
  const zonas = [...new Set(todos.map((d) => d.zonaId))].sort();
  const hubs = [...new Set(todos.map((d) => d.hubId))].sort();
  const fechas = [...new Set(todos.map((d) => d.fecha))].sort();
  const { zona = '', hub = '', desde = '', hasta = '' } = searchParams;
  const q = {};
  if (zona) q.zonaId = zona;
  if (hub) q.hubId = hub;
  if (desde || hasta) q.fecha = { ...(desde && { $gte: desde }), ...(hasta && { $lte: hasta }) };
  const docs = await col.find(q).toArray(); // pocas filas: ya vienen pre-agregadas por día, zona y franja

  const zf = {}, dia = {}, mot = {}, sla = {}, th = {};
  for (const d of docs) {
    const pi = d.primerIntento || { total: 0, entregados: 0 };
    const a = ((zf[d.zonaId] ||= {})[d.franja] ||= { t: 0, e: 0 }); a.t += pi.total; a.e += pi.entregados;
    const b = (dia[d.fecha] ||= { t: 0, e: 0 }); b.t += pi.total; b.e += pi.entregados;
    for (const [k, v] of Object.entries(d.motivos || {})) { const m = (mot[d.zonaId] ||= {}); m[k] = (m[k] || 0) + v; }
    const s = ((sla[d.hubId] ||= {})[d.fecha] ||= { e: 0, t: 0 }); s.e += d.sla?.entregados || 0; s.t += d.sla?.tarde || 0;
    if (d.tiempoHub) { const h = ((th[d.hubId] ||= {})[d.fecha] ||= { s: 0, n: 0 }); h.s += d.tiempoHub.sumMs; h.n += d.tiempoHub.n; }
  }
  const tasa = (x) => (x && x.t ? x.e / x.t : null);
  let peor = null;
  for (const [z, fr] of Object.entries(zf)) for (const [f, x] of Object.entries(fr)) if (x.t >= 30 && (!peor || tasa(x) < peor.tasa)) peor = { z, f, tasa: tasa(x), t: x.t };
  const dias = Object.keys(dia).sort();
  const zs = Object.keys(zf).sort();
  const hs = [...new Set([...Object.keys(sla), ...Object.keys(th)])].sort();
  const sumHub = (obj, h) => Object.values(obj[h] || {}).reduce((a, x) => ({ e: a.e + x.e, t: a.t + x.t }), { e: 0, t: 0 });
  const horas = (h) => { const v = Object.values(th[h] || {}).reduce((a, x) => ({ s: a.s + x.s, n: a.n + x.n }), { s: 0, n: 0 }); return v.n ? v.s / v.n / 3600e3 : null; };
  const hayCuarto = Object.keys(th).length > 0;

  return (
    <>
      <h1>Analítica de mejora</h1>
      <p className="sub">Cuatro indicadores calculados con incrementos sobre los eventos, sin recorrer envíos.</p>
      <form className="filtros" method="get">
        <label>Zona<select name="zona" defaultValue={zona}><option value="">Todas</option>{zonas.map((z) => <option key={z}>{z}</option>)}</select></label>
        <label>Hub<select name="hub" defaultValue={hub}><option value="">Todos</option>{hubs.map((h) => <option key={h}>{h}</option>)}</select></label>
        <label>Desde<select name="desde" defaultValue={desde}><option value="">Inicio</option>{fechas.map((f) => <option key={f} value={f}>{fmtDia(f)}</option>)}</select></label>
        <label>Hasta<select name="hasta" defaultValue={hasta}><option value="">Hoy</option>{fechas.map((f) => <option key={f} value={f}>{fmtDia(f)}</option>)}</select></label>
        <button>Aplicar filtros</button>
      </form>
      {docs.length === 0 ? <p className="panel vacio">Todavía no hay métricas. Dispará escenarios desde el Panel de demo o dejá correr el flujo continuo unos minutos.</p> : (<>
        {peor && <div className="hallazgo"><b>Dónde fallamos más:</b> la zona {peor.z} en la franja de la {franjaTxt(peor.f)} entrega solo el {pct(peor.tasa)} en el primer intento ({peor.t} intentos). {peor.f === 'MANANA' ? 'Oportunidad: ofrecer la franja vespertina en esa zona.' : ''}</div>}
        <div className="grid2">
          <section className="panel">
            <h2>1 · Entrega en primer intento por zona y franja</h2>
            <table><thead><tr><th>Zona</th><th className="n">Mañana</th><th className="n">Tarde</th></tr></thead><tbody>
              {zs.map((z) => <tr key={z}><td>{z}</td>{['MANANA', 'TARDE'].map((f) => { const t = tasa(zf[z][f]); return <td key={f} className="n" style={{ background: tinte(t) }}>{pct(t)}</td>; })}</tr>)}
            </tbody></table>
            <h3 style={{ marginTop: 16 }}>Por día</h3>
            <table><tbody>{dias.map((d) => <tr key={d}><td>{fmtDia(d)}</td><td className="n" style={{ background: tinte(tasa(dia[d])) }}>{pct(tasa(dia[d]))}</td><td className="n meta">{dia[d].t} intentos</td></tr>)}</tbody></table>
          </section>
          <section className="panel">
            <h2>2 · Por qué fallamos</h2>
            {Object.keys(mot).sort().map((z) => { const m = mot[z]; const tot = Object.values(m).reduce((a, b) => a + b, 0) || 1; return (
              <div key={z} style={{ marginBottom: 12 }}><div className="meta">Zona {z} · {tot} fallos</div>
                <div className="barra-h" role="img" aria-label={`Motivos de fallo zona ${z}`}>{Object.keys(COLOR).map((k) => <i key={k} style={{ width: `${((m[k] || 0) / tot) * 100}%`, background: COLOR[k] }} title={`${MOTIVO[k]}: ${m[k] || 0}`} />)}</div></div>); })}
            <div className="leyenda">{Object.keys(COLOR).map((k) => <span key={k}><i style={{ background: COLOR[k] }} />{MOTIVO[k]}</span>)}</div>
          </section>
        </div>
        <div className="grid2">
          <section className="panel">
            <h2>3 · Cumplimiento de SLA por hub</h2>
            <table><thead><tr><th>Hub</th><th className="n">Entregados</th><th className="n">A tiempo</th></tr></thead><tbody>
              {Object.keys(sla).sort().map((h) => { const s = sumHub(sla, h); const c = s.e ? 1 - s.t / s.e : null; return <tr key={h}><td>{h}</td><td className="n">{s.e}</td><td className="n" style={{ background: tinte(c) }}>{pct(c)}</td></tr>; })}
            </tbody></table>
            <h3 style={{ marginTop: 16 }}>Por día</h3>
            <table><thead><tr><th>Hub</th>{dias.map((d) => <th key={d} className="n">{fmtDia(d)}</th>)}</tr></thead><tbody>
              {Object.keys(sla).sort().map((h) => <tr key={h}><td>{h}</td>{dias.map((d) => { const x = sla[h][d]; const c = x && x.e ? 1 - x.t / x.e : null; return <td key={d} className="n" style={{ background: tinte(c) }}>{pct(c)}</td>; })}</tr>)}
            </tbody></table>
          </section>
          <section className="panel">
            <h2>4 · Tiempo medio en el hub (ingreso → reparto)</h2>
            {!hayCuarto ? <p className="vacio">Este indicador todavía no está activo. Para completarlo con todo el histórico sin tocar los demás servicios: <pre>docker compose --profile replay up -d analitica-tiempo-hub</pre></p> : (
              <table><thead><tr><th>Hub</th><th className="n">Promedio</th>{dias.map((d) => <th key={d} className="n">{fmtDia(d)}</th>)}</tr></thead><tbody>
                {hs.map((h) => <tr key={h}><td>{h}</td><td className="n"><b>{horas(h) == null ? '—' : horas(h).toFixed(1) + ' h'}</b></td>{dias.map((d) => { const x = th[h]?.[d]; return <td key={d} className="n">{x && x.n ? (x.s / x.n / 3600e3).toFixed(1) + ' h' : '—'}</td>; })}</tr>)}
              </tbody></table>)}
          </section>
        </div>
      </>)}
    </>
  );
}
