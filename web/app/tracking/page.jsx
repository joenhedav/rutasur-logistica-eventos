import { getDb } from '@/lib/mongo';
import Link from 'next/link';
export const dynamic = 'force-dynamic';

export default async function Buscar() {
  const recientes = await (await getDb()).collection('envios').find({ estado: { $exists: true } }).sort({ ultimoEventoEn: -1 }).limit(6).project({ shipmentId: 1 }).toArray();
  return (
    <div className="pub">
      <h1>Seguí tu envío</h1>
      <p className="sub">Ingresá el código que te enviaron por correo.</p>
      <form className="buscar" action="/tracking/ir" method="get">
        <input name="id" placeholder="Ej.: RS-0000001" aria-label="Código de envío" required />
        <button>Buscar envío</button>
      </form>
      {recientes.length > 0 && <p className="meta" style={{ marginTop: 20 }}>Envíos recientes: {recientes.map((r) => <span key={r.shipmentId}><Link href={`/tracking/${r.shipmentId}`}>{r.shipmentId}</Link>{' '}</span>)}</p>}
    </div>
  );
}
