import { getDb, plano } from '@/lib/mongo';
import DetalleClient from '@/components/DetalleClient';
export const dynamic = 'force-dynamic';

export default async function Detalle({ params }) {
  const doc = await (await getDb()).collection('envios').findOne({ shipmentId: params.id });
  return <DetalleClient id={params.id} inicial={doc ? plano(doc) : null} />;
}
