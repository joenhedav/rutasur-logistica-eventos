import { getDb, plano } from '@/lib/mongo';
import { publico } from '@/lib/shape';
import TrackingClient from '@/components/TrackingClient';
export const dynamic = 'force-dynamic';

export default async function Tracking({ params }) {
  const doc = await (await getDb()).collection('envios').findOne({ shipmentId: params.id });
  return <TrackingClient id={params.id} inicial={doc ? plano(publico(doc)) : null} />;
}
