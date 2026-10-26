import { getDb, plano } from '@/lib/mongo';
import { resumen, alertasAbiertas, ultimos } from '@/lib/queries';
import TorreClient from '@/components/TorreClient';
export const dynamic = 'force-dynamic';

export default async function Torre() {
  const db = await getDb();
  const [r, a, f] = await Promise.all([resumen(db), alertasAbiertas(db), ultimos(db)]);
  return <TorreClient inicial={plano({ resumen: r, alertas: a, feed: f })} />;
}
