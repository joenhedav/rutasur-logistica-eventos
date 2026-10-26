import { getDb } from '@/lib/mongo';
import { resumen } from '@/lib/queries';
export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';
export async function GET() {
  return Response.json(await resumen(await getDb()), { headers: { 'Cache-Control': 'no-store' } });
}
