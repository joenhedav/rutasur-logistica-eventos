export function GET(req) {
  const id = (new URL(req.url).searchParams.get('id') || '').trim();
  return new Response(null, { status: 302, headers: { Location: id ? `/tracking/${encodeURIComponent(id)}` : '/tracking' } });
}
