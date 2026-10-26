// Log JSON de una línea. Los consumidores incluyen eventId, partition y offset (observabilidad).
module.exports = (svc) => (msg, extra = {}) =>
  console.log(JSON.stringify({ t: new Date().toISOString(), svc, msg, ...extra }));
