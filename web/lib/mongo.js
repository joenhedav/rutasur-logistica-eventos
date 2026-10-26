import { MongoClient } from 'mongodb';

// Conexión perezosa y compartida (el build no necesita MongoDB). La interfaz SOLO lee de MongoDB.
export function getDb() {
  if (!globalThis.__rutasurDb) {
    const url = process.env.MONGO_URL || 'mongodb://localhost:27017/rutasur?directConnection=true';
    globalThis.__rutasurDb = new MongoClient(url).connect().then((c) => c.db(process.env.MONGO_DB || 'rutasur'))
      .catch((e) => { globalThis.__rutasurDb = null; throw e; });
  }
  return globalThis.__rutasurDb;
}
export const plano = (x) => JSON.parse(JSON.stringify(x));
