import { Db, MongoClient } from 'mongodb';

let client = null;
const mongoDbUrl = process.env['MONGODB_URL'] || 'mongodb://localhost:27017/timeseries-test';

export async function getMongoDb(): Promise<Db> {
  if (!client) {
    const options = {minSize: 1};
    client = await MongoClient.connect(mongoDbUrl, options as any);
  }
  return await client.db();
}
