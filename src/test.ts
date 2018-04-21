import { getMongoDb } from './mongodb';
import { TimeSeriesBucket } from './time-series';
import { showBytes } from './helpers';

async function testDb() {

  const db = await getMongoDb();
  console.log('db name', db.databaseName);

  const bucket = new TimeSeriesBucket('minuteBucket', 60000, [1000], [
    'v'
  ]);

  const startTs = new Date('2018-01-01T00:00:00Z');
  const endTs = new Date('2018-12-31T23:59:00Z');
  const collection = db.collection(bucket.name);

  let lastDay = null;
  let days = 0;
  for (let i = startTs.getTime(); i < endTs.getTime(); i += 60000) {
    await bucket.add(db, new Date(i), {v: Math.round(Math.random() * 100) + 1});
    const today = Math.floor(i / 86400000) * 86400000;
    if (lastDay !== today) {
      days++;
      const stats = await collection.stats();
      console.log(`day ${days} size: ${showBytes(stats.size)}, storage: ${showBytes(stats.storageSize)}, index: ${showBytes(stats.totalIndexSize)}, avg: ${stats.avgObjSize}`);
    }
    lastDay = today;
  }

  const stats = await collection.stats();
  console.log(`size: ${showBytes(stats.size)}, storage: ${showBytes(stats.storageSize)}, index: ${showBytes(stats.totalIndexSize)}, avg: ${stats.avgObjSize}`);

}

testDb()
  .then(() => {
    process.exit(0);
  })
  .catch(err => {
    console.error(err);
    process.exit(1);
  });
