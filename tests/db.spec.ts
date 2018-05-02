import { getMongoDb } from '../src/mongodb';
import { TimeSeriesBucketSimple } from '../src/time-series-bucket-simple';
import * as fs from 'fs';
import { showBytes } from '../src/helpers';
import { Logger } from 'mongodb';
import { TimeSeriesBucketVariant } from '../src/time-series-bucket-variant';

const protocol = fs.createWriteStream('protocol.log', {flags: 'a'});

const startTs = new Date('2018-01-01T00:00:00Z');
const endTs = new Date('2018-12-31T23:59:00Z');

describe('DB Test', function () {
  let db;

  const hourBucket = new TimeSeriesBucketSimple('hourBucketSimple', 3600000, 1000, ['v']);
  const minuteBucket = new TimeSeriesBucketSimple('minuteBucketSimple', 60000, 1000, ['v']);
  const minuteBucketV = new TimeSeriesBucketVariant('minuteBucketVariant', 60000, 1000, ['v']);


  beforeAll(async function () {
    db = await getMongoDb();
  });

  afterAll(async function () {
    await db.close();
  });

  it('should insert hour bucket', async function () {
    await insertData('hour bucket', db, hourBucket, startTs, endTs, 30000);
  }, 600000);

  it('should query hour bucket last 24h in seconds', async function () {
    await queryTest('hour bucket 24h sec', db, hourBucket, new Date(endTs.getTime() - 86400000), endTs, 1000);
    await queryTest2('hour bucket 24h sec2', db, hourBucket, new Date(endTs.getTime() - 86400000), endTs, 1000);
  });

  it('should query hour bucket last 48h in minutes', async function () {
    await queryTest('hour bucket 48h min', db, hourBucket, new Date(endTs.getTime() - 86400000 * 2), endTs, 60000);
    await queryTest2('hour bucket 48h sec2', db, hourBucket, new Date(endTs.getTime() - 86400000 * 2), endTs, 60000);
  });

  it('should query hour bucket last 30 days in hours', async function () {
    // Logger.setLevel('debug');
    await queryTest('hour bucket 30d hour', db, hourBucket, new Date(endTs.getTime() - 86400000 * 30), endTs, 3600000);
    await queryTest2('hour bucket 30d hour', db, hourBucket, new Date(endTs.getTime() - 86400000 * 30), endTs, 3600000);
  }, 60000);


  // minute bucket simple
  it('should insert minute bucket', async function () {
    await insertData('minute bucket', db, minuteBucket, startTs, endTs, 30000);
  }, 600000);

  it('should query minute bucket last 24h in seconds', async function () {
    await queryTest('minute bucket 24h sec', db, minuteBucket, new Date(endTs.getTime() - 86400000), endTs, 1000);
    await queryTest2('minute bucket 24h sec2', db, minuteBucket, new Date(endTs.getTime() - 86400000), endTs, 1000);
  });

  it('should query minute bucket last 48h in minutes', async function () {
    await queryTest('minute bucket 48h min', db, minuteBucket, new Date(endTs.getTime() - 86400000 * 2), endTs, 60000);
    await queryTest2('minute bucket 48h sec2', db, minuteBucket, new Date(endTs.getTime() - 86400000 * 2), endTs, 60000);
  });

  it('should query minute bucket last 30 days in hours', async function () {
    // Logger.setLevel('debug');
    await queryTest('minute bucket 30d hour', db, minuteBucket, new Date(endTs.getTime() - 86400000 * 30), endTs, 3600000);
    await queryTest2('minute bucket 30d hour', db, minuteBucket, new Date(endTs.getTime() - 86400000 * 30), endTs, 3600000);
  }, 60000);

  // minute bucket V
  it('should insert minute bucketV', async function () {
    await insertData('minute bucketV', db, minuteBucketV, startTs, endTs, 30000);
  }, 600000);

  it('should query minute bucketV last 24h in seconds', async function () {
    await queryTest('minute bucketV 24h sec', db, minuteBucketV, new Date(endTs.getTime() - 86400000), endTs, 1000);
  });

  it('should query minute bucketV last 48h in minutes', async function () {
    await queryTest('minute bucketV 48h min', db, minuteBucketV, new Date(endTs.getTime() - 86400000 * 2), endTs, 60000);
  });

  it('should query minute bucketV last 30 days in hours', async function () {
    // Logger.setLevel('debug');
    await queryTest('minute bucketV 30d hour', db, minuteBucketV, new Date(endTs.getTime() - 86400000 * 30), endTs, 3600000);
  }, 60000);


});

async function insertData(label, db, bucket, startTs, endTs, insertEvery) {
  const collection = db.collection(bucket.name);

  const iterations = Math.floor((endTs.getTime() - startTs.getTime()) / insertEvery);

  const startTime = new Date().getTime();
  let lastDay = null;
  let days = 0;
  let updates = 0;
  for (let i = startTs.getTime(); i < endTs.getTime(); i += insertEvery) {
    await bucket.add(db, new Date(i), {v: Math.round(Math.random() * 100) + 1});
    updates++;
    const today = Math.floor(i / 86400000 / 7) * 86400000 * 7;
    if (lastDay !== today) {
      days++;
      const stats = await collection.stats();
      printValues(`week ${days}`, {
        perc: (updates / iterations * 100).toFixed(0) + '%',
        size: showBytes(stats.size),
        storage: showBytes(stats.storageSize),
        index: showBytes(stats.totalIndexSize),
        avg: stats.avgObjSize
      });
    }
    lastDay = today;
  }

  const duration = new Date().getTime() - startTime;

  const stats = await collection.stats();
  printProtocol(label, {
    updates: updates,
    size: showBytes(stats.size),
    storage: showBytes(stats.storageSize),
    index: showBytes(stats.totalIndexSize),
    avg: stats.avgObjSize,
    dur: duration
  });
}

async function queryTest(label, db, bucket, startTs, endTs, aggregate) {
  const startTime = new Date().getTime();

  const result = await bucket.findAggregates(db, aggregate, startTs, endTs);

  const duration = new Date().getTime() - startTime;
  printProtocol(label, {
    received: result.length,
    dur: duration
  });

}

async function queryTest2(label, db, bucket, startTs, endTs, aggregate) {
  const startTime = new Date().getTime();

  const result = await bucket.findAggregates2(db, aggregate, startTs, endTs);

  const duration = new Date().getTime() - startTime;
  printProtocol(label + ' [2]', {
    received: result.length,
    dur: duration
  });

}

function printValues(prefix, data) {
  const text = [];
  Object.keys(data).forEach(key => {
    text.push(key + ': ' + data[key]);
  });
  const log = prefix + ' ' + text.join(' ');
  console.log(log);
  return log;
}

function printProtocol(prefix, data) {
  const log = printValues(prefix, data);
  protocol.write(new Date().toISOString() + ' ' + log + '\n');
}
