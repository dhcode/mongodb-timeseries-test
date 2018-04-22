import { getMongoDb } from './mongodb';
import { TimeSeriesBucketExtended } from './time-series';
import { showBytes } from './helpers';
import { TimeSeriesBucketSimple } from './time-series-bucket-simple';

const startTs = new Date('2018-01-01T00:00:00Z');
const endTs = new Date('2018-12-31T23:59:00Z');

const testConfigs = [
  // {
  //   label: 'Nested minute - second precision',
  //   bucket: new TimeSeriesBucketExtended('minuteSecBucketNested', 60000, [1000], ['v']),
  //   startTs: startTs,
  //   endTs: endTs,
  //   insertEvery: 60000
  // },
  // {
  //   label: 'Nested hour - second precision',
  //   bucket: new TimeSeriesBucketExtended('hourSecBucketNested', 3600000, [1000], ['v']),
  //   startTs: startTs,
  //   endTs: endTs,
  //   insertEvery: 60000
  // },
  // {
  //   label: 'Nested hour - minute - second precision',
  //   bucket: new TimeSeriesBucketExtended('hourMinSecBucketNested', 3600000, [60000, 1000], ['v']),
  //   startTs: startTs,
  //   endTs: endTs,
  //   insertEvery: 60000
  // },
  // {
  //   label: 'Simple hour - second precision',
  //   bucket: new TimeSeriesBucketSimple('hourBucketSimple', 3600000, 1000, ['v']),
  //   startTs: startTs,
  //   endTs: endTs,
  //   insertEvery: 60000
  // },
  {
    label: 'Simple minute - second precision',
    bucket: new TimeSeriesBucketSimple('minuteBucketSimple', 60000, 1000, ['v']),
    startTs: startTs,
    endTs: endTs,
    insertEvery: 30000
  }
];

async function testConfig(config) {
  const db = await getMongoDb();
  const bucket = config.bucket;

  const startTs = new Date('2018-01-01T00:00:00Z');
  const endTs = new Date('2018-12-31T23:59:00Z');
  const collection = db.collection(bucket.name);

  const iterations = Math.floor((endTs.getTime() - startTs.getTime()) / config.insertEvery);

  const startTime = new Date().getTime();
  let lastDay = null;
  let days = 0;
  let updates = 0;
  for (let i = startTs.getTime(); i < endTs.getTime(); i += config.insertEvery) {
    await bucket.add(db, new Date(i), {v: Math.round(Math.random() * 100) + 1});
    updates++;
    const today = Math.floor(i / 86400000 / 7) * 86400000 * 7;
    if (lastDay !== today) {
      days++;
      const stats = await collection.stats();
      printValues(`day ${days}`, {
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
  printValues('', {
    updates: updates,
    size: showBytes(stats.size),
    storage: showBytes(stats.storageSize),
    index: showBytes(stats.totalIndexSize),
    avg: stats.avgObjSize,
    dur: duration
  });

  await collection.drop();

  return stats;
}

function printValues(prefix, data) {
  const text = [];
  Object.keys(data).forEach(key => {
    text.push(key + ': ' + data[key]);
  });
  console.log(prefix + ' ' + text.join(' '));
}

async function testDb() {

  for (let i = 0; i < testConfigs.length; i++) {
    const config = testConfigs[i];
    console.log(`Start ${config.label}`);
    await testConfig(config);
    console.log(`Done ${config.label}`);
  }
}

testDb()
  .then(() => {
    process.exit(0);
  })
  .catch(err => {
    console.error(err);
    process.exit(1);
  });
