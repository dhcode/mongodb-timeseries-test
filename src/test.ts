import { getMongoDb } from './mongodb';
import { TimeSeriesBucketExtended } from './time-series-bucket-extended';
import { showBytes } from './helpers';
import { TimeSeriesBucketSimple } from './time-series-bucket-simple';
import * as fs from 'fs';

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
  {
    label: 'Simple hour - second precision',
    bucket: new TimeSeriesBucketSimple('hourBucketSimple', 3600000, 1000, ['v']),
    startTs: startTs,
    endTs: endTs,
    insertEvery: 60000
  },
  {
    label: 'Simple minute - second precision',
    bucket: new TimeSeriesBucketSimple('minuteBucketSimple', 60000, 1000, ['v']),
    startTs: startTs,
    endTs: endTs,
    insertEvery: 30000
  }
];

const queryTestConfigs = [
  {
    label: 'Last 24 hours in seconds',
    startTs: new Date(endTs.getTime() - 86400000),
    endTs: endTs,
    aggregate: 1000,
  },
  {
    label: 'Last 48 hours in minutes',
    startTs: new Date(endTs.getTime() - 86400000 * 2),
    endTs: endTs,
    aggregate: 60000,
  },
  {
    label: 'Last 30 days in minutes',
    startTs: new Date(endTs.getTime() - 86400000 * 30),
    endTs: endTs,
    aggregate: 60000,
  },
  {
    label: 'Last 30 days in hours',
    startTs: new Date(endTs.getTime() - 86400000 * 30),
    endTs: endTs,
    aggregate: 3600000,
  }
];

const protocol = fs.createWriteStream('protocol.log', {flags: 'a'});

async function testConfig(config) {
  const db = await getMongoDb();
  const bucket = config.bucket;

  const startTs = config.startTs;
  const endTs = config.endTs;
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
  printProtocol(config.label, {
    updates: updates,
    size: showBytes(stats.size),
    storage: showBytes(stats.storageSize),
    index: showBytes(stats.totalIndexSize),
    avg: stats.avgObjSize,
    dur: duration
  });

  for (let i = 0; i < queryTestConfigs.length; i++) {
    await queryTest(config, queryTestConfigs[i]);
  }

  // await collection.drop();

  return stats;
}

async function queryTest(config, queryConfig) {
  const db = await getMongoDb();
  const bucket = config.bucket;

  const startTime = new Date().getTime();

  const cursor = bucket.findAggregates(db, queryConfig.aggregate, queryConfig.startTs, queryConfig.endTs);
  let result = null;
  let received = 0;
  while (result = await cursor.next()) {
    received++;
  }

  const duration = new Date().getTime() - startTime;
  printProtocol(queryConfig.label, {
    received: received,
    dur: duration
  });

}

async function queryTest2(config, queryConfig) {
  const db = await getMongoDb();
  const bucket = config.bucket;

  const startTime = new Date().getTime();

  const result = await bucket.findAggregates2(db, queryConfig.aggregate, queryConfig.startTs, queryConfig.endTs);

  const duration = new Date().getTime() - startTime;
  printProtocol(queryConfig.label + ' [2]', {
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

async function testDb() {
  const mode = process.argv[2];
  if (mode === 'query') {
    for (let i = 0; i < testConfigs.length; i++) {
      const config = testConfigs[i];
      console.log(`Start ${config.label}`);
      for (let i = 0; i < queryTestConfigs.length; i++) {
        await queryTest(config, queryTestConfigs[i]);
        await queryTest2(config, queryTestConfigs[i]);
      }
      console.log(`Done ${config.label}`);
    }
  } else if (mode === 'all') {
    for (let i = 0; i < testConfigs.length; i++) {
      const config = testConfigs[i];
      console.log(`Start ${config.label}`);
      await testConfig(config);
      console.log(`Done ${config.label}`);
    }
  } else {
    console.log('unknown mode ', mode);
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
