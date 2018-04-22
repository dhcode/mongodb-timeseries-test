import { TimeSeriesBucketExtended } from './time-series';
import { Db } from 'mongodb';

describe('Time Series', function () {
  const hourBucket = new TimeSeriesBucketExtended('hourBucket', 3600000, [], [
    'value'
  ]);

  const minuteBucket = new TimeSeriesBucketExtended('minuteBucket', 60000, [1000], [
    'value'
  ]);

  it('should produce zero bucket', function () {
    const dt = new Date('2018-04-21T00:00:00Z');
    const zeroBucket = hourBucket.getZeroBucket(dt);
    expect(zeroBucket).toEqual({_id: dt, value: 0});
  });

  it('should produce zero bucket with aggregate', function () {
    const dt = new Date('2018-04-21T00:00:00Z');
    const zeroBucket = minuteBucket.getZeroBucket(dt);
    const seconds = new Array(60).fill(null).map((v, i) => ({
      dt: new Date(dt.getTime() + i * 1000),
      value: 0
    }));
    expect(zeroBucket).toEqual({_id: dt, value: 0, s: seconds});
  });

  it('should produce update', function () {
    const dt = new Date('2018-04-21T00:00:00Z');
    const update = hourBucket.getUpdate(dt, {value: 324});
    expect(update).toEqual({'$inc': {value: 324}});
  });

  it('should produce update with aggregate', function () {
    const dt = new Date('2018-04-21T00:00:05Z');
    const update = minuteBucket.getUpdate(dt, {value: 324});
    expect(update).toEqual({
      '$inc': {
        value: 324,
        's.5.value': 324
      }
    });
  });

  it('should not update without changes', function () {
    const dt = new Date('2018-04-21T00:00:00Z');
    const update = hourBucket.getUpdate(dt, {value: 0});
    expect(update).toBeNull();
  });

  it('should add update without changes', function () {
    const dt = new Date('2018-04-21T00:00:00Z');
    const update = hourBucket.add(null, dt, {value: 0});
    expect(update).resolves.toBe(true);
  });

  it('should add a new bucket', async function () {
    const dt = new Date('2018-04-21T00:00:00Z');
    let inserted = false;
    const collectionMock = {
      updateOne: jest.fn(() => {
        return Promise.resolve({modifiedCount: inserted ? 1 : 0});
      }),
      insertOne: jest.fn(() => {
        inserted = true;
      })
    };
    const dbMock = {
      collection(name: string) {
        return collectionMock;
      }
    };

    const update = hourBucket.add(dbMock as any as Db, dt, {value: 324});
    await expect(update).resolves.toBe(true);
    expect(collectionMock.updateOne.mock.calls[0]).toEqual([{_id: dt}, {'$inc': {'value': 324}}]);
    expect(collectionMock.insertOne.mock.calls[0]).toEqual([{_id: dt, value: 0}]);
    expect(collectionMock.updateOne.mock.calls[1]).toEqual([{_id: dt}, {'$inc': {'value': 324}}]);
  });

  it('should update an existing bucket', async function () {
    const dt = new Date('2018-04-21T00:00:00Z');
    const collectionMock = {
      updateOne: jest.fn(),
      insertOne: jest.fn()
    };
    const dbMock = {
      collection(name: string) {
        return collectionMock;
      }
    };
    collectionMock.updateOne.mockResolvedValue({modifiedCount: 1});

    const update = hourBucket.add(dbMock as any as Db, dt, {value: 324});
    await expect(update).resolves.toBe(true);
    expect(collectionMock.updateOne.mock.calls[0]).toEqual([{_id: dt}, {'$inc': {'value': 324}}]);
  });

  it('should find buckets', async function () {
    const cursorMock = {
      map: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis()
    };
    const collectionMock = {
      find: jest.fn().mockReturnValue(cursorMock)
    };
    const dbMock = {
      collection(name: string) {
        return collectionMock;
      }
    };
    const from = new Date('2018-04-21T00:00:00Z');
    const to = new Date('2018-04-21T06:00:00Z');
    const cursor = minuteBucket.findBuckets(dbMock as any as Db, from, to);
    expect(cursor).toBe(cursorMock);
    expect(collectionMock.find.mock.calls[0]).toEqual([{_id: {$gte: from, $lte: to}}]);

  });

  it('should find aggregates', async function () {
    const cursorMock = jest.fn();
    const collectionMock = {
      aggregate: jest.fn().mockReturnValue(cursorMock)
    };
    const dbMock = {
      collection(name: string) {
        return collectionMock;
      }
    };
    const from = new Date('2018-04-21T00:00:00Z');
    const to = new Date('2018-04-21T06:00:00Z');
    const cursor = minuteBucket.findAggregates(dbMock as any as Db, 1000, from, to);
    expect(cursor).toBe(cursorMock);
    expect(collectionMock.aggregate.mock.calls[0]).toEqual([[
      {$match: {_id: {$gte: from, $lte: to}}},
      {$unwind: '$s'},
      {$replaceRoot: {newRoot: '$s'}}
    ]]);

  });


});
