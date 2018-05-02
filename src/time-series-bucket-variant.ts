import { PropertyValues, TimeSeriesBucket } from './time-series-bucket.model';
import { aggregationNames } from './time-series-bucket-extended';
import { Cursor, Db } from 'mongodb';

export class TimeSeriesBucketVariant implements TimeSeriesBucket {

  /**
   * Collection name
   */
  name: string;

  /**
   * Amount of milliseconds one bucket represents.
   * E.g. 60000 for 1 minute
   */
  size: number;

  /**
   * Aggregation level to add in each bucket
   * E.g. 1000 for seconds
   */
  aggregation = 1000;

  /**
   * The number properties to save for each entry
   */
  properties: string[] = [];


  constructor(name: string, size: number, aggregation: number, properties: string[]) {
    this.name = name;
    this.size = size;
    this.aggregation = aggregation || 1000;
    this.properties = properties;
    if (!properties || !properties.length) {
      throw new Error(`Properties must be provided`);
    }
    properties.forEach(name => {
      if (!this.isPropertyNameAllowed(name)) {
        throw new Error(`Property name ${name} is not allowed`);
      }
    });
  }

  async add(db: Db, date: Date, data: PropertyValues): Promise<boolean> {
    const bucketTs = this.getBucketTs(date);
    const update = this.getUpdate(date, data);
    if (!update) {
      return true;
    }
    const collection = db.collection(this.name);
    let updateResult = await collection.updateOne({_id: bucketTs}, update);
    if (!updateResult.modifiedCount) {
      await collection.insertOne(this.getZeroBucket(date));
      updateResult = await collection.updateOne({_id: bucketTs}, update);
    }

    return updateResult.modifiedCount === 1;
  }

  findBuckets(db: Db, from: Date, to: Date): Cursor {
    const collection = db.collection(this.name);
    return collection.find({_id: {$gte: from, $lt: to}}).map(doc => {
      doc.dt = doc._id;
      delete doc._id;
      this.properties.forEach(name => doc[name] = doc[name].reduce((sum, curr) => sum + curr, 0));
      return doc;
    });
  }

  findAggregates(db: Db, aggregate: number, from: Date, to: Date): Promise<object[]> {
    const collection = db.collection(this.name);

    const stages = [];
    stages.push({$match: {_id: {$gte: from, $lt: to}}});

    if (aggregate >= this.size) {
      const project = {
        _id: true,
      };
      this.properties.forEach(name => {
        project[name] = '$_s.' + name;
      });
      stages.push({$project: project});
      if (aggregate > this.size) {
        const epochStart = new Date(0);
        const project = {
          _id: {$add: [epochStart, {$multiply: ['$_id', aggregate]}]}
        };
        const group = {
          _id: {$floor: {$divide: [{$subtract: ['$_id', epochStart]}, aggregate]}},
        };
        this.properties.forEach(name => {
          group[name] = {$sum: '$' + name};
          project[name] = true;
        });
        stages.push({
          $group: group
        });
        stages.push({$project: project});
      }
      return collection.aggregate(stages).toArray();
    }


    const inputs = [];
    const project = {
      _id: {$add: ['$_id', {$multiply: ['$_i', this.aggregation]}]}
    };

    this.properties.forEach((name, i) => {
      inputs.push('$' + name);
      project[name] = {$arrayElemAt: ['$_data', i]};
    });

    stages.push({$project: {_id: 1, _data: {$zip: {inputs: inputs}}}});
    stages.push({$unwind: {path: '$_data', includeArrayIndex: '_i'}});
    stages.push({$project: project});
    // stages.push(stages[0]);

    if (aggregate !== this.aggregation) {
      aggregate = aggregate || this.size;
      const epochStart = new Date(0);
      const project = {
        _id: {$add: [epochStart, {$multiply: ['$_id', aggregate]}]}
      };
      const group = {
        _id: {$floor: {$divide: [{$subtract: ['$_id', epochStart]}, aggregate]}},
      };
      this.properties.forEach(name => {
        group[name] = {$sum: '$' + name};
        project[name] = true;
      });
      stages.push({
        $group: group
      });
      stages.push({$project: project});
    }

    return collection.aggregate(stages).toArray();

  }

  async findAggregates2(db: Db, aggregate: number, from: Date, to: Date): Promise<object[]> {
    const collection = db.collection(this.name);

    const startTime = new Date().getTime();
    const expect = Math.ceil((to.getTime() - from.getTime()) / this.size);

    const cursor = collection.find({_id: {$gte: from, $lte: to}}).batchSize(expect);
    const count = this.size / this.aggregation;
    let entry;
    const results = new Map();
    const emptyResult = id => this.properties.reduce((obj, name) => {
      obj[name] = 0;
      return obj;
    }, {_id: id});
    while (entry = await cursor.next()) {
      // console.log('recv cursor', new Date().getTime() - startTime);

      for (let i = 0; i < count; i++) {

        const id = Math.floor((entry._id.getTime() + i * this.aggregation) / aggregate) * aggregate;
        let result = results.get(id);
        if (!result) {
          result = emptyResult(new Date(id));
          results.set(id, result);
        }
        this.properties.forEach(name => {
          result[name] += entry[name][i];
        });

      }

    }
    return Array.from(results.values());

  }

  getBucketTs(date: Date): Date {
    return new Date(Math.floor(date.getTime() / this.size) * this.size);
  }

  getZeroBucket(date: Date): any {
    const bucketTs = this.getBucketTs(date);

    return {
      _id: bucketTs,
      ...this.getZeroSumProperties(),
      ...this.getZeroProperties()
    };
  }

  getUpdate(date: Date, data: PropertyValues): any {
    const names = this.properties.filter(name => !!data[name]);
    if (!names.length) {
      return null;
    }
    const ts = date.getTime();
    const update = {};
    const baseTs = Math.floor(ts / this.size) * this.size;
    const index = Math.floor((ts - baseTs) / this.aggregation);

    names.forEach(name => {
      update['_s.' + name] = data[name];
      update[name + '.' + index] = data[name];
    });
    return {
      $inc: update
    };
  }

  private getZeroProperties(): PropertyValues {
    const data = {};
    const count = this.size / this.aggregation;
    this.properties.forEach(name => data[name] = new Array(count).fill(0));
    return data;
  }

  private getZeroSumProperties() {
    const data = {_s: {}};
    this.properties.forEach(name => data._s[name] = 0);
    return data;
  }

  private isPropertyNameAllowed(name) {
    return !Object.values(aggregationNames).includes(name) && name !== '_id';
  }

}
