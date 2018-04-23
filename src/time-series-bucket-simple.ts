import { aggregationNames, PropertyValues } from './time-series';
import { AggregationCursor, Cursor, Db } from 'mongodb';


export class TimeSeriesBucketSimple {

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
   * Aggregation levels to add in each bucket
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
    return collection.find({_id: {$gte: from, $lte: to}}).map(doc => {
      doc.dt = doc._id;
      delete doc._id;
      this.properties.forEach(name => doc[name] = doc[name].reduce((sum, curr) => sum + curr, 0));
      return doc;
    });
  }

  findAggregates(db: Db, aggregate: number, from: Date, to: Date): AggregationCursor | Cursor {
    /*
    db.getCollection('minuteBucketSimple').aggregate([
    {$unwind: {path: '$v', includeArrayIndex: 'i_v'}},
    {$unwind: {path: '$a', includeArrayIndex: 'i_a'}},
    {$project: {
        _id: false,
        dt: {$add: ['$_id', {$multiply: ['$i_v', 1000]}]},
        v: '$v',
        a: '$a',
        same: {$eq: ['$i_v', '$i_a']}
    }},
    {$match: {same: true}},
    {$project: {dt: 1, v: 1, a: 1}}
])

db.getCollection('minuteBucketSimple').aggregate([
    {$project: {
        _id: 1,
        data: {$zip: {inputs: ['$v', '$a']}}
    }},
    {$unwind: {path: '$data', includeArrayIndex: '_i'}},
    {$project: {
        _id: false,
        dt: {$add: ['$_id', {$multiply: ['$_i', 1000]}]},
        v: {$arrayElemAt: ['$data', 0]},
        a: {$arrayElemAt: ['$data', 1]}
    }}
])
     */
    const collection = db.collection(this.name);
    if (aggregate === this.size || !aggregate) {
      return this.findBuckets(db, from, to);

    } else if (aggregate === this.aggregation) {
      const stages = [];
      stages.push({$match: {_id: {$gte: from, $lte: to}}});

      const inputs = [];
      const project = {
        _id: false,
        dt: {$add: ['$_id', {$multiply: ['$_i', this.aggregation]}]}
      };

      this.properties.forEach((name, i) => {
        inputs.push('$' + name);
        project[name] = {$arrayElemAt: ['$_data', i]};
      });

      stages.push({$project: {_id: 1, _data: {$zip: {inputs: inputs}}}});
      stages.push({$unwind: {path: '$_data', includeArrayIndex: '_i'}});
      stages.push({$project: project});

      return collection.aggregate(stages);

    }

  }

  getBucketTs(date: Date): Date {
    return new Date(Math.floor(date.getTime() / this.size) * this.size);
  }

  getZeroBucket(date: Date): any {
    const bucketTs = this.getBucketTs(date);

    return {
      _id: bucketTs,
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

    names.forEach(name => update[name + '.' + index] = data[name]);
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

  private isPropertyNameAllowed(name) {
    return !Object.values(aggregationNames).includes(name) && name !== '_id';
  }

}
