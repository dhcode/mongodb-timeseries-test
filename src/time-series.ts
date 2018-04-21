import { AggregationCursor, Cursor, Db } from 'mongodb';

export interface PropertyValues {
  [name: string]: number;
}

export const aggregationNames = {
  1: 'ms', // Milliseconds
  10: 'cs', // Centiseconds
  100: 'ds', // Deciseconds
  1000: 's', // Seconds
  60000: 'm', // Minutes
  3600000: 'h', // Hours
  86400000: 'd', // Days
  604800000: 'w' // Weeks
};

/**
 * Represents a time series bucket based on the idea described there:
 * http://rcardin.github.io/database/mongodb/time-series/2017/01/31/implementing-time-series-in-mongodb.html
 */
export class TimeSeriesBucket {

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
   * E.g. 1000, 100 for seconds and tenths of a second
   */
  aggregations: number[] = [];

  /**
   * The number properties to save for each entry
   */
  properties: string[] = [];


  constructor(name: string, size: number, aggregations: number[], properties: string[]) {
    this.name = name;
    this.size = size;
    this.aggregations = aggregations || [];
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

  // async setupCollection(db: Db) {
  //   const collection = db.collection(this.name);
  //   collection.createIndexes();
  // }

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
    const project = {};
    if (this.aggregations.length) {
      project[this.getAggregationName(this.aggregations[0])] = false;
    }
    return collection.find({_id: {$gte: from, $lte: to}}).map(doc => {
      doc.dt = doc._id;
      delete doc._id;
      return doc;
    }).project(project);
  }

  findAggregates(db: Db, aggregate: number, from: Date, to: Date): AggregationCursor | Cursor {
    const collection = db.collection(this.name);
    if (aggregate === this.size || !aggregate) {
      return this.findBuckets(db, from, to);

    } else if (this.aggregations.includes(aggregate)) {
      const stages = [];
      stages.push({$match: {_id: {$gte: from, $lte: to}}});
      let path = [];
      this.aggregations
        .filter(aggregationSize => aggregationSize >= aggregate)
        .forEach(aggregationSize => {
          const name = this.getAggregationName(aggregationSize);
          path.push(name);
          stages.push({$unwind: '$' + path.join('.')});
        });
      stages.push({$replaceRoot: {newRoot: '$' + path.join('.')}});
      return collection.aggregate(stages);

    } else {
      throw new Error(`Aggregate size ${aggregate} not available`);
    }

  }

  getBucketTs(date: Date): Date {
    return new Date(Math.floor(date.getTime() / this.size) * this.size);
  }

  getZeroBucket(date: Date): any {
    const bucketTs = this.getBucketTs(date);

    return {
      _id: bucketTs,
      ...this.getZeroProperties(),
      ...this.getAggregationProperties(bucketTs.getTime())
    };
  }

  getUpdate(date: Date, data: PropertyValues): any {
    const names = this.properties.filter(name => !!data[name]);
    if (!names.length) {
      return null;
    }
    const update = {};
    names.forEach(name => update[name] = data[name]);
    return {
      $inc: {
        ...update,
        ...this.getUpdateForAggregation(date.getTime(), names, data)
      }
    };
  }

  private isPropertyNameAllowed(name) {
    return !Object.values(aggregationNames).includes(name);
  }

  private getAggregationName(aggregationSize) {
    return aggregationNames[aggregationSize] || 'a' + aggregationSize;
  }

  private getUpdateForAggregation(ts: number, names: string[], data: PropertyValues,
                                  aggregationsIndex = 0, prefix = '') {
    const update = {};
    if (aggregationsIndex >= this.aggregations.length) {
      return update;
    }
    const aggregationSize = this.aggregations[aggregationsIndex];
    const size = aggregationsIndex === 0 ? this.size : this.aggregations[aggregationsIndex - 1];
    const aggregationName = this.getAggregationName(aggregationSize);
    const baseTs = Math.floor(ts / size) * size;
    const index = Math.floor((ts - baseTs) / aggregationSize);
    const path = prefix + aggregationName + '.' + index;
    names.forEach(name => {
      update[path + '.' + name] = data[name];
    });
    return {...update, ...this.getUpdateForAggregation(ts, names, data, aggregationsIndex + 1, path + '.')};
  }

  private getZeroProperties(): PropertyValues {
    const data: PropertyValues = {};
    this.properties.forEach(name => data[name] = 0);
    return data;
  }

  private getAggregationProperties(startTs: number, aggregationsIndex = 0) {
    const data = {};
    if (aggregationsIndex >= this.aggregations.length) {
      return data;
    }
    const aggregationSize = this.aggregations[aggregationsIndex];
    const size = aggregationsIndex === 0 ? this.size : this.aggregations[aggregationsIndex - 1];
    const aggregationName = this.getAggregationName(aggregationSize);
    const list = data[aggregationName] = [];
    const endTs = startTs + size;
    for (let i = startTs; i < endTs; i += aggregationSize) {
      list.push({
        dt: new Date(i),
        ...this.getZeroProperties(),
        ...this.getAggregationProperties(i, aggregationsIndex + 1)
      });
    }

    return data;
  }

}
