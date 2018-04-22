import { aggregationNames, PropertyValues } from './time-series';
import { Db } from 'mongodb';


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
