import { AggregationCursor, Cursor, Db } from 'mongodb';

export interface PropertyValues {
  [name: string]: number;
}

export interface TimeSeriesBucket {
  /**
   * Collection name
   */
  name: string;

  /**
   * Amount of milliseconds one bucket represents.
   * E.g. 60000 for 1 minute
   */
  size: number;

  add(db: Db, date: Date, data: PropertyValues): Promise<boolean>;

  findBuckets(db: Db, from: Date, to: Date): Cursor;

  findAggregates(db: Db, aggregate: number, from: Date, to: Date): Promise<object[]>;

}
