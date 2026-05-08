// public API surface
export { StatsCollector } from "./tinyStats";
export { RollupJob } from "./rollup";

export { LocalAdapter } from "./adapters/local";
export { RedisAdapter, hourlyKeysInRange } from "./adapters/redis";
export { 
  PostgresAdapter, 
  queryHourly, 
  queryDaily, 
  queryMonthly, 
  queryStats 
} from "./adapters/postgres";
export { CompositeAdapter } from "./adapters/composite";

export type { 
  FlushAdapter, 
  CollectorOptions, 
  RollupOptions, 
  RollupResult, 
  PgPool, 
  PgClient, 
  RedisClient,
  QueryAdapter,
  StatRow,
  StatRangeRow,
  Granularity,
} from "./types";
