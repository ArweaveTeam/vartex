import {config} from 'dotenv';

process.env.NODE_ENV !== 'test' && config();

export const HTTP_TIMEOUT_SECONDS: number = process.env['HTTP_TIMEOUT_SECONDS'] ?
  parseInt(process.env['HTTP_TIMEOUT_SECONDS'] || '15') :
  15;

if (HTTP_TIMEOUT_SECONDS === NaN) {
  console.error('the provided value for HTTP_TIMEOUT_SECONDS was not a number');
  process.exit(1);
}

export const KEYSPACE: string = process.env['KEYSPACE'] ?
  process.env['KEYSPACE'] :
  'gateway';

export const IPC_DATA = {};

export const POLLTIME_DELAY_SECONDS: number = process.env[
    'POLLTIME_DELAY_SECONDS'
] ?
  parseInt(process.env['POLLTIME_DELAY_SECONDS'] || '5') :
  5;
