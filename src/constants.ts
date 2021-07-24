import { config } from 'dotenv';

config();

export const HTTP_TIMEOUT_SECONDS: number = process.env['HTTP_TIMEOUT_SECONDS']
  ? parseInt(process.env['HTTP_TIMEOUT_SECONDS'] || '15')
  : 15;

if (HTTP_TIMEOUT_SECONDS === NaN) {
  console.error('the provided value for HTTP_TIMEOUT_SECONDS was not a number');
  process.exit(1);
}