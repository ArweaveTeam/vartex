import { config } from 'dotenv';

process.env.NODE_ENV !== 'test' && config();

let tmpIndices: string[] = [];
try { 
  tmpIndices = JSON.parse(process.env.INDICES || '[]');
} catch (e) {
  console.error('[order] invalid indices', process.env.INDICES);
}

export const indices = tmpIndices;
export const blockOrder = [
  'id',
  'previous_block',
  'mined_at',
  'height',
  'txs',
  'extended',
];
export const transactionOrder = [
  'format',
  'id',
  'signature',
  'owner',
  'owner_address',
  'target',
  'reward',
  'last_tx',
  'height',
  'tags',
  'quantity',
  'content_type',
  'data_size',
  'data_root',
];
export const tagOrder = ['tx_id', 'index', 'name', 'value'];
