import { IPC_DATA } from '../constants';

process.on('message', (msg) => {
  console.log('Message from parent:', msg);
});
