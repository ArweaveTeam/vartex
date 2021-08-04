const exitHook = require('exit-hook');
const child_process = require('child_process');

const date = new Date();
const timestamp = [
  date.getFullYear(),
  ('0' + (date.getMonth() + 1)).slice(-2),
  ('0' + date.getDate()).slice(-2),
  date.getHours(),
  date.getMinutes(),
  date.getSeconds(),
].join('-');

let proc;
let retry = true;
let retryCount = 0;
const retries = 10;

function start() {
  proc = child_process.spawn(
    'node',
    [
      '--experimental-specifier-resolution=node',
      '--max-old-space-size=4096',
      '--loader=ts-node/esm.mjs',
      'src/Gateway.ts',
    ],
    {
      env: { ...process.env, TS_NODE_FILES: true },
      shell: true,
      stdio: 'inherit',
    }
  );

  proc.on('exit', function (code) {
    if (retry && retries < retryCount) {
      console.log('child process exited with code ' + code, 'retrying...');
      setTimeout(start, 0);
    } else {
      console.log('child process exited with code ' + code);
    }
  });
}

exitHook(() => {
  // exitP && exitP();
  console.error('Received exit signal on main process.');
  retry = false;
  if (proc) proc.kill('SIGINT');
  process.exit(0);
});

setTimeout(() => console.log('ALIVE!'), Number.MAX_NUMBER);

start();

// process.stdin.resume();
