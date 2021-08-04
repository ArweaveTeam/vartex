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

function start(nodefile) {
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
      stdio: ['pipe', null, null, null, 'pipe'], // 'overlapped', //['pipe', 'pipe', 'pipe'], //
    }
  );

  proc.stdout.on('data', function (data) {
    process.stdout.write(data.toString());
  });

  proc.stderr.on('data', function (data) {
    process.stdout.write(data.toString());
  });

  proc.on('exit', function (code) {
    console.log('child process exited with code ' + code);
    setTimeout(start, 0);
  });
}

exitHook(() => {
  // exitP && exitP();
  console.error('Received exit signal on main process.');
  retry = false;
  if (proc) proc.kill('SIGINT');
  process.exit(0);
});

start();

process.stdin.resume();
