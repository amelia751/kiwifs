const { spawn } = require('child_process');
const path = require('path');
const os = require('os');

// Path to the binary
const platform = os.platform();
const binaryName = platform === 'win32' ? 'kiwifs.exe' : 'kiwifs';
const binaryPath = path.join(__dirname, 'bin', binaryName);

/**
 * Run KiwiFS programmatically
 * @param {string[]} args - Command-line arguments
 * @param {object} options - Spawn options
 * @returns {Promise<{code: number, stdout: string, stderr: string}>}
 */
function run(args = [], options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(binaryPath, args, {
      ...options,
      env: { ...process.env, ...options.env },
    });

    let stdout = '';
    let stderr = '';

    if (child.stdout) {
      child.stdout.on('data', (data) => {
        stdout += data.toString();
      });
    }

    if (child.stderr) {
      child.stderr.on('data', (data) => {
        stderr += data.toString();
      });
    }

    child.on('error', reject);
    child.on('close', (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

/**
 * Start a KiwiFS server programmatically
 * @param {object} config - Server configuration
 * @returns {ChildProcess}
 */
function serve(config = {}) {
  const args = ['serve'];

  if (config.root) args.push('--root', config.root);
  if (config.port) args.push('--port', String(config.port));
  if (config.host) args.push('--host', config.host);
  if (config.search) args.push('--search', config.search);
  if (config.versioning) args.push('--versioning', config.versioning);
  if (config.auth) args.push('--auth', config.auth);
  if (config.apiKey) args.push('--api-key', config.apiKey);
  if (config.noWatch) args.push('--no-watch');
  if (config.webdav) args.push('--webdav');
  if (config.webdavPort) args.push('--webdav-port', String(config.webdavPort));
  if (config.nfs) args.push('--nfs');
  if (config.nfsPort) args.push('--nfs-port', String(config.nfsPort));
  if (config.s3) args.push('--s3');
  if (config.s3Port) args.push('--s3-port', String(config.s3Port));

  return spawn(binaryPath, args, {
    stdio: 'inherit',
    env: process.env,
  });
}

module.exports = {
  binaryPath,
  run,
  serve,
};
