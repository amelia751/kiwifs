#!/usr/bin/env node

const { spawn } = require('child_process');
const path = require('path');
const os = require('os');

// Path to the binary
const platform = os.platform();
const binaryName = platform === 'win32' ? 'kiwifs.exe' : 'kiwifs';
const binaryPath = path.join(__dirname, '..', 'bin', binaryName);

// Forward all arguments to the kiwifs binary
const args = process.argv.slice(2);

const child = spawn(binaryPath, args, {
  stdio: 'inherit',
  env: process.env,
});

child.on('exit', (code) => {
  process.exit(code || 0);
});

child.on('error', (err) => {
  console.error('Failed to start KiwiFS:', err.message);
  console.error('');
  console.error('If the binary is missing, try reinstalling:');
  console.error('  npm install -g kiwifs');
  process.exit(1);
});
