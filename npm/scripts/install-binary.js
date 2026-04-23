#!/usr/bin/env node

const https = require('https');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const os = require('os');

// Detect platform and architecture
const platform = os.platform();
const arch = os.arch();

const platformMap = {
  darwin: 'darwin',
  linux: 'linux',
  win32: 'windows',
};

const archMap = {
  x64: 'amd64',
  arm64: 'arm64',
};

const kiwiPlatform = platformMap[platform];
const kiwiArch = archMap[arch];

if (!kiwiPlatform || !kiwiArch) {
  console.error(`Unsupported platform: ${platform}-${arch}`);
  process.exit(1);
}

// Get latest release version
function getLatestVersion() {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'api.github.com',
      path: '/repos/kiwifs/kiwifs/releases/latest',
      headers: {
        'User-Agent': 'kiwifs-npm-installer',
      },
    };

    https.get(options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          const release = JSON.parse(data);
          resolve(release.tag_name);
        } catch (err) {
          reject(err);
        }
      });
    }).on('error', reject);
  });
}

// Download binary
function downloadBinary(version) {
  const filename = `kiwifs-${kiwiPlatform}-${kiwiArch}.tar.gz`;
  const url = `https://github.com/kiwifs/kiwifs/releases/download/${version}/${filename}`;
  const binDir = path.join(__dirname, '..', 'bin');
  const tarPath = path.join(binDir, filename);

  if (!fs.existsSync(binDir)) {
    fs.mkdirSync(binDir, { recursive: true });
  }

  console.log(`Downloading KiwiFS ${version} for ${kiwiPlatform}-${kiwiArch}...`);

  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(tarPath);
    https.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`Failed to download: ${res.statusCode}`));
        return;
      }

      res.pipe(file);
      file.on('finish', () => {
        file.close();
        console.log('Download complete. Extracting...');

        // Extract tar.gz
        try {
          execSync(`tar -xzf "${tarPath}" -C "${binDir}"`, { stdio: 'inherit' });
          fs.unlinkSync(tarPath);

          // Rename extracted binary to just "kiwifs"
          const extractedName = `kiwifs-${kiwiPlatform}-${kiwiArch}${platform === 'win32' ? '.exe' : ''}`;
          const extractedPath = path.join(binDir, extractedName);
          const finalPath = path.join(binDir, `kiwifs${platform === 'win32' ? '.exe' : ''}`);

          if (fs.existsSync(extractedPath)) {
            fs.renameSync(extractedPath, finalPath);
            fs.chmodSync(finalPath, 0o755);
          }

          console.log('✅ KiwiFS installed successfully!');
          resolve();
        } catch (err) {
          reject(err);
        }
      });
    }).on('error', reject);
  });
}

// Main installation flow
async function install() {
  try {
    const version = await getLatestVersion();
    await downloadBinary(version);
  } catch (err) {
    console.error('Installation failed:', err.message);
    process.exit(1);
  }
}

install();
