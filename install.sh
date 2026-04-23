#!/bin/bash
# KiwiFS installation script
# Usage: curl -fsSL https://kiwifs.dev/install.sh | sh

set -e

# Detect OS and architecture
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$OS" in
  linux)
    OS="linux"
    ;;
  darwin)
    OS="darwin"
    ;;
  *)
    echo "Error: Unsupported operating system: $OS"
    exit 1
    ;;
esac

case "$ARCH" in
  x86_64)
    ARCH="amd64"
    ;;
  aarch64|arm64)
    ARCH="arm64"
    ;;
  *)
    echo "Error: Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

# Get latest version from GitHub releases
LATEST_VERSION=$(curl -s https://api.github.com/repos/kiwifs/kiwifs/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')

if [ -z "$LATEST_VERSION" ]; then
  echo "Error: Could not determine latest version"
  exit 1
fi

echo "Installing KiwiFS $LATEST_VERSION for $OS/$ARCH..."

# Download URL
DOWNLOAD_URL="https://github.com/kiwifs/kiwifs/releases/download/${LATEST_VERSION}/kiwifs-${OS}-${ARCH}.tar.gz"

# Temporary directory
TMP_DIR=$(mktemp -d)
trap "rm -rf $TMP_DIR" EXIT

# Download and extract
echo "Downloading from $DOWNLOAD_URL..."
curl -fsSL "$DOWNLOAD_URL" -o "$TMP_DIR/kiwifs.tar.gz"

echo "Extracting..."
tar -xzf "$TMP_DIR/kiwifs.tar.gz" -C "$TMP_DIR"

# Determine install location
if [ -w "/usr/local/bin" ]; then
  INSTALL_DIR="/usr/local/bin"
elif [ -w "$HOME/.local/bin" ]; then
  INSTALL_DIR="$HOME/.local/bin"
  mkdir -p "$INSTALL_DIR"
else
  INSTALL_DIR="$HOME/bin"
  mkdir -p "$INSTALL_DIR"
fi

# Install binary
echo "Installing to $INSTALL_DIR..."
mv "$TMP_DIR/kiwifs-${OS}-${ARCH}" "$INSTALL_DIR/kiwifs"
chmod +x "$INSTALL_DIR/kiwifs"

# Verify installation
if ! command -v kiwifs &> /dev/null; then
  echo ""
  echo "⚠️  KiwiFS was installed to $INSTALL_DIR, but it's not in your PATH."
  echo "Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
  echo ""
  echo "    export PATH=\"\$PATH:$INSTALL_DIR\""
  echo ""
else
  echo ""
  echo "✅ KiwiFS installed successfully!"
  echo ""
  kiwifs --version
fi

echo ""
echo "Get started:"
echo "  kiwifs init ~/my-knowledge"
echo "  kiwifs serve --root ~/my-knowledge --port 3333"
echo ""
echo "Documentation: https://github.com/kiwifs/kiwifs"
