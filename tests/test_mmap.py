#!/usr/bin/env python3
"""Verify mmap works on NFS mount.

Usage:
    1. Start KiwiFS:  go run . serve --nfs-addr :2049 --root /tmp/kiwi-mmap-test
    2. Mount NFS:      sudo mount -t nfs -o vers=3,port=2049,mountport=2049,tcp localhost:/ /mnt/kiwi-nfs
    3. Run:            python3 tests/test_mmap.py /mnt/kiwi-nfs
"""
import mmap
import os
import sys

mount = sys.argv[1] if len(sys.argv) > 1 else "/mnt/kiwi-nfs"
test_file = os.path.join(mount, "mmap-test.txt")

with open(test_file, "w") as f:
    f.write("hello mmap world\n")

# Read-only mmap
fd = os.open(test_file, os.O_RDONLY)
try:
    mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
    content = mm.read()
    mm.close()
    assert content == b"hello mmap world\n", f"unexpected: {content!r}"
    print("READ-ONLY MMAP: PASS")
except Exception as e:
    print(f"READ-ONLY MMAP: FAIL ({e})")
finally:
    os.close(fd)

# Writable mmap
fd = os.open(test_file, os.O_RDWR)
try:
    mm = mmap.mmap(fd, 0, access=mmap.ACCESS_WRITE)
    mm[0:5] = b"HELLO"
    mm.close()
    print("WRITABLE MMAP: PASS")
except Exception as e:
    print(f"WRITABLE MMAP: FAIL ({e})")
finally:
    os.close(fd)

# Verify content after writable mmap
with open(test_file, "r") as f:
    final = f.read()
    expected = "HELLO mmap world\n"
    if final == expected:
        print("CONTENT VERIFY: PASS")
    else:
        print(f"CONTENT VERIFY: FAIL (got {final!r}, want {expected!r})")

os.remove(test_file)
print("\nDone. Clean up: sudo umount /mnt/kiwi-nfs")
