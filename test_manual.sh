#!/bin/bash

# Manual test script for Exercise 7

DISK="test.img"
MOUNT="mnt"

echo "=== Manual Testing Exercise 7 ==="
echo ""

# Create mount point
mkdir -p $MOUNT

# Test 1: Write data, unmount, remount, verify persistence
echo "Test 1: Data persistence after unmount"
echo "---------------------------------------"

# Mount filesystem
./fs5600 $DISK $MOUNT &
FUSE_PID=$!
sleep 1

# Write test data
echo "Writing test data..."
echo "Hello from Exercise 7!" > $MOUNT/test_persist.txt
echo "Second line of data" >> $MOUNT/test_persist.txt

# Read it back immediately
echo "Reading back (from cache):"
cat $MOUNT/test_persist.txt
echo ""

# Unmount (this should flush caches via fs_destroy)
echo "Unmounting filesystem (triggering fs_destroy)..."
fusermount -u $MOUNT 2>/dev/null || umount $MOUNT
wait $FUSE_PID 2>/dev/null

# Remount
echo "Remounting filesystem..."
./fs5600 $DISK $MOUNT &
FUSE_PID=$!
sleep 1

# Verify data persisted
echo "Reading back after remount (from disk):"
if cat $MOUNT/test_persist.txt 2>/dev/null; then
    echo ""
    echo "✓ Data persisted correctly!"
else
    echo "✗ Data was lost - cache not flushed!"
fi

# Cleanup
fusermount -u $MOUNT 2>/dev/null || umount $MOUNT
wait $FUSE_PID 2>/dev/null

echo ""
echo "Test complete!"
