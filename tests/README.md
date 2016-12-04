Basic Testing
=============
This documents describes a couple of tests apart from the ones posted on piazza.

# Table of Contents
1. Testing indirect pointers
2. Max filesize test
    1. Using dd
    2. Using a for loop and echo

## Testing indirect pointers
In order to test `fs_read` and `fs_write` with indirect pointers we run the
following tests.

To stay within indir_1 we want more than 6 x 1024 bytes
but less than (6 + 256) x 1024. We chose to write 510 x 540 = 275400 bytes.
```
yes A | dd oflag=direct bs=510 count=540 of=rootfs/file.A
dd iflag=direct if=rootfs/file.A bs=510 | cksum
```
To use the indir_2 pointers we chose to write 510 x 132112 = 67377120 bytes.
```
yes A | dd oflag=direct bs=510 count=132112 of=rootfs/file.max
dd iflag=direct if=rootfs/file.A bs=510 | cksum
```

## Max filesize test

Based on the the blocksize and size of variables used in the structs,
the maximum file size is (6 + 256 + 256^2) x 1024 = 67377152 bytes.
We first create a image large enough to hold such a file:
```
mkfs-x6 -size 70m rootfs.img
```

We then write and read file using the following two methods.

### Using dd
We use a block size of 1024 bytes.
```
unlink rootfs/file.max
yes A | dd oflag=direct bs=1024 count=65798 of=rootfs/file.max
```

### Using for loop and echo
We write a byte per loop iteration. This takes quite some time.

```
unlink rootfs/file.max
seq 1 67377152 | while IFS= read -r line
do
    echo -n $((i % 10)) >> rootfs/file.max
done
```

In both cases we issue the command
```
cat rootfs/file.max | wc -c
```
where the output should be 67377152.
