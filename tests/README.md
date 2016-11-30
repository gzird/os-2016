Basic Testing
=============
This documents describes a couple of tests apart from the ones posted on piazza.

# Table of Contents
1. [Max filesize test]
    1. [Using dd]
    2. [Using a for loop and echo]


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
yes A | dd bs=1024 count=65798 of=rootfs/file.max
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
