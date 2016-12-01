#!/usr/bin/env python
import sys

read_ops   = 0
write_ops  = 0
read_blk   = 0
write_blk  = 0

with open(sys.argv[1]) as f:
    content = f.readlines()

"""
Count all ops until the last one.
If the prev op is the the same as the cur and the operation is continuous
in terms of blocks, we continue to the next op until we find a different op
or a non-continuous op of the same type.
We count blocks in a strainforward way.
We skip the first 2 lines of the log.
"""

for i in xrange(3, len(content)):
    lst_cur    = content[i].split()
    lst_prev   = content[i-1].split()
    op_cur     = lst_cur[0]
    op_prev    = lst_prev[0]
    start_prev = int(lst_prev[1])
    len_prev   = int(lst_prev[2])
    start_cur  = int(lst_cur[1])

    # count blocks
    if op_prev == "read":
        read_blk += len_prev
    else:
        write_blk += len_prev

    if (op_cur == op_prev and (start_prev + len_prev == start_cur)):
        continue
    else:
        if op_prev == "read":
            read_ops += 1
        else:
            write_ops += 1

# process the last line
i = len(content) - 1
lst_cur    = content[i].split()
lst_prev   = content[i-1].split()
op_cur     = lst_cur[0]
op_prev    = lst_prev[0]
start_prev = int(lst_prev[1])
len_prev   = int(lst_prev[2])
start_cur  = int(lst_cur[1])

# count blocks
if op_cur == "read":
    read_blk += int(lst_cur[2])
else:
    write_blk += int(lst_cur[2])

# count ops
if (op_cur == op_prev and (start_prev + len_prev == start_cur)):
    if op_prev == "read":
        read_ops += 1
    else:
        write_ops += 1
else:
    if op_cur == "read":
        read_ops += 1
    else:
        write_ops += 1

# print the results
print "Read blocks   :  " + str(read_blk)
print "Write blocks  :  " + str(write_blk)
print "Read ops      :  " + str(read_ops)
print "Write ops     :  " + str(write_ops)
print "---------------"
print "Total ops     :  " + str(read_ops + write_ops)

