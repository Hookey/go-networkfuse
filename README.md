# GO-Network-FUSE

A user-space filesystem based on 
[github.com/hanwen/go-fuse/fs](https://godoc.org/github.com/hanwen/go-fuse/fs)

Use badgerDB to hold metadata, and use underlying filesystem to hold data.
This design will enable to operate on network storage.


