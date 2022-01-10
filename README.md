# GO-Network-FUSE

[![CI](https://github.com/Hookey/go-networkfuse/actions/workflows/ci.yml/badge.svg)](https://github.com/Hookey/go-networkfuse/actions/workflows/ci.yml)

A user-space filesystem based on 
[github.com/hanwen/go-fuse/fs](https://godoc.org/github.com/hanwen/go-fuse/fs)

Use [badgerhold](https://github.com/timshannon/badgerhold) to store metadata, and use underlying filesystem to hold cache.

# How to use AFS
1. Take dropbox as an example, and apply dropbox app key/secret.
1. Setup [syncagent](https://github.com/Hookey/go-sync) with key/secret and finish oauth2 token exchange
```
DROPBOX_PERSONAL_APP_KEY=xxxxxxxxxx DROPBOX_PERSONAL_APP_SECRET=xxxxxxxxx go run sync/cmd/agent.go --port $syncAddr dropbox
(seconds later...)
1. Go to https://www.dropbox.com/1/oauth2/authorize?client_id=rw101uczlj4t3on&response_type=code&state=state
2. Click "Allow" (you might have to log in first).
3. Copy the authorization code.
Enter the authorization code here:
```
1. Setup fuse
```
go run networkfuse/cmd/nfs/nfs.go --port $fuseAddr --addr $syncAddr $mnt $cache
```
1. Archive a file with nfs cli, and the file will be uploaded to dropbox and deleted at local.
```
go run networkfuse/cmd/cli.go --addr $fuseAddr put $file
```
1. Download a archived file with nfs cli
```
go run networkfuse/cmd/cli.go --addr $fuseAddr get $file
```
