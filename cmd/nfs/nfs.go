// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This program is the analogon of libfuse's hello.c, a a program that
// exposes a single file "file.txt" in the root directory.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"

	"github.com/Hookey/go-networkfuse/api"
	pb "github.com/Hookey/go-networkfuse/api/pb"
	"github.com/Hookey/go-networkfuse/nfs"
	"github.com/Hookey/go-networkfuse/sync"
	fuse "github.com/hanwen/go-fuse/v2/fs"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

var log = logging.Logger("main")

// SetupDefaultLoggingConfig sets up a standard logging configuration.
func SetupDefaultLoggingConfig(file string) error {
	c := logging.Config{
		Format: logging.ColorizedOutput,
		Stderr: true,
		Level:  logging.LevelError,
	}
	if file != "" {
		if err := os.MkdirAll(filepath.Dir(file), os.ModePerm); err != nil {
			return err
		}
		c.File = file
	}
	logging.SetupLogging(c)
	return nil
}

// SetLogLevels sets levels for the given systems.
func SetLogLevels(systems map[string]logging.LogLevel) error {
	for sys, level := range systems {
		l := zapcore.Level(level)
		if sys == "*" {
			for _, s := range logging.GetSubsystems() {
				if err := logging.SetLogLevel(s, l.CapitalString()); err != nil {
					return err
				}
			}
		}
		if err := logging.SetLogLevel(sys, l.CapitalString()); err != nil {
			return err
		}
	}
	return nil
}

// LevelFromDebugFlag returns the debug or info log level.
func LevelFromDebugFlag(debug bool) logging.LogLevel {
	if debug {
		return logging.LevelDebug
	} else {
		return logging.LevelInfo
	}
}

func runServer(port *string, r *nfs.NFSRoot) error {
	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterAPIServer(s, &api.NFSService{NFSRoot: r})

	log.Infof("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return nil
}

func main() {
	// TODO: use config or is this ok?
	sync.Addr = flag.String("addr", "localhost:50051", "sync address")
	port := flag.String("port", ":50052", "nfuse cli port")
	debug := flag.Bool("debug", false, "print debug data")
	db := flag.String("db", ".db", "db location")
	logFile := flag.String("logFile", "", "File to write logs to")
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Printf("usage: %s MOUNTPOINT ORIGINAL\n", path.Base(os.Args[0]))
		fmt.Printf("\noptions:\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	orig := flag.Arg(1)
	mnt := flag.Arg(0)

	if err := SetupDefaultLoggingConfig(*logFile); err != nil {
		log.Fatal(err)
	}

	if err := SetLogLevels(map[string]logging.LogLevel{
		"nfs":  LevelFromDebugFlag(*debug),
		"main": LevelFromDebugFlag(*debug),
	}); err != nil {
		log.Fatal(err)
	}

	store, err := nfs.NewMetaStore(*db)
	if err != nil {
		log.Fatalf("Open badgerDB(%s): %v\n", db, err)
	}

	defer store.Close()

	//TODO: root.embed().stableattr.ino is set to 0, should be 1 instead. Need to wait go-fuse fix
	// https://github.com/hanwen/go-fuse/issues/399
	nfsRoot, embedNode, err := nfs.NewNFSRoot(orig, store)
	if err != nil {
		log.Fatalf("NewLoopbackRoot(%s): %v\n", orig, err)
	}

	opts := &fuse.Options{}
	opts.Debug = *debug
	nfs, err := fuse.Mount(mnt, embedNode, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	//TODO: umount mountpoint correctly
	defer nfs.Unmount()

	//TODO: use ctx to check runserver is okay
	go runServer(port, nfsRoot)

	nfs.Wait()
}
