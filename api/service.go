package api

import (
	"context"
	"syscall"

	pb "github.com/Hookey/go-networkfuse/api/pb"
	"github.com/Hookey/go-networkfuse/nfs"
	"github.com/Hookey/go-networkfuse/sync"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server is used to implement APIServer
type NFSService struct {
	pb.UnimplementedAPIServer
	*nfs.NFSRoot
}

func (s *NFSService) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutReply, error) {
	path := in.GetPath()
	item := s.NFSRoot.Resolve(path)
	if item.Ino == 0 {
		return nil, status.Error(codes.NotFound, "File not found")
	}

	// TODO: Get a dir?
	if item.Stat.Mode&syscall.S_IFREG == 0 {
		return nil, status.Error(codes.FailedPrecondition, "Not a file")
	}

	cachePath := s.NFSRoot.CachePath(item.Ino)

	// TODO: sync client pool
	cli, err := sync.NewClient(grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	if err := cli.Put(cachePath, path); err != nil {
		return nil, err
	}
	// TODO: remove local cache

	return &pb.PutReply{Msg: "Put done"}, nil
}

func (s *NFSService) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetReply, error) {
	path := in.GetPath()
	//remove mountpoint?
	item := s.NFSRoot.Resolve(path)
	if item.Ino == 0 {
		return nil, status.Error(codes.NotFound, "File not found")
	}

	// TODO: Get a dir?
	if item.Stat.Mode&syscall.S_IFREG == 0 {
		return nil, status.Error(codes.FailedPrecondition, "Not a file")
	}

	cachePath := s.NFSRoot.CachePath(item.Ino)

	cli, err := sync.NewClient(grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	if err := cli.Get(path, cachePath); err != nil {
		return nil, err
	}
	// TODO: mark meta cached

	//err := s.Storage.Get(src, dst)
	return &pb.GetReply{Msg: "Get done"}, nil
}
