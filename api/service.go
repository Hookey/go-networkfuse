package api

import (
	"context"

	pb "github.com/Hookey/go-networkfuse/api/pb"
	"github.com/Hookey/go-networkfuse/nfs"
)

// server is used to implement APIServer
type Service struct {
	pb.UnimplementedAPIServer
	*nfs.NFSRoot
}

func (s *Service) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutReply, error) {
	//p := in.GetPath()
	//err := s.Storage.Put(p)
	return &pb.PutReply{}, nil
}

func (s *Service) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetReply, error) {
	//p := in.GetPath()
	//err := s.Storage.Get(src, dst)
	return &pb.GetReply{}, nil
}
