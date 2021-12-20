package api

import (
	"context"

	pb "github.com/Hookey/go-networkfuse/api/pb"
	"github.com/Hookey/go-networkfuse/nfs"
)

// server is used to implement APIServer
type NFSService struct {
	pb.UnimplementedAPIServer
	*nfs.NFSRoot
}

func (s *NFSService) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutReply, error) {
	path := in.GetPath()
	if err := s.NFSRoot.Upload(path); err != nil {
		return nil, err
	}

	return &pb.PutReply{Msg: "Put done"}, nil
}

func (s *NFSService) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetReply, error) {
	path := in.GetPath()
	if err := s.NFSRoot.Download(path); err != nil {
		return nil, err
	}

	return &pb.GetReply{Msg: "Get done"}, nil
}
