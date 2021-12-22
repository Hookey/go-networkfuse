package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "github.com/Hookey/go-networkfuse/api/pb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func put(cmd *cobra.Command, args []string) (err error) {
	if len(args) != 1 {
		return errors.New("`put` requires only a path argument")
	}

	path := args[0]
	path = strings.TrimRight(path, "/")

	reply, err := c.Put(context.Background(), &pb.PutRequest{Path: path})
	st, ok := status.FromError(err)
	if !ok {
		// Error was not a status error
		fmt.Println(err.Error())
		return
	}

	// Use st.Message() and st.Code()
	if st.Code() != codes.OK {
		fmt.Println(st.Message())
	} else {
		fmt.Println(path, reply.GetMsg())
	}

	return
}

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put <source>",
	Short: "Archive a single file or folder",
	RunE:  put,
}

func init() {
	RootCmd.AddCommand(putCmd)
}
