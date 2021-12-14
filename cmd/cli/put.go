package cli

import (
	"context"
	"errors"

	pb "github.com/Hookey/go-networkfuse/api/pb"
	"github.com/spf13/cobra"
)

func put(cmd *cobra.Command, args []string) (err error) {
	if len(args) != 1 {
		return errors.New("`put` requires only a path argument")
	}

	path := args[0]

	_, err = c.Put(context.Background(), &pb.PutRequest{Path: path})

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
