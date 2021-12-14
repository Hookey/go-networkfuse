package cli

import (
	"context"
	"errors"

	pb "github.com/Hookey/go-networkfuse/api/pb"
	"github.com/spf13/cobra"
)

func get(cmd *cobra.Command, args []string) (err error) {
	if len(args) != 1 {
		return errors.New("`get` requires only a path argument")
	}

	path := args[0]

	_, err = c.Get(context.Background(), &pb.GetRequest{Path: path})

	return
}

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get <source>",
	Short: "Download a file or folder",
	RunE:  get,
}

func init() {
	RootCmd.AddCommand(getCmd)
}
