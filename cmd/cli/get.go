package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func get(cmd *cobra.Command, args []string) (err error) {
	if len(args) != 1 {
		return errors.New("`get` requires only a path argument")
	}

	path := args[0]
	path = strings.TrimRight(path, "/")

	reply, err := c.Get(path)
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

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get <source>",
	Short: "Download a file or folder",
	RunE:  get,
}

func init() {
	RootCmd.AddCommand(getCmd)
}
