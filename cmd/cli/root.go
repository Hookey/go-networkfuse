package cli

import (
	"log"
	"os"

	fs "github.com/Hookey/go-networkfuse/api/client"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var c *fs.Client

func initClient(cmd *cobra.Command, args []string) (err error) {
	address, _ := cmd.Flags().GetString("addr")
	c, err = fs.NewClient(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return err
}

func closeClient(cmd *cobra.Command, args []string) error {
	if c != nil {
		return c.Close()
	}
	return nil
}

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:                "nfs",
	Short:              "A command line tool for network fuse",
	SilenceUsage:       true,
	PersistentPreRunE:  initClient,
	PersistentPostRunE: closeClient,
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(-1)
	}
}

func init() {
	RootCmd.PersistentFlags().String("addr", "localhost:50052", "API address")
}
