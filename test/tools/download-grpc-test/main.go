package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"
)

var (
	subDir = flag.String("sub-dir", "", "")
)

func main() {
	flag.Parse()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return net.Dial("unix", addr)
	}

	unixAddr := "/run/dfdaemon.sock"

	// create grpc conn via unix domain socket
	conn, err := grpc.DialContext(
		context.Background(),
		unixAddr,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// initial daemon client
	var (
		client = dfdaemonv1.NewDaemonClient(conn)
		ctx    = context.Background()
	)

	// initial download request
	request := dfdaemonv1.DownRequest{
		Uuid:              "95305fa2-138b-4466-acec-62865ab6403c",
		Url:               "s3://minio-test-bucket/dragonfly-test/usr/" + *subDir,
		Output:            "/var/lib/dragonfly-grpc-test/usr/" + *subDir,
		Recursive:         true, // recursive download
		Timeout:           0,
		Limit:             0,
		DisableBackSource: false,
		UrlMeta: &commonv1.UrlMeta{
			Digest: "",
			Tag:    "",
			Range:  "",
			Filter: "Expires&Signature",
			Header: map[string]string{
				"awsEndpoint":         "http://minio.dragonfly-e2e.svc:9000",
				"awsRegion":           "us-west-1",
				"awsAccessKeyID":      "root",
				"awsSecretAccessKey":  "password",
				"awsS3ForcePathStyle": "true",
			},
		},
		Uid:                1000, // target uid
		Gid:                1000, // target gid
		KeepOriginalOffset: false,
	}

	start := time.Now()
	// call daemon client download grpc
	downloadClient, err := client.Download(ctx, &request)
	if err != nil {
		log.Fatal(err)
	}

	// save results
	results := map[string]*dfdaemonv1.DownResult{}
	for {
		resp, err := downloadClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		results[resp.Output] = resp
	}

	for output, result := range results {
		fmt.Printf("output: %s, content length: %d, done: %v\n",
			output, result.CompletedLength, result.Done)
	}
	fmt.Printf("%dms", time.Now().Sub(start).Milliseconds())
}
