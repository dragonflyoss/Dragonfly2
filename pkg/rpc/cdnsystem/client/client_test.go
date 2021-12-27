/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package client

import (
	"net"
	"reflect"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
)

func TestDial(t *testing.T) {
	type args struct {
		target string
		opts   []grpc.DialOption
	}
	tests := []struct {
		name    string
		args    args
		want    CDNClient
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Dial(tt.args.target, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Dial() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Dial() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDialWithTimeout(t *testing.T) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Error while listening. Err: %v", err)
	}
	defer lis.Close()
	lisDone := make(chan struct{})
	dialDone := make(chan struct{})
	// 1st listener accepts the connection and then does nothing
	go func() {
		defer close(lisDone)
		conn, err := lis.Accept()
		if err != nil {
			t.Errorf("Error while accepting. Err: %v", err)
			return
		}
		framer := http2.NewFramer(conn, conn)
		if err := framer.WriteSettings(http2.Setting{}); err != nil {
			t.Errorf("Error while writing settings. Err: %v", err)
			return
		}
		<-dialDone // Close conn only after dial returns.
	}()

	r := rpc.NewD7yResolverBuilder("whatever")
	client, err := Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithResolvers(r))
	close(dialDone)
	if err != nil {
		t.Fatalf("Dial failed. Err: %v", err)
	}
	defer client.Close()
	timeout := time.After(1 * time.Second)
	select {
	case <-timeout:
		t.Fatal("timed out waiting for server to finish")
	case <-lisDone:
	}
}
