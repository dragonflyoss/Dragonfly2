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

package rpcserver

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	_ "d7y.io/dragonfly/v2/pkg/source/httpprotocol"
	_ "d7y.io/dragonfly/v2/pkg/source/ossprotocol"
)

func TestPluginsTestSuite(t *testing.T) {
	suite.Run(t, new(RPCServerTestSuite))
}

type RPCServerTestSuite struct {
	suite.Suite
	*Server
}

func (s *RPCServerTestSuite) SetUpSuite() {
	s.Server = &Server{
		service: nil,
		config:  Config{},
	}
}

func (s *RPCServerTestSuite) TestCdnSeedServer_GetPieceTasks() {
	type args struct {
		ctx context.Context
		req *base.PieceTaskRequest
	}
	tests := []struct {
		name            string
		args            args
		wantPiecePacket *base.PiecePacket
		wantErr         bool
	}{
		//{},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			gotPiecePacket, err := s.GetPieceTasks(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				//s.FailNowf("GetPieceTasks() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPiecePacket, tt.wantPiecePacket) {
				s.FailNowf("", "GetPieceTasks() gotPiecePacket = %v, want %v", gotPiecePacket, tt.wantPiecePacket)
			}
		})
	}
}

func (s *RPCServerTestSuite) TestCdnSeedServer_ObtainSeeds() {
	type args struct {
		ctx context.Context
		req *cdnsystem.SeedRequest
		psc chan *cdnsystem.PieceSeed
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		//{
		//	name: "testObtain",
		//	args: args{
		//		ctx: context.Background(),
		//		req: &cdnsystem.SeedRequest{
		//			TaskId: uuid.Generate().String(),
		//			Url:    "http://ant:sys@fileshare.glusterfs.svc.eu95.alipay.net/misc/d7y-test/blobs/sha256/16M",
		//			UrlMeta: &base.UrlMeta{
		//				Digest: "",
		//				Tag:    "",
		//				Range:  "",
		//				Filter: "",
		//				Header: nil,
		//			},
		//		},
		//		psc: make(chan *cdnsystem.PieceSeed, 4),
		//	},
		//},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			if err := s.ObtainSeeds(tt.args.ctx, tt.args.req, tt.args.psc); (err != nil) != tt.wantErr {
				s.FailNowf("", "ObtainSeeds() error = %v, wantErr %v", err, tt.wantErr)
			}
			//if !reflect.DeepEqual(got, tt.want) {
			//	t.Errorf("NewCdnSeedServer() got = %v, want %v", got, tt.want)
			//}
		})
	}
}
