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

package progress

import (
	"d7y.io/dragonfly/v2/cdnsystem/supervisor/mock"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"github.com/golang/mock/gomock"
	_ "github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"testing"
	"time"
)

func TestNewMockSeedProgressMgr(t *testing.T) {
	//assert := testifyassert.New(t)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	NewMock := mock.NewMockSeedProgressMgr(ctl)
	subContext, _ := context.WithTimeout(context.Background(), 5*time.Second)
	var Pieces []*types.SeedPiece
	Pieces, _ = NewMock.GetPieces(subContext, "123456")
	if Pieces == nil {
		t.Errorf("no pieces")
	}

	NewMock.InitSeedProgress(subContext, "123456")

}
