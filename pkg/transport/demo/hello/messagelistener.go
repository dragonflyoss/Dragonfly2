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

package hello

import (
	"time"
)

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/transport"
)

type MessageHandler struct {
	SessionOnOpen func(session transport.Session)
}

func (h *MessageHandler) OnOpen(session transport.Session) error {
	log.Infof("OnOpen session{%s} open", session.Stat())
	if h.SessionOnOpen != nil {
		h.SessionOnOpen(session)
	}
	return nil
}

func (h *MessageHandler) OnError(session transport.Session, err error) {
	log.Infof("OnError session{%s} got error{%v}, will be closed.", session.Stat(), err)
}

func (h *MessageHandler) OnClose(session transport.Session) {
	log.Infof("OnClose session{%s} is closing......", session.Stat())
}

func (h *MessageHandler) OnMessage(session transport.Session, pkg interface{}) {
	s, ok := pkg.(string)
	if !ok {
		log.Infof("illegal package{%#v}", pkg)
		return
	}

	log.Infof("OnMessage: %s", s)
}

func (h *MessageHandler) OnCron(session transport.Session) {
	active := session.GetActive()
	if CronPeriod < time.Since(active).Nanoseconds() {
		log.Infof("OnCorn session{%s} timeout{%s}", session.Stat(), time.Since(active).String())
		session.Close()
	}
}
