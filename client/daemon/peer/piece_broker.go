package peer

type pieceBroker struct {
	stopCh    chan struct{}
	publishCh chan *pieceInfo
	subCh     chan chan *pieceInfo
	unsubCh   chan chan *pieceInfo
}

type pieceInfo struct {
	num      int32
	finished bool
}

func newPieceBroker() *pieceBroker {
	return &pieceBroker{
		stopCh:    make(chan struct{}),
		publishCh: make(chan *pieceInfo, 10),
		subCh:     make(chan chan *pieceInfo),
		unsubCh:   make(chan chan *pieceInfo),
	}
}

func (b *pieceBroker) Start() {
	subs := map[chan *pieceInfo]struct{}{}
	for {
		select {
		case <-b.stopCh:
			//for msgCh := range subs {
			//	close(msgCh)
			//}
			return
		case msgCh := <-b.subCh:
			subs[msgCh] = struct{}{}
		case msgCh := <-b.unsubCh:
			delete(subs, msgCh)
		case msg := <-b.publishCh:
			for msgCh := range subs {
				// msgCh is buffered, use non-blocking send to protect the broker:
				select {
				case msgCh <- msg:
				default:
				}
			}
		}
	}
}

func (b *pieceBroker) Stop() {
	close(b.stopCh)
}

func (b *pieceBroker) Subscribe() chan *pieceInfo {
	msgCh := make(chan *pieceInfo, 5)
	select {
	case <-b.stopCh:
	case b.subCh <- msgCh:
	}

	return msgCh
}

func (b *pieceBroker) Unsubscribe(msgCh chan *pieceInfo) {
	b.unsubCh <- msgCh
}

func (b *pieceBroker) Publish(msg *pieceInfo) {
	select {
	case b.publishCh <- msg:
	case <-b.stopCh:
	}
}
