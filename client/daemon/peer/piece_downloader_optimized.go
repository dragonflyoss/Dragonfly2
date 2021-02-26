package peer

import (
	"io"
	"net"
	"net/http/httputil"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/client/clientutil"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
)

type optimizedPieceDownloader struct {
}

func NewOptimizedPieceDownloader(opts ...func(*optimizedPieceDownloader) error) (PieceDownloader, error) {
	pd := &optimizedPieceDownloader{}
	for _, opt := range opts {
		if err := opt(pd); err != nil {
			return nil, err
		}
	}
	return pd, nil
}

func (o optimizedPieceDownloader) DownloadPiece(request *DownloadPieceRequest) (io.Reader, io.Closer, error) {
	logger.Debugf("download piece, addr: %s, task: %s, peer: %s, piece: %d",
		request.TaskID, request.DstAddr, request.DstPid, request.piece.PieceNum)
	// TODO get from connection pool
	conn, err := net.Dial("tcp", request.DstAddr)
	if err != nil {
		panic(err)
	}
	// TODO refactor httputil.NewClientConn
	client := httputil.NewClientConn(conn, nil)
	req := buildDownloadPieceHTTPRequest(request)

	// write request to tcp conn
	if err = client.Write(req); err != nil {
		return nil, nil, err
	}
	// read response header
	resp, err := client.Read(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.ContentLength <= 0 {
		logger.Errorf("can not get ContentLength, addr: %s, task: %s, peer: %s, piece: %d",
			request.TaskID, request.DstAddr, request.DstPid, request.piece.PieceNum)
		return nil, nil, errors.New("can not get ContentLength")
	}
	conn, buf := client.Hijack()
	if buf.Buffered() > 0 {
		logger.Warnf("buffer size is not 0, addr: %s, task: %s, peer: %s, piece: %d",
			request.TaskID, request.DstAddr, request.DstPid, request.piece.PieceNum)
		return io.LimitReader(clientutil.BufferReader(buf, conn), resp.ContentLength), conn, nil
	}
	return io.LimitReader(conn, resp.ContentLength), conn, nil
}
