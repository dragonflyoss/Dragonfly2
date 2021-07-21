package proxy

import (
	"fmt"
	"io"
	"net"
	"sync"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/config"
)

type Proxy interface {
	Serve() error
	Stop()
}

type proxy struct {
	from string
	to   string
	done chan struct{}
}

func New(cfg *config.RedisConfig) Proxy {
	return &proxy{
		from: fmt.Sprintf(":%d", cfg.Port),
		to:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		done: make(chan struct{}),
	}
}

func (p *proxy) Serve() error {
	logger.Infof("proxy start to listen port %s", p.from)
	listener, err := net.Listen("tcp", p.from)
	if err != nil {
		return err
	}

	for {
		select {
		case <-p.done:
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				logger.Errorf("error accepting conn %v", err)
			} else {
				go p.handleConn(conn)
			}
		}
	}
}

func (p *proxy) Stop() {
	logger.Infof("proxy stop to listen port %s", p.from)
	if p.done == nil {
		return
	}
	close(p.done)
	p.done = nil
}

func (p *proxy) handleConn(conn net.Conn) {
	logger.Infof("handling", conn)
	defer logger.Infof("done handling", conn)
	defer conn.Close()
	rConn, err := net.Dial("tcp", p.to)
	if err != nil {
		logger.Errorf("error dialing remote host %v", err)
		return
	}
	defer rConn.Close()

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go p.copy(rConn, conn, wg)
	go p.copy(conn, rConn, wg)
	wg.Wait()
}

func (p *proxy) copy(from, to net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	select {
	case <-p.done:
		return
	default:
		if _, err := io.Copy(to, from); err != nil {
			logger.Errorf("error copy %v", err)
			p.Stop()
			return
		}
	}
}
