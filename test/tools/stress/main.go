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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/go-echarts/statsview"
	"github.com/go-echarts/statsview/viewer"
	"github.com/montanaflynn/stats"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/unit"
)

var (
	target   string
	output   string
	proxy    string
	con      int
	duration *time.Duration
)

func init() {
	flag.StringVar(&target, "url", "", "target url for stress testing, example: http://localhost")
	flag.StringVar(&output, "output", "/tmp/statistics.txt", "all request statistics")
	flag.StringVar(&proxy, "proxy", "", "target proxy for downloading, example: http://127.0.0.1:65001")
	flag.IntVar(&con, "connections", 100, "concurrency count of connections")
	duration = flag.Duration("duration", 100*time.Second, "testing duration")
}

type Result struct {
	StatusCode int
	StartTime  time.Time
	EndTime    time.Time
	Cost       time.Duration
	TaskID     string
	PeerID     string
	Size       int64
	Message    string
}

func main() {
	go debug()

	flag.Parse()

	var (
		wgProcess = &sync.WaitGroup{}
		wgCollect = &sync.WaitGroup{}
	)
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan *Result, 1024)

	if proxy != "" {
		pu, err := url.Parse(proxy)
		if err != nil {
			panic(err)
		}
		http.DefaultClient.Transport = &http.Transport{
			Proxy: http.ProxyURL(pu),
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}

	wgCollect.Add(1)
	go collect(wgCollect, resultCh)

	for i := 0; i < con; i++ {
		wgProcess.Add(1)
		go process(ctx, wgProcess, resultCh)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go forceExit(signals)

loop:
	for {
		select {
		case <-time.After(*duration):
			break loop
		case sig := <-signals:
			log.Printf("receive signal: %v", sig)
			break loop
		}
	}
	cancel()
	wgProcess.Wait()
	close(resultCh)
	wgCollect.Wait()
}

func debug() {
	debugAddr := fmt.Sprintf("%s:%d", ip.IPv4.String(), 18066)
	viewer.SetConfiguration(viewer.WithAddr(debugAddr))
	if err := statsview.New().Start(); err != nil {
		log.Println("stat view start failed", err)
	}
}

func forceExit(signals chan os.Signal) {
	var count int
	for {
		select {
		case <-signals:
			count++
			if count > 2 {
				log.Printf("force exit")
				os.Exit(1)
			}
		}
	}
}

func process(ctx context.Context, wg *sync.WaitGroup, result chan *Result) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		resp, err := http.Get(target)
		if err != nil {
			log.Printf("connect target error: %s", err)
			continue
		}
		var msg string
		n, err := io.Copy(io.Discard, resp.Body)
		if err != nil {
			msg = err.Error()
			log.Printf("discard data error: %s", err)
		}
		end := time.Now()
		result <- &Result{
			StatusCode: resp.StatusCode,
			StartTime:  start,
			EndTime:    end,
			Cost:       end.Sub(start),
			Size:       n,
			TaskID:     resp.Header.Get(config.HeaderDragonflyTask),
			PeerID:     resp.Header.Get(config.HeaderDragonflyPeer),
			Message:    msg,
		}
		resp.Body.Close()
	}
}

func collect(wg *sync.WaitGroup, resultCh chan *Result) {
	defer wg.Done()
	var results = make([]*Result, 0, 1000)
loop:
	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				break loop
			}
			results = append(results, result)
		}
	}

	printStatistics(results)
	saveToOutput(results)
}

func printStatistics(results []*Result) {
	sort.Slice(results, func(i, j int) bool {
		return results[i].Cost < results[j].Cost
	})
	printLatency(results)
	printStatus(results)
	printThroughput(results)
}

func printStatus(results []*Result) {
	var status = make(map[int]int)
	for _, v := range results {
		status[v.StatusCode]++
	}

	fmt.Printf("HTTP codes\n")
	for code, count := range status {
		fmt.Printf("\t%d\t %d\n", code, count)
	}
}

func printLatency(results []*Result) {
	var dur []int64

	for _, v := range results {
		if v.StatusCode == 200 {
			dur = append(dur, v.EndTime.Sub(v.StartTime).Nanoseconds())
		}
	}
	if len(dur) == 0 {
		log.Printf("empty result with 200 status")
		return
	}

	d := stats.LoadRawData(dur)

	min, _ := stats.Min(d)
	max, _ := stats.Max(d)
	mean, _ := stats.Mean(d)
	fmt.Printf("Latency\n")
	fmt.Printf("\tavg\t %v\n", time.Duration(int64(mean)))
	fmt.Printf("\tmin\t %v\n", time.Duration(int64(min)))
	fmt.Printf("\tmax\t %v\n", time.Duration(int64(max)))

	fmt.Printf("Latency Distribution\n")
	for _, p := range []float64{50, 75, 90, 95, 99} {
		percentile, err := stats.Percentile(d, p)
		if err != nil {
			panic(err)
		}
		fmt.Printf("\t%.0f%%\t%v\n", p, time.Duration(int64(percentile)))
	}
}

func printThroughput(results []*Result) {
	var total int64
	for _, v := range results {
		total += v.Size
	}
	fmt.Printf("Throughput\t%v\n", unit.Bytes(total/int64(*duration/time.Second)))
	fmt.Printf("Request\t\t%d/s\n", len(results)/int(*duration/time.Second))
}

func saveToOutput(results []*Result) {
	out, err := os.OpenFile(output, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	for _, v := range results {
		if v.TaskID == "" {
			v.TaskID = "unknown"
		}
		if v.PeerID == "" {
			v.PeerID = "unknown"
		}
		if _, err := out.WriteString(fmt.Sprintf("%s %s %d %v %d %d %s\n",
			v.TaskID, v.PeerID, v.StatusCode, v.Cost,
			v.StartTime.UnixNano()/100, v.EndTime.UnixNano()/100, v.Message)); err != nil {
			log.Panicln("write string failed", err)
		}
	}
}
