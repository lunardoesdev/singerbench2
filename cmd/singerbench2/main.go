package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"time"

	"flag"

	"github.com/lunardoesdev/singerbox"
)

func getFreePort() int {
	for {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			continue
		}
		defer ln.Close()

		return ln.Addr().(*net.TCPAddr).Port
	}
}

func Measure(link string, maxDelay float64) (ping int64, err error) {
	port := getFreePort()

	proxy, err := singerbox.FromSharedLink(
		link,
		singerbox.ProxyConfig{
			ListenAddr: fmt.Sprintf("127.0.0.1:%d", port),
		},
	)
	if err != nil {
		return 0, err
	}
	defer proxy.Stop()

	proxyURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	if err != nil {
		return 0, err
	}

	var (
		connectStart time.Time
		connectDone  time.Time
		gotFirstByte time.Time
	)

	start := time.Now()

	trace := &httptrace.ClientTrace{
		ConnectStart: func(network, addr string) {
			connectStart = time.Now()
		},
		ConnectDone: func(network, addr string, err error) {
			connectDone = time.Now()
		},
		GotFirstResponseByte: func() {
			gotFirstByte = time.Now()
		},
	}

	httpClient := &http.Client{
		Timeout: time.Duration(maxDelay) * time.Millisecond,
		Transport: &http.Transport{
			Proxy:             http.ProxyURL(proxyURL),
			DisableKeepAlives: true,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxDelay)*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(
		httptrace.WithClientTrace(ctx, trace),
		http.MethodGet,
		"http://cachefly.cachefly.net/1mb.test",
		nil,
	)
	if err != nil {
		return 0, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return 0, err
	}

	end := time.Now()

	var pingDur time.Duration
	if !connectStart.IsZero() && !connectDone.IsZero() {
		pingDur = connectDone.Sub(connectStart)
	}

	// var firstByteDur time.Duration
	if !gotFirstByte.IsZero() {
		// firstByteDur = gotFirstByte.Sub(start)
	}

	lastByteDur := end.Sub(start)

	_ = pingDur
	return lastByteDur.Milliseconds(), nil
}

func spawnMeasureWorker(links chan string, maxDelay float64) {
myloop:
	for {
		select {
		case link := <-links:
			ping, err := Measure(link, maxDelay)
			if err != nil {
				//log.Printf("Warning: %v\n", err)
				break
			}

			fmt.Println(link)

			_ = ping
		case <-time.After(3 * time.Second):
			break myloop
		}
	}
}

func spawnGoMeasurer(threads int, maxDelay float64) chan string {
	channel := make(chan string, threads)
	for i := 0; i < threads; i++ {
		go spawnMeasureWorker(channel, maxDelay)
	}

	return channel
}

func main() {
	var maxDelay float64 = 500
	flag.Float64Var(&maxDelay, "max-delay-ms", 500, "max delay for downloading 1mb in milliseconds")

	flag.Parse()
	toWorker := spawnGoMeasurer(100, maxDelay)

	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("Usage: singerbench2 subscription-url")
	}

	for _, subscription := range args {
		resp, err := http.Get(subscription)
		if err != nil {
			//log.Printf("Warning: coudn't get %v: %v", subscription, err)
			continue //yes, skip subscription
		}

		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}

			trimmedLine := strings.TrimSpace(line)
			toWorker <- trimmedLine
		}
		resp.Body.Close()
	}
}
