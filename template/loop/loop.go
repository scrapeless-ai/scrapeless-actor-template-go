package main

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	// import sdk
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless"
	proxy2 "github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/proxy"

	log "github.com/sirupsen/logrus"
)

func main() {
	// New scrapeless actor
	sl := scrapeless.New(scrapeless.WithStorage(), scrapeless.WithProxy())
	defer sl.Close()

	// Get proxy
	proxy, err := sl.Proxy.Proxy(context.TODO(), proxy2.ProxyActor{
		Country:         "us",
		SessionDuration: 10,
	})
	if err != nil {
		panic(err)
	}
	parse, err := url.Parse(proxy)
	if err != nil {
		panic(err)
	}
	// Set up proxy using Golang's native HTTP
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(parse)}}
	// Monitor program exits
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-quit:
			log.Info("quit success")
			return
		default:
			// crawl services
			doCrawl(sl, client)
		}
	}

}

func doCrawl(sl *scrapeless.Actor, client *http.Client) {
	// Get tasks from the queue
	queueResp, err := sl.Storage.GetQueue().Pull(context.TODO(), 1)
	if err != nil {
		log.Error(err)
		panic(err)
	}
	if len(queueResp) == 0 {
		log.Info("no task")
		time.Sleep(time.Second)
		return
	}
	// get data
	data := getData(queueResp[0].Payload)
	// Store data
	// You can use datasetId to store data in a specific dataset
	// Example:
	// 			sl.Storage.GetDataset("datasetId").AddItems(...)
	ok, err := sl.Storage.GetDataset().AddItems(context.Background(), []map[string]any{
		{
			"key": data,
		},
	})
	if err != nil {
		log.Error(err)
		return
	}
	if ok {
		log.Info("ok")
	} else {
		log.Error("Store data error")
		return
	}
	// ack task when task success
	if err := sl.Storage.GetQueue().Ack(context.TODO(), queueResp[0].ID); err != nil {
		log.Error(err)
	}

}

func getData(task any) string {
	//... Logic
	return "data"
}
