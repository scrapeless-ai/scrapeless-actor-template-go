package main

import (
	"context"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless"
	proxy2 "github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/proxy"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"time"
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

	// crawl services
	doCrawl(sl, client)
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
	// You can use bucketId to store data in a specific Object
	// Example:
	// 			sl.Storage.GetObject("bucketId").Put(...)

	objectId, err := sl.Storage.GetObject().Put(context.TODO(), "key", []byte(data))
	if err != nil {
		log.Error(err)
		return
	}
	log.Info(objectId)
	// ack task when task success
	if err := sl.Storage.GetQueue().Ack(context.TODO(), queueResp[0].ID); err != nil {
		log.Error(err)
	}

}

func getData(task any) string {
	//... Logic
	return "data"
}
