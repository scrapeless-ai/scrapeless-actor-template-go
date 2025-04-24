package main

import (
	"bytes"
	"context"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless"
	proxy2 "github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/proxy"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/PuerkitoBio/goquery"
	log "github.com/sirupsen/logrus"
)

func main() {
	sl := scrapeless.New(scrapeless.WithStorage(), scrapeless.WithProxy())
	defer sl.Close()
	proxy, err := sl.Proxy.Proxy(context.TODO(), proxy2.ProxyActor{
		Country:         "us",
		SessionDuration: 10,
		Gateway:         "gw-us.scrapeless.io:8789",
	})
	if err != nil {
		panic(err)
	}
	parse, err := url.Parse(proxy)
	if err != nil {
		panic(err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(parse)}}
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-quit:
			log.Info("quit success")
			return
		default:
			doCrawl(sl, client)
		}
	}

}

func doCrawl(sl *scrapeless.Actor, client *http.Client) {
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
	request, err := http.NewRequest(http.MethodGet, queueResp[0].Payload, nil)
	if err != nil {
		panic(err)
	}
	do, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	body, _ := io.ReadAll(do.Body)
	dc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		log.Error(err)
		return
	}
	var (
		urlMapping = make(map[string][]string)
	)
	dc.Find("url").Each(func(i int, selection *goquery.Selection) {
		url := selection.Find("loc").Text()
		var urlLanguage []string
		selection.Contents().Each(func(i int, s *goquery.Selection) {
			urlLan := s.AttrOr("href", "")
			if urlLan != "" {
				urlLanguage = append(urlLanguage, urlLan)
			}
		})
		urlMapping[url] = append(urlLanguage, url)
	})
	for _, u := range urlMapping {
		var titles []map[string]any
		for _, v := range u {
			title := getTitle(client, v)
			titles = append(titles, map[string]any{
				"url":   v,
				"title": title,
			})
		}
		ok, err := sl.Storage.GetDataset().AddItems(context.Background(), titles)
		if err != nil {
			log.Error(err)
		}
		if ok {
			log.Info("ok")
		}
		break
	}
	if err := sl.Storage.GetQueue().Ack(context.TODO(), queueResp[0].ID); err != nil {
		log.Error(err)
	}
}

func getTitle(client *http.Client, url string) string {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		panic(err)
	}
	do, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	body, _ := io.ReadAll(do.Body)
	dc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		log.Error(err)
		return ""
	}
	title := dc.Find("title").Text()
	return title
}
