package main

import (
	"fmt"
	"github.com/SimonBackx/lantern-crawler/config"
	"github.com/SimonBackx/lantern-crawler/crawler"
	"net/url"
)

func run(quit chan bool, finished chan bool) {
	defer func() {
		finished <- true
	}()

	// Door tor sturen
	conf := &config.CrawlerConfig{
		UseTorProxy:   true,
		OnlyOnion:     false,
		LoadFromFiles: false,
		SaveToFiles:   false,
		//MaxDomains:    1000,

		LogRecrawlingEnabled: false,
		LogGoroutinesEnabled: false,
	}

	// Niet door tor sturen
	//conf := &config.CrawlerConfig{}

	myCrawler := crawler.NewCrawler(conf)

	query := crawler.NewQuery(crawler.NewQueryOperation(crawler.NewQueryRegexp("Simon"), crawler.AndOperator, crawler.NewQueryRegexp("Backx")))
	myCrawler.AddQuery(query)

	//urls := []string{"http://torlinkbgs6aabns.onion/", "http://zqktlwi4fecvo6ri.onion/wiki/index.php/Main_Page", "http://w363zoq3ylux5rf5.onion/"}
	urls := []string{"http://www.scoutswetteren.be"}

	for _, str := range urls {
		u, err := url.ParseRequestURI(str)
		if err == nil {
			myCrawler.ProcessUrl(u, nil)
		} else {
			fmt.Println(err)
		}
	}

	signal := make(chan int, 1)

	go func() {
		<-quit
		fmt.Println("Sending shutdown signal")
		// Stop signaal sturen naar onze crawler
		signal <- 1
	}()

	myCrawler.Start(signal)
}
