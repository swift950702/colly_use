package main

import (
	"fmt"
	"github.com/gocolly/colly"
	"github.com/gocolly/colly/proxy"
	"log"
)

func main() {
	c := colly.NewCollector(
		colly.Async(true),
		colly.UserAgent("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)"),
	)
	temp := []string{
		"http://221.182.31.54:8080",
	}
	rp, err := proxy.RoundRobinProxySwitcher(temp...)
	if err != nil {
		fmt.Println(err)
	}
	c.SetProxyFunc(rp)
	c.OnRequest(func(r *colly.Request) {
	})

	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Something went wrong:", err)
	})

	c.OnHTML("div#content", func(e *colly.HTMLElement) {
	})
	c.OnResponse(func(resp *colly.Response) {
		fmt.Println(resp.StatusCode)
	})

	c.Visit("https://movie.douban.com/subject/1292052/")
	c.Wait()
}
