package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/proxy"
)

func main() {
	start := time.Now()
	//实例化默认收集器
	c := colly.NewCollector(
		colly.Async(true),
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36"),
	)

	temp := []string{
		"http://114.55.95.112:8999",
	}
	rp, err := proxy.RoundRobinProxySwitcher(temp...)
	if err != nil {
		fmt.Println(err)
	}
	c.SetProxyFunc(rp)
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 5})
	c.OnRequest(func(r *colly.Request) {
		log.Println("Visiting", r.URL)
	})
	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Something went wrong:", err)
	})
	// 每个有href的a标签
	c.OnHTML("tr", func(e *colly.HTMLElement) {
	})
	c.OnResponse(func(resp *colly.Response) {
		fmt.Println(string(resp.Body))
	})
	c.Visit("http://httpbin.org/get")
	c.Wait()
	fmt.Println("花费时间：", time.Since(start))
}
