package main

import (
	"fmt"
	"log"
	"strings"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocolly/colly"
	"github.com/gocolly/colly/proxy"
)

// https://juejin.im/post/6844903906078801934
// https://www.w3cschool.cn/colly/colly-5z4k30nn.html
// https://blog.csdn.net/qq_36314165/article/details/100189914
// https://www.ctolib.com/topics-135608.html

// OnRequest 请求执行之前调用
// OnResponse 响应返回之后调用
// OnHTML 监听执行
// selectorOnXML 监听执行
// selectorOnHTMLDetach，取消监听，参数为 selector 字符串
// OnXMLDetach，取消监听，参数为 selector 字符串
// OnScraped，完成抓取后执行，完成所有工作后执行
// OnError，错误回调
// Find and visit all links
func main() {

	//数据库配置
	const (
		userName = "root"
		password = "lijun950702"
		ip       = "127.0.0.1"
		port     = "3306"
		dbName   = "movie"
	)

	path := strings.Join([]string{userName, ":", password, "@tcp(", ip, ":", port, ")/", dbName, "?charset=utf8"}, "")
	fmt.Println(path)
	DB, _ := sql.Open("mysql", path)
	//验证连接
	if errConn := DB.Ping(); errConn != nil {
		fmt.Println("open database fail")
		return
	}
	fmt.Println("connnect success")
	defer DB.Close()
	//实例化默认收集器
	c := colly.NewCollector(
		colly.Async(true),
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36"),
		//colly.UserAgent("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"),
	)
	s := []string{"http://182.253.170.38:8080", "socks5://61.135.185.38:8088"}
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 5})
	rp, _ := proxy.RoundRobinProxySwitcher(s...)
	c.SetProxyFunc(rp)
	c.OnRequest(func(r *colly.Request) {
		// Request头部设定
		r.Headers.Set("Method", "POST")
		// r.Headers.Set("Host", "httpbin.org")
		r.Headers.Set("Connection", "keep-alive")
		r.Headers.Set("Accept", "*/*")
		r.Headers.Set("Origin", "")
		// r.Headers.Set("Referer", "http://www.baidu.com")
		r.Headers.Set("Accept-Encoding", "gzip, deflate")
		r.Headers.Set("Accept-Language", "zh-CN, zh;q=0.9")
		log.Println("Visiting", r)
	})

	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Something went wrong:", err)
	})
	// 每个有href的a标签
	c.OnHTML("[pre]", func(e *colly.HTMLElement) {
		fmt.Println("Visited on HTML", e)
		// log.Println(strings.Split(e.ChildAttr("a", "href"), "/")[4],
		// 	strings.TrimSpace(e.DOM.Find("span.title").Eq(0).Text()))
	})

	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Visited", r.Request)
	})

	// c.OnHTML(".paginator a", func(e *colly.HTMLElement) {
	// 	// e.Request.Visit(e.Attr("href"))
	// })
	//初始开始抓取的页面
	// c.PostRaw("http://127.0.0.1:8080/v1/salestype/id/1", nil)
	c.Visit("http://www.baidu.com")
	c.Wait()
}
