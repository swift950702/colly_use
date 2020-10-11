package main

import (
	"colly_use/model"
	"colly_use/redisgo"
	util "colly_use/utils"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/gocolly/colly"
)

func main() {
	start := time.Now()
	//数据库配置
	ipTable := model.NewModel("ip")
	ipTable.GetConnect()
	redisgo.NewConnection("redis", "../config/configredis.ini")
	key := "all_ip"
	all := ipTable.GetAll()
	var a map[string]interface{}
	json.Unmarshal([]byte(all.(string)), &a)
	var ips []int64
	for _, v := range a["result"].(map[string]interface{}) {
		ips = append(ips, util.IpToBite(v.(map[string]interface{})["ip"].(string)))
	}
	util.QuickSort(ips, 0, len(ips)-1)
	ipsjson, _ := json.Marshal(ips)
	redisgo.RedigoConn.String.Set(key, ipsjson, 10000)
	v, _ := redisgo.RedigoConn.String.Get(key).Result()
	var s []int64
	json.Unmarshal(v.([]byte), &s)
	//实例化默认收集器
	c := colly.NewCollector(
		colly.Async(true),
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36"),
	)
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 5})
	c.OnRequest(func(r *colly.Request) {
		log.Println("Visiting", r.URL)
	})
	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Something went wrong:", err)
	})
	// 每个有href的a标签
	c.OnHTML("tr", func(e *colly.HTMLElement) {
		row := []interface{}{}
		e.ForEach("td", func(i int, elem *colly.HTMLElement) {
			row = append(row, elem.Text)
		})
		if len(row) > 0 {
			if util.BinarySearch(s, util.IpToBite(row[1].(string))) == -1 {
				sql := fmt.Sprintf("INSERT INTO ip (ip, port, nettype, type, location, isp) VALUES ('%s', %s, '%s', '%s', '%s', '%s');", row[0].(string), row[1].(string), row[3].(string), row[2].(string), row[6].(string), row[4].(string))
				ipTable.Exec(sql)
			} else {
				fmt.Println(row[0], row[1], row[3], row[2], row[6], row[4])
			}
		}
	})
	c.OnResponse(func(resp *colly.Response) {
		reg := regexp.MustCompile(`[1-9]\d*`)
		var url = fmt.Sprintf("%v", resp.Request.URL)
		result := reg.FindAllString(url, -1)
		resultstr, _ := strconv.Atoi(result[0])
		resultpage := resultstr + 1
		c.Visit("https://ip.jiangxianli.com/?page=" + strconv.Itoa(resultpage))

		// goquery直接读取resp.Body的内容
		// htmlDoc, err := goquery.NewDocumentFromReader(bytes.NewReader(resp.Body))

		// 读取url再传给goquery，访问url读取内容，此处不建议使用
		// htmlDoc, err := goquery.NewDocument(resp.Request.URL.String())

		// if err != nil {
		// 	log.Fatal(err)
		// }
		// c.Visit("https://ip.jiangxianli.com/?page=1")

		// 找到抓取项 <div class="hotnews" alog-group="focustop-hotnews"> 下所有的a解析
		// htmlDoc.Find("a").Each(func(i int, s *goquery.Selection) {
		// 	band, _ := s.Attr("href")
		// 	title := s.Text()
		// 	fmt.Printf(" %d: %s - %s\n", i, title, band)

		// })

	})
	c.Visit("https://ip.jiangxianli.com/?page=1")
	c.Wait()
	fmt.Println("花费时间：", time.Since(start))
}
