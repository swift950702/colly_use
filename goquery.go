package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func main() {
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(10)

	for i := 0; i < 10; i++ {
		go func(a int) {
			defer wg.Done()
			parseUrls("https://strconv.com/posts/web-crawler-exercise-3/")
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Took %s", elapsed)
}
func fetch(url string) *goquery.Document {
	fmt.Println("Fetch Url", url)
	client := &http.Client{}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://110.73.1.204:8123)")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Http get err:", err)
		return nil
	}
	if resp.StatusCode != 200 {
		fmt.Println("Http status code:", resp.StatusCode)
		return nil
	}
	defer resp.Body.Close()
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	return doc
}
func parseUrls(url string) {
	doc := fetch(url)
	fmt.Println(*doc)
	// doc.Find("ol.grid_view li").Find(".hd").Each(func(index int, ele *goquery.Selection) {
	// 	movieUrl, _ := ele.Find("a").Attr("href")
	// 	fmt.Println(strings.Split(movieUrl, "/")[4], ele.Find(".title").Eq(0).Text())
	// })
	time.Sleep(2 * time.Second)
}
