package main

import (
	"log"

	"github.com/gocolly/colly"
)

func main() {
	c := colly.NewCollector()

	c.OnResponse(func(r *colly.Response) {
		log.Println(string(r.Body))
		if !visited {
			visited = true
			r.Request.Visit("/get?q=2")
		}
	})

	c.Visit("http://httpbin.org/get")
}
