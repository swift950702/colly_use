package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"colly_use/model"

	"github.com/gocolly/colly"
	"github.com/gocolly/colly/proxy"
)

var UserAgent []string = []string{
	"Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19",
	"Mozilla/5.0 (Linux; U; Android 4.0.4; en-gb; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
	"Mozilla/5.0 (Linux; U; Android 2.2; en-gb; GT-P1000 Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
	"Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0",
	"Mozilla/5.0 (Android; Mobile; rv:14.0) Gecko/14.0 Firefox/14.0",
	"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36",
	"Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19",
	"Mozilla/5.0 (iPad; CPU OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
	"Mozilla/5.0 (iPod; U; CPU like Mac OS X; en) AppleWebKit/420.1 (KHTML, like Gecko) Version/3.0 Mobile/3A101a Safari/419.3",
	"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
	"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
	"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
	"Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11",
	"Opera/9.25 (Windows NT 5.1; U; en)",
	"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
	"Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)",
	"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12",
	"Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9",
	"Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
	"Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 ",
}

func main() {
	start := time.Now()
	ips := getIp()
	movieTable := model.NewModel("movie250")
	collyDouban(ips, &movieTable)
	fmt.Println("花费时间：", time.Since(start))
}
func getIp() (ips []string) {
	//数据库配置
	ipTable := model.NewModel("ip")
	ipTable.GetConnect()
	all := ipTable.GetAll()
	var a map[string]interface{}
	json.Unmarshal([]byte(all.(string)), &a)
	for _, v := range a["result"].(map[string]interface{}) {
		ips = append(ips, v.(map[string]interface{})["ip"].(string)+":"+v.(map[string]interface{})["port"].(string))
	}
	return
}

func collyDouban(ips []string, m *model.Model) {
	//实例化默认收集器
	c := colly.NewCollector(
		colly.Async(true),
	)
	c.Limit(&colly.LimitRule{DomainGlob: "*.douban.*", Parallelism: 5, RandomDelay: 5 * time.Second})

	c1 := c.Clone()

	temp := []string{
		"http://47.74.84.218:80",
	}
	rp, err := proxy.RoundRobinProxySwitcher(temp...)
	if err != nil {
		fmt.Println(err)
	}
	c.SetProxyFunc(rp)
	c1.SetProxyFunc(rp)
	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", UserAgent[rand.Intn(15)])
		r.Headers.Set("Host", "http://movie.douban.com")
		r.Headers.Set("Connection", "keep-alive")
		r.Headers.Set("Accept", "*/*")
		r.Headers.Set("Origin", "http://61.135.185.90:80")
		r.Headers.Set("Referer", "https://googleads.g.doubleclick.net/pagead/ads?client=ca-pub-4830389020085397&output=html&h=250&slotname=1983604743&adk=2656724884&adf=367891091&w=300&lmt=1602316115&psa=1&guci=2.2.0.0.2.2.0.0&format=300x250&url=https%3A%2F%2Fmovie.douban.com%2Ftop250%3Fstart%3D0%26filter%3D&flash=0&wgl=1&tt_state=W3siaXNzdWVyT3JpZ2luIjoiaHR0cHM6Ly9hZHNlcnZpY2UuZ29vZ2xlLmNvbSIsInN0YXRlIjowfV0.&dt=1602316115736&bpp=11&bdt=298&idt=30&shv=r20201007&cbv=r20190131&ptt=9&saldr=aa&abxe=1&cookie=ID%3Dcf1215f44c0bfdb7-229de89522c300bd%3AT%3D1598422888%3ART%3D1598422888%3AS%3DALNI_MZ8rwqbW7YbodFq-1FunAvLZSB60g&correlator=6337832098576&frm=20&pv=2&ga_vid=1605997608.1600250295&ga_sid=1602316081&ga_hid=791880235&ga_fc=1&ga_cid=1029445433.1600250295&iag=0&icsg=2147524736&dssz=29&mdo=0&mso=0&u_tz=480&u_his=4&u_java=0&u_h=900&u_w=1440&u_ah=801&u_aw=1440&u_cd=24&u_nplug=3&u_nmime=4&adx=940&ady=328&biw=1440&bih=197&scr_x=0&scr_y=0&eid=21066468&oid=3&pvsid=4208435453415890&pem=80&rx=0&eae=0&fc=896&brdim=0%2C23%2C0%2C23%2C1440%2C23%2C1440%2C801%2C1440%2C197&vis=1&rsz=%7C%7CoeEbr%7C&abl=CS&pfx=0&fu=8192&bc=31&ifi=1&uci=a!1&btvi=1&fsb=1&xpc=IPu5JiyqsS&p=https%3A//movie.douban.com&dtd=82")
		// r.Headers.Set("Accept-Encoding", "gzip, deflate, br")
		r.Headers.Set("Accept-Language", "zh-CN,zh;q=0.9")
		// r.Headers.Set("Cookie", "bid=6j4z-X_hsas; douban-fav-remind=1; __gads=ID=cf1215f44c0bfdb7-229de89522c300bd:T=1598422888:RT=1598422888:S=ALNI_MZ8rwqbW7YbodFq-1FunAvLZSB60g; __utmc=30149280; ll=\"108288\"; _ga=GA1.2.1029445433.1600250295; Hm_lvt_6d4a8cfea88fa457c3127e14fb5fabc2=1600264552,1600264582; Hm_lpvt_6d4a8cfea88fa457c3127e14fb5fabc2=1600264582; _vwo_uuid_v2=DC79BF8AF75619C84426E82CB462D101D|574d6489562859057681b37b39317517; ct=y; __utmz=30149280.1602340441.8.4.utmcsr=cn.bing.com|utmccn=(referral)|utmcmd=referral|utmcct=/; ap_v=0,6.0; __utma=30149280.1029445433.1600250295.1602340441.1602397145.9; push_doumail_num=0; push_noty_num=0; dbcl2=\"157055086:/D+HBc1POKA\"; ck=Uqgk; __utmt=1; __utmv=30149280.15705; douban-profile-remind=1; __utmb=30149280.6.10.1602397145; frodotk=\"28bec1630fff660f7a3c44292ffe322d\"")
		// log.Println("Visiting", r.URL)
	})
	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("Request URL:", r.Request.URL, "failed with response:", r, "\nError:", err)
	})
	c.OnHTML("div.item", func(e *colly.HTMLElement) {
		hd := e.DOM.Find("div.hd")
		detail, _ := hd.Find("a").Attr("href")
		c1.Visit(detail)

	})
	c.OnHTML(".paginator a", func(e *colly.HTMLElement) {
		fmt.Println(e.Attr("href"))
		e.Request.Visit(e.Attr("href"))
	})

	// 电影详情
	var name, year, director, actor, grade, imgurl, movieTypes, place string
	c1.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", UserAgent[rand.Intn(15)])
		r.Headers.Set("Host", "m.douban.com")
		r.Headers.Set("Connection", "keep-alive")
		r.Headers.Set("Accept", "*/*")
		r.Headers.Set("Origin", "https://movie.douban.com")
		r.Headers.Set("Referer", "https://googleads.g.doubleclick.net/pagead/ads?client=ca-pub-4830389020085397&output=html&h=250&slotname=1983604743&adk=2656724884&adf=367891091&w=300&lmt=1602316115&psa=1&guci=2.2.0.0.2.2.0.0&format=300x250&url=https%3A%2F%2Fmovie.douban.com%2Ftop250%3Fstart%3D0%26filter%3D&flash=0&wgl=1&tt_state=W3siaXNzdWVyT3JpZ2luIjoiaHR0cHM6Ly9hZHNlcnZpY2UuZ29vZ2xlLmNvbSIsInN0YXRlIjowfV0.&dt=1602316115736&bpp=11&bdt=298&idt=30&shv=r20201007&cbv=r20190131&ptt=9&saldr=aa&abxe=1&cookie=ID%3Dcf1215f44c0bfdb7-229de89522c300bd%3AT%3D1598422888%3ART%3D1598422888%3AS%3DALNI_MZ8rwqbW7YbodFq-1FunAvLZSB60g&correlator=6337832098576&frm=20&pv=2&ga_vid=1605997608.1600250295&ga_sid=1602316081&ga_hid=791880235&ga_fc=1&ga_cid=1029445433.1600250295&iag=0&icsg=2147524736&dssz=29&mdo=0&mso=0&u_tz=480&u_his=4&u_java=0&u_h=900&u_w=1440&u_ah=801&u_aw=1440&u_cd=24&u_nplug=3&u_nmime=4&adx=940&ady=328&biw=1440&bih=197&scr_x=0&scr_y=0&eid=21066468&oid=3&pvsid=4208435453415890&pem=80&rx=0&eae=0&fc=896&brdim=0%2C23%2C0%2C23%2C1440%2C23%2C1440%2C801%2C1440%2C197&vis=1&rsz=%7C%7CoeEbr%7C&abl=CS&pfx=0&fu=8192&bc=31&ifi=1&uci=a!1&btvi=1&fsb=1&xpc=IPu5JiyqsS&p=https%3A//movie.douban.com&dtd=82")
		r.Headers.Set("Accept-Language", "zh-CN,zh;q=0.9")
		// r.Headers.Set("Cookie", "bid=6j4z-X_hsas; douban-fav-remind=1; __gads=ID=cf1215f44c0bfdb7-229de89522c300bd:T=1598422888:RT=1598422888:S=ALNI_MZ8rwqbW7YbodFq-1FunAvLZSB60g; __utmc=30149280; ll=\"108288\"; _ga=GA1.2.1029445433.1600250295; Hm_lvt_6d4a8cfea88fa457c3127e14fb5fabc2=1600264552,1600264582; Hm_lpvt_6d4a8cfea88fa457c3127e14fb5fabc2=1600264582; _vwo_uuid_v2=DC79BF8AF75619C84426E82CB462D101D|574d6489562859057681b37b39317517; ct=y; __utmz=30149280.1602340441.8.4.utmcsr=cn.bing.com|utmccn=(referral)|utmcmd=referral|utmcct=/; ap_v=0,6.0; __utma=30149280.1029445433.1600250295.1602340441.1602397145.9; push_doumail_num=0; push_noty_num=0; dbcl2=\"157055086:/D+HBc1POKA\"; ck=Uqgk; __utmt=1; __utmv=30149280.15705; douban-profile-remind=1; __utmb=30149280.6.10.1602397145; frodotk=\"28bec1630fff660f7a3c44292ffe322d\"")
		// log.Println("Visiting", r.URL)
	})
	c1.OnError(func(r *colly.Response, err error) {
		fmt.Println("Request URL:", r.Request.URL, "failed with response:", r, "\nError:", err)
	})
	c1.OnHTML("div#content", func(e *colly.HTMLElement) {
		a := []rune(e.DOM.Find("h1 span").Text())
		if len(a) > 6 {
			name = string(a[:len(a)-6])
			year = string(a[len(a)-5 : len(a)-1])
		}
		director = e.DOM.Find("div#info span.attrs").First().Text()
		actor = e.DOM.Find("span.actor span.attrs").First().Text()
		b := string(e.Response.Body)

		pattern3 := `property="v:average">.*<`
		rp3 := regexp.MustCompile(pattern3)
		grade = rp3.FindString(b)[21:24]
		imgurl, _ = e.DOM.Find("a.nbgnbg img").Attr("src")

		pattern4 := `property="v:genre">.*<`
		rp4 := regexp.MustCompile(pattern4)
		types := strings.Join(rp4.FindAllString(b, -1), "")
		pattern5 := "[\u4e00-\u9fa5]+"
		rp5 := regexp.MustCompile(pattern5)
		ts := rp5.FindAllString(types, -1)
		movieTypes = strings.Join(ts, "/")

		imgurl, _ = e.DOM.Find("a.nbgnbg img").Attr("src")

		pattern6 := `制片国家/地区:</span>.*<br/>`
		rp6 := regexp.MustCompile(pattern6)
		place = string([]rune(rp6.FindAllString(b, -1)[0])[15 : len([]rune(rp6.FindAllString(b, -1)[0]))-5])

		sql := fmt.Sprintf("INSERT INTO movie250 (name, detailurl, imgurl, grade, time, country, type, director,actor) VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s');", name, e.Request.URL, imgurl, grade, year, place, movieTypes, director, actor)
		m.Exec(sql)
	})
	c1.OnResponse(func(resp *colly.Response) {
	})
	c1.OnScraped(func(r *colly.Response) {
	})
	c.Visit("https://movie.douban.com/top250?start=0&filter=")
	c.Wait()
	c1.Wait()
}

func collyDetail(details []string, m *model.Model) {
	fmt.Println(details[0])
	var name, year, director, actor, grade, imgurl, movieTypes, place string
	var start = 0
	//实例化默认收集器
	c := colly.NewCollector(
		colly.Async(true),
	)
	c.Limit(&colly.LimitRule{DomainGlob: "*.douban.*", Parallelism: 5})
	// temp := ips[900:910]
	temp := []string{
		"http://47.74.84.218:80",
		"http://46.4.96.137:8080",
	}
	rp, err := proxy.RoundRobinProxySwitcher(temp...)
	if err != nil {
		fmt.Println(err)
	}
	c.SetProxyFunc(rp)

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("User-Agent", UserAgent[rand.Intn(15)])
		r.Headers.Set("Host", "m.douban.com")
		r.Headers.Set("Connection", "keep-alive")
		r.Headers.Set("Accept", "*/*")
		r.Headers.Set("Origin", "https://movie.douban.com")
		r.Headers.Set("Referer", "https://googleads.g.doubleclick.net/pagead/ads?client=ca-pub-4830389020085397&output=html&h=250&slotname=1983604743&adk=2656724884&adf=367891091&w=300&lmt=1602316115&psa=1&guci=2.2.0.0.2.2.0.0&format=300x250&url=https%3A%2F%2Fmovie.douban.com%2Ftop250%3Fstart%3D0%26filter%3D&flash=0&wgl=1&tt_state=W3siaXNzdWVyT3JpZ2luIjoiaHR0cHM6Ly9hZHNlcnZpY2UuZ29vZ2xlLmNvbSIsInN0YXRlIjowfV0.&dt=1602316115736&bpp=11&bdt=298&idt=30&shv=r20201007&cbv=r20190131&ptt=9&saldr=aa&abxe=1&cookie=ID%3Dcf1215f44c0bfdb7-229de89522c300bd%3AT%3D1598422888%3ART%3D1598422888%3AS%3DALNI_MZ8rwqbW7YbodFq-1FunAvLZSB60g&correlator=6337832098576&frm=20&pv=2&ga_vid=1605997608.1600250295&ga_sid=1602316081&ga_hid=791880235&ga_fc=1&ga_cid=1029445433.1600250295&iag=0&icsg=2147524736&dssz=29&mdo=0&mso=0&u_tz=480&u_his=4&u_java=0&u_h=900&u_w=1440&u_ah=801&u_aw=1440&u_cd=24&u_nplug=3&u_nmime=4&adx=940&ady=328&biw=1440&bih=197&scr_x=0&scr_y=0&eid=21066468&oid=3&pvsid=4208435453415890&pem=80&rx=0&eae=0&fc=896&brdim=0%2C23%2C0%2C23%2C1440%2C23%2C1440%2C801%2C1440%2C197&vis=1&rsz=%7C%7CoeEbr%7C&abl=CS&pfx=0&fu=8192&bc=31&ifi=1&uci=a!1&btvi=1&fsb=1&xpc=IPu5JiyqsS&p=https%3A//movie.douban.com&dtd=82")
		// r.Headers.Set("Accept-Encoding", "gzip, deflate, br")
		r.Headers.Set("Accept-Language", "zh-CN,zh;q=0.9")
		r.Headers.Set("Cookie", "bid=6j4z-X_hsas; douban-fav-remind=1; __gads=ID=cf1215f44c0bfdb7-229de89522c300bd:T=1598422888:RT=1598422888:S=ALNI_MZ8rwqbW7YbodFq-1FunAvLZSB60g; __utmc=30149280; ll=\"108288\"; _ga=GA1.2.1029445433.1600250295; Hm_lvt_6d4a8cfea88fa457c3127e14fb5fabc2=1600264552,1600264582; Hm_lpvt_6d4a8cfea88fa457c3127e14fb5fabc2=1600264582; _vwo_uuid_v2=DC79BF8AF75619C84426E82CB462D101D|574d6489562859057681b37b39317517; ct=y; __utmz=30149280.1602340441.8.4.utmcsr=cn.bing.com|utmccn=(referral)|utmcmd=referral|utmcct=/; ap_v=0,6.0; __utma=30149280.1029445433.1600250295.1602340441.1602397145.9; push_doumail_num=0; push_noty_num=0; dbcl2=\"157055086:/D+HBc1POKA\"; ck=Uqgk; __utmt=1; __utmv=30149280.15705; douban-profile-remind=1; __utmb=30149280.6.10.1602397145; frodotk=\"28bec1630fff660f7a3c44292ffe322d\"")
		log.Println("Visiting", r.URL)
	})
	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("Request URL:", r.Request.URL, "failed with response:", r, "\nError:", err)
	})
	c.OnHTML("div#content", func(e *colly.HTMLElement) {
		a := []rune(e.DOM.Find("h1 span").Text())
		if len(a) > 6 {
			name = string(a[:len(a)-6])
			year = string(a[len(a)-5 : len(a)-1])
		}
		director = e.DOM.Find("div#info span.attrs").First().Text()
		actor = e.DOM.Find("span.actor span.attrs").First().Text()
		b := string(e.Response.Body)

		pattern3 := `property="v:average">.*<`
		rp3 := regexp.MustCompile(pattern3)
		grade = rp3.FindString(b)[21:24]
		imgurl, _ = e.DOM.Find("a.nbgnbg img").Attr("src")

		pattern4 := `property="v:genre">.*<`
		rp4 := regexp.MustCompile(pattern4)
		types := strings.Join(rp4.FindAllString(b, -1), "")
		pattern5 := "[\u4e00-\u9fa5]+"
		rp5 := regexp.MustCompile(pattern5)
		ts := rp5.FindAllString(types, -1)
		movieTypes = strings.Join(ts, "/")

		imgurl, _ = e.DOM.Find("a.nbgnbg img").Attr("src")

		pattern6 := `制片国家/地区:</span>.*<br/>`
		rp6 := regexp.MustCompile(pattern6)
		place = string([]rune(rp6.FindAllString(b, -1)[0])[15 : len([]rune(rp6.FindAllString(b, -1)[0]))-5])

		sql := fmt.Sprintf("INSERT INTO movie250 (name, detailurl, imgurl, grade, time, country, type, director,actor) VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s');", name, e.Request.URL, imgurl, grade, year, place, movieTypes, director, actor)
		m.Exec(sql)
	})
	c.OnResponse(func(resp *colly.Response) {
	})
	c.OnScraped(func(r *colly.Response) {
		fmt.Println("Finished", r.Request.URL)
		start++
		c.Visit(details[start])
	})
	fmt.Println(details[0])
	c.Visit(details[0])
	c.Wait()
}

func collyPost() {
	c := colly.NewCollector()
	type data struct {
		Phone string `json:"phone" binding:"required"`
	}
	d := &data{
		Phone: "18190897361",
	}
	da, err := json.Marshal(d)

	if err != nil {
		fmt.Println(err)
	}
	c.OnResponse(func(response *colly.Response) {
		fmt.Println(string(response.Body))
	})
	c.OnRequest(func(r *colly.Request) {
		fmt.Println(r)
		fmt.Println(r.Method)
		r.Headers.Set("Content-Type", "application/json;charset=UTF-8")
		r.Headers.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36")
	})
	c.OnError(func(response *colly.Response, e error) {
		fmt.Println(e)
	})
	c.PostRaw("http://www.××××.com:×××/baseDevice/getUserInfo", da)
}
