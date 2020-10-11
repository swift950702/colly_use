import requests,pymysql

def test_proxy(proxy):
    # https_url = "http://ip.tool.chinaz.com/"
    # https_url = "http://httpbin.org/get"
    https_url = "https://movie.douban.com/"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"}
    try:
        proxies = {"http": proxy}
        requests.packages.urllib3.disable_warnings()
        r = requests.get(https_url, headers=headers, verify=False, proxies=proxies, timeout=10)
        # content = r.content.decode("utf-8")
        # root = etree.HTML(content)
        # items = root.xpath('.//li[@class="subject-item"]')

        # print(r.status_code)
        if r.status_code == 200:
            return True
        return False
    except Exception as e:
        msg = str(e)
        return False
if __name__ == "__main__":
    db = pymysql.connect("localhost","root","lijun950702","movie" )
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # SQL 查询语句
    sql = "SELECT * FROM ip"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            temp = "http://"+row[1]+":"+str(row[2])
            if test_proxy(temp)==True:
                print(temp)
    except:
         print ("Error: unable to fetch data")
    # 关闭数据库连接
    db.close()
    # print(test_proxy("http://61.135.186.243:80"))
