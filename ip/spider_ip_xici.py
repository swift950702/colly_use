# -*- coding:utf-8 -*-
import urllib2
import gc
import socket
import functools
import ssl
import sys
from bs4 import BeautifulSoup
import Crawler
# https://www.jianshu.com/p/1236d69337dc
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)

sys.setdefaultencoding(default_encoding)

reload(sys)
sys.path.append("..")
socket.setdefaulttimeout(20.0)
urllib2.socket.setdefaulttimeout(20)
urllib2.disable_warnings = True


def cb_print(str):
    # print str
    pass

# 强制ssl使用TLSv1
def sslwrap(func):
    @functools.wraps(func)
    def bar(*args, **kw):
        kw['ssl_version'] = ssl.PROTOCOL_TLSv1
        return func(*args, **kw)
    return bar

ssl.wrap_socket = sslwrap(ssl.wrap_socket)

ip_arr = []

def get_ip_arr():
    gc.enable()
    try:
        url = 'http://vtp.daxiangdaili.com/ip/?tid=559609709731038&num=2000&delay=1&protocol=https'
        headers = {"User-Agent": "Mozilla/5.0"}
        req = urllib2.Request(url, headers=headers)
        res = urllib2.urlopen(req, timeout=20)
        res = res.read()
        ips_arr = res.split('\r\n')
        return ips_arr
    except Exception as e:
        cb_print('ip_arr_error:{}'.format(e))
    gc.collect()

def get_66_ip(index):
    gc.enable()
    try:
        url = 'http://www.66ip.cn/'+str(index)
        headers = {"User-Agent": "Mozilla/5.0"}
        req = urllib2.Request(url, headers=headers)
        res = urllib2.urlopen(req, timeout=20)
        res = res.read()
        # print res
        soup = BeautifulSoup(res, "html.parser")
        table_arr = soup('table')
        ip_soup_arr = table_arr[len(table_arr)-1]('tr')
        ips_arr = []
        for it in ip_soup_arr:
            if it != ip_soup_arr[0]:
                ip = it('td')[0].string
                port = it('td')[1].string
                ip_port = ip + ':' + port
                ips_arr.append(ip_port)
        return ips_arr
    except Exception as e:
        cb_print('ip_arr_error:{}'.format(e))
    gc.collect()


def get_xici_ip():
    gc.enable()
    try:
        url = 'http://www.xicidaili.com/wn/'
        headers = {"User-Agent": "Mozilla/5.0"}
        req = urllib2.Request(url, headers=headers)
        res = urllib2.urlopen(req, timeout=20)
        res = res.read()
        soup = BeautifulSoup(res, "html.parser")
        table_arr = soup('table')
        ip_soup_arr = table_arr[len(table_arr) - 1]('tr')
        ips_arr = []
        for it in ip_soup_arr:
            if it != ip_soup_arr[0]:
                ip = it('td')[1].string
                port = it('td')[2].string
                ip_port = ip + ':' + port
                ips_arr.append(ip_port)
        return ips_arr
    except Exception as e:
        cb_print('ip_arr_error:{}'.format(e))
    gc.collect()
    pass

# 测试方法
ip_arr = get_xici_ip()
print(ip_arr)
