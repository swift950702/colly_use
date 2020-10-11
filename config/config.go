package config

import (
	"fmt"

	"github.com/spf13/viper"
)

/**
参数说明：
app.spider_path: 爬虫路由
app.spider_path_name: 爬虫路由名称
app.debug_path: debug的路由
app.debug_path_name: debug的路由名称
cron.timing_spider: 定时爬虫的CRON表达式
ding.access_token: 钉钉机器人token
*/

func Init() (interface{}, error) { //模块中供其他包调用的方法，首字母必须大写
	//viper设置 配置
	viper.Set("name", "abc")
	fmt.Printf("name的值是%v\n", viper.GetString("name"))

	//读取配置文件配置
	viper.AddConfigPath("../config")
	viper.SetConfigName("config")
	error := viper.ReadInConfig()
	/*
		代码解析：
			viper.AddConfigPath("conf")用来指定yaml配置文件的路径
			viper.SetConfigName("config")用来指定配置文件的名称
			viper.ReadInConfig()是解析配置文件的函数，如果配置文件的路径错误获名称错误则解析失败，会报错误
			viper.GetString("database.name")是用来从配置文件中根据层级关系来获取数据
			最后，通过fmt.Println()对数据结果进行输出
	*/
	if error != nil {
		panic(error)
	}
	c := viper.AllSettings() //获取所有配置
	return c, nil
}

//获取mysql数据库配置信息
func GetDatabaseInfo() map[string]interface{} {
	return viper.GetStringMap("common.mysql")
}

//获取环境变量
func GetEnvInfo(env string) string {
	viper.AutomaticEnv()
	return viper.GetString(env)
}
