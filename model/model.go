package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var (
	key        string
	value      string
	conditions string
	str        string
)

type Model struct {
	link      *sql.DB  //存储连接对象
	tableName string   //存储表名
	field     string   //存储字段
	allFields []string //存储当前表所有字段
	where     string   //存储where条件
	order     string   //存储order条件
	limit     string   //存储limit条件
}

//构造方法
func NewModel(table string) Model {
	var this Model
	this.field = "*"
	//1.存储操作的表名
	this.tableName = table
	//2.初始化连接数据库
	this.GetConnect()
	//3.获得当前表的所有字段
	this.getFields()
	return this
}

// 初始化连接数据库操作
func (this *Model) GetConnect() {
	this.link = Init()
}

// 释放数据库连接
func (this *Model) ReleaseConnect() {
	// this.link.close()
}

/**
 * 获取当前表的所有字段
 */
func (this *Model) getFields() {

	//查看表结构
	sql := "DESC " + this.tableName
	//执行并发送SQL
	result, err := this.link.Query(sql)

	if err != nil {
		fmt.Printf("sql fail ! [%s]", err)
	}

	this.allFields = make([]string, 0)

	for result.Next() {
		var field string
		var Type interface{}
		var Null string
		var Key string
		var Default interface{}
		var Extra string
		err := result.Scan(&field, &Type, &Null, &Key, &Default, &Extra)
		if err != nil {
			fmt.Printf("scan fail ! [%s]", err)
		}
		this.allFields = append(this.allFields, field)
	}

}

// 根据特定条件的查询
func (this *Model) Query(sql string) interface{} {
	res, err := this.link.Query(sql)
	//查询数据，取所有字段
	if err != nil {
		return returnRes(0, ``, err)
	}
	//返回所有列名
	cols, err := res.Columns()
	if err != nil {
		return returnRes(0, ``, err)
	}
	//一行所有列的值，每个值单独用[]byte表示
	vals := make([][]byte, len(cols))
	//相当于保存每个值的地址
	scans := make([]interface{}, len(cols))
	//这里scans引用vals，把数据填充到[]byte里
	for k, _ := range vals {
		scans[k] = &vals[k]
	}
	result := make(map[int]map[string]string)
	for i := 0; res.Next(); i++ {
		res.Scan(scans...)
		row := make(map[string]string)
		for k, v := range vals {
			key := cols[k]
			row[key] = string(v)
		}
		result[i] = row
	}
	return returnRes(1, result, "success")
}
func (this *Model) GetAll() interface{} {
	sql := `select ` + this.field + ` from ` + this.tableName + ` ` + this.where + ` ` + this.order + ` ` + this.limit
	result := this.Query(sql)
	return result
}
func (this *Model) find(id int) interface{} {
	where := ` where user_id = ` + strconv.Itoa(id)
	sql := `select ` + this.field + ` from ` + this.tableName + where + ` limit 1`
	result := this.Query(sql)
	return result
}
func (this *Model) Field(field string) *Model {
	this.field = field
	return this
}

/**
 * order排序条件
 * @param string  $order  以此为基准进行排序
 * @return $this  返回自己，保证连贯操作
 */
func (this *Model) Order(order string) *Model {
	this.order = `order by ` + order
	return this
}

/**
 * limit条件
 * @param string $limit 输入的limit条件
 * @return $this 返回自己，保证连贯操作
 */
func (this *Model) Limit(limit int) *Model {
	this.limit = "limit " + strconv.Itoa(limit)
	return this
}

/**
 * where条件
 * @param string $where 输入的where条件
 * @return $this 返回自己，保证连贯操作
 */
func (this *Model) Where(where string) *Model {
	this.where = `where ` + where
	return this
}

/**
 * 统计总条数
 * @return int 返回总数
 */
func (this *Model) count() interface{} {
	//准备SQL语句
	sql := `select count(*) as total from ` + this.tableName + ` limit 1`
	result := this.Query(sql)
	return returnRes(1, result, "success")
}

/**
 * 执行并发送SQL语句(增删改)
 * @param string $sql 要执行的SQL语句
 * @return bool|int|string 添加成功则返回上一次操作id,删除修改操作则返回true,失败则返回false
 */
func (this *Model) Exec(sql string) interface{} {

	res, err := this.link.Exec(sql)

	if err != nil {
		return returnRes(0, ``, err)
	}

	result, err := res.LastInsertId()
	if err != nil {
		return returnRes(0, ``, err)
	}
	return returnRes(1, result, "success")
}

/**
 * 添加操作
 * @param array  $data 要添加的数组
 * @return bool|int|string 添加成功则返回上一次操作的id,失败则返回false
 */
func (this *Model) add(data map[string]interface{}) interface{} {

	//过滤非法字段
	for k, v := range data {
		if res := in_array(k, this.allFields); res != true {
			delete(data, k)
		} else {
			key += `,` + k
			value += `,` + `'` + v.(string) + `'`
		}
	}

	//将map中取出的键转为字符串拼接
	key = strings.TrimLeft(key, ",")
	//将map中的值转化为字符串拼接
	value = strings.TrimLeft(value, ",")
	//准备SQL语句
	sql := `insert into ` + this.tableName + ` (` + key + `) values (` + value + `)`
	// //执行并发送SQL
	result := this.Exec(sql)

	return result

}

/**
 * 删除操作
 * @param string $id 要删除的id
 * @return bool  删除成功则返回true,失败则返回false
 */
func (this *Model) delete(user_id int) interface{} {

	//判断id是否存在
	if this.where == "" {
		conditions = `where user_id = ` + strconv.Itoa(user_id)
	} else {
		conditions = this.where + ` and user_id = ` + strconv.Itoa(user_id)
	}

	sql := `delete from ` + this.tableName + ` ` + conditions

	//执行并发送
	result := this.Exec(sql)

	return result
}

/**
 * 修改操作
 * @param  array $data  要修改的数组
 * @return bool 修改成功返回true，失败返回false
 */
func (this *Model) update(data map[string]interface{}) interface{} {

	//过滤非法字段
	for k, v := range data {
		if res := in_array(k, this.allFields); res != true {
			delete(data, k)
		} else {
			str += k + ` = '` + v.(string) + `',`
		}
	}

	//去掉最右侧的逗号
	str = strings.TrimRight(str, ",")

	//判断是否有条件
	if this.where == "" {
		fmt.Println("没有条件")
	}

	sql := `update ` + this.tableName + ` set ` + str + ` ` + this.where

	result := this.Exec(sql)
	return result
}

//是否存在数组内
func in_array(need interface{}, needArr []string) bool {
	for _, v := range needArr {
		if need == v {
			return true
		}
	}
	return false
}

//返回json
func returnRes(errCode int, res interface{}, msg interface{}) string {
	result := make(map[string]interface{})
	result["errCode"] = errCode
	result["result"] = res
	result["msg"] = msg
	data, _ := json.Marshal(result)
	return string(data)
}

// func main() {

// 	M := NewModel("ecm_user")
// 	//查询链式操作
// 	// res :=M.Field("user_id,user_name").Order("user_id desc").Where("user_id = 1").Limit(1).get()

// 	//添加操作
// 	// data := make(map[string]interface{})
// 	// data["ddd"] = "118284901@qq.com"
// 	// data["daaa"] = "118284901@qq.com"
// 	// data["email"] = "118284901@qq.com"
// 	// data["user_name"] = "张三"
// 	// data["add_time"] = time.Unix(time.Now().Unix(),0).Format("2006-01-02 15:04:05")
// 	// res:=M.add(data)

// 	//删除操作
// 	// res :=M.delete(18)

// 	//更新操作
// 	// data := make(map[string]interface{})
// 	// data["email"] = "118284901@qq.com"
// 	// data["user_name"] = "打啊"
// 	// data["add_time"] = time.Unix(time.Now().Unix(),0).Format("2006-01-02 15:04:05")
// 	// res:=M.Where("user_id = 1").update(data)
// 	fmt.Println(res)

// }
