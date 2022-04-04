# 使用golang链接达梦数据库
# Go DM 数据库驱动介绍
> Go 语言标准库 database/sql（https://golang.google.cn/pkg/database/sql/）提供了一系列数据库操作的标准接口，DM 数据库基于 GO1.13 版本通过实现 database/sql 包的接口，向开发人员提供 DM 数据库操作的 Go 语言接口。 

# 环境准备
* 达梦安装后在安装目录的drivers下有go目录，该目录下面的dm-go-driver.zip

![image](images/i3y8cnkDoLzz4VGbbt5tnrAx_m5oTi0t0ok8-5pNdYg.png)

* 建立go工程

```bash
[root@vmpc ~]# mkdir go_dm8
[root@vmpc ~]# cd go_dm8/
[root@vmpc go_dm8]# mkdir src
[root@vmpc go_dm8]# cd src/
[root@vmpc src]# mkdir app
[root@vmpc src]# cd app
```
vim main.go

```go
/*该例程实现插入数据，修改数据，删除数据，数据查询等基本操作。*/
package main

// 引入相关包
import (
        "database/sql"
        "dm"
        "fmt"
        "io/ioutil"
        "time"
)

var db *sql.DB
var err error

func main() {
        driverName := "dm"
        dataSourceName := "dm://SYSDBA:SYSDBA@localhost:5236"

        if db, err = connect(driverName, dataSourceName); err != nil {
                fmt.Println(err)
                return
        }
        if err = insertTable(); err != nil {
                fmt.Println(err)
                return
        }
        if err = updateTable(); err != nil {
                fmt.Println(err)
                return
        }
        if err = queryTable(); err != nil {
                fmt.Println(err)
                return
        }
        if err = deleteTable(); err != nil {
                fmt.Println(err)
                return
        }
        if err = disconnect(); err != nil {
                fmt.Println(err)
                return
        }
}

/* 创建数据库连接 */
func connect(driverName string, dataSourceName string) (*sql.DB, error) {
        var db *sql.DB
        var err error
        if db, err = sql.Open(driverName, dataSourceName); err != nil {
                return nil, err
        }
        if err = db.Ping(); err != nil {
                return nil, err
        }
        fmt.Printf("connect to \"%s\" succeed.\n", dataSourceName)
        return db, nil
}

/* 往产品信息表插入数据 */
func insertTable() error {
        var inFileName = "sanguo.txt"
        var sql = `INSERT INTO production.product(name,author,publisher,publishtime,
                                product_subcategoryid,productno,satetystocklevel,originalprice,nowprice,discount,
                                description,photo,type,papertotal,wordtotal,sellstarttime,sellendtime)
                VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17);`
        data, err := ioutil.ReadFile(inFileName)
        if err != nil {
                return err
        }
        t1, _ := time.Parse("2006-Jan-02", "2005-Apr-01")
        t2, _ := time.Parse("2006-Jan-02", "2006-Mar-20")
        t3, _ := time.Parse("2006-Jan-02", "1900-Jan-01")
        _, err = db.Exec(sql, "三国演义", "罗贯中", "中华书局", t1, 4, "9787101046121", 10, 19.0000, 15.2000,
                8.0,
                "《三国演义》是中国第一部长篇章回体小说，中国小说由短篇发展至长篇的原因与说书有关。",
                data, "25", 943, 93000, t2, t3)
        if err != nil {
                return err
        }
        fmt.Println("insertTable succeed")
        return nil
}

/* 修改产品信息表数据 */
func updateTable() error {
        var sql = "UPDATE production.product SET name = :name WHERE productid = 11;"
        if _, err := db.Exec(sql, "三国演义（上）"); err != nil {
                return err
        }
        fmt.Println("updateTable succeed")
        return nil
}

/* 查询产品信息表 */
func queryTable() error {
        var productid int
        var name string
        var author string
        var description dm.DmClob
        var photo dm.DmBlob
        var sql = "SELECT productid,name,author,description,photo FROM production.product WHERE productid=11"
        rows, err := db.Query(sql)
        if err != nil {
                return err
        }
        defer rows.Close()

        fmt.Println("queryTable results:")
        for rows.Next() {
                if err = rows.Scan(&productid, &name, &author, &description, &photo); err != nil {
                        return err
                }
                blobLen, _ := photo.GetLength()
                fmt.Printf("%v %v %v %v %v\n", productid, name, author, description, blobLen)
        }
        return nil
}

/* 删除产品信息表数据 */
func deleteTable() error {
        var sql = "DELETE FROM production.product WHERE productid = 11;"
        if _, err := db.Exec(sql); err != nil {
                return err
        }
        fmt.Println("deleteTable succeed")
        return nil
}

/* 关闭数据库连接 */
func disconnect() error {
        if err := db.Close(); err != nil {
                fmt.Printf("db close failed: %s.\n", err)
                return err
        }
        fmt.Println("disconnect succeed")
        return nil
}
```


将解压后的dm-go-driver.zip后的达梦go驱动放入工程目录

```bash
[root@vmpc dm8]# unzip dm-go-driver.zip
[root@vmpc dm8]# mv dm ../go_dm8/src/
```


设置环境变量

```bash
[root@vmpc go_dm8]# export GOPATH=/root/go_dm8
[root@vmpc go_dm8]# export GO111MODULE="off"
```


手工下载** golang.org/x/text **

```bash
[root@vmpc go_dm8]# mkdir -p $GOPATH/src/golang.org/x/
[root@vmpc go_dm8]# cd $GOPATH/src/golang.org/x/
[root@vmpc x]# git clone https://github.com/golang/text.git
```
# 编译项目
```bash
[root@vmpc x]# cd $GOPATH
[root@vmpc go_dm8]# go get app
[root@vmpc go_dm8]# go install app
```


目录结构

```bash
[root@vmpc go_dm8]# tree -L 2
.
├── bin
│   ├── app
│   └── sanguo.txt
└── src
    ├── app
    ├── dm
    ├── github.com
    └── golang.org

6 directories, 2 files
```
