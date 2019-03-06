package mysql

import (
	"fmt"
	"sync"
	"testing"
	// "time"
)

const testMysqlDBConn = "root:123qwe@tcp(localhost:3306)/tmpdb"

var (
	testMysqlInited = false

	dbMysql *Mysql
)

/*
// init sql
create database tmpdb
create table tmptab (
    name  varchar(32) not null,
    remark varchar(64) not null
);
*/
func testInitMysql() {
	if !testMysqlInited {
		testMysqlInited = true

		dbMysql = NewMysql(MysqlConf{
			ConnString:        testMysqlDBConn,
			PoolSize:          10,
			KeepaliveInterval: 2, //300
		})
	}
}

func TestMysql(t *testing.T) {
	testInitMysql()
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				func() {
					if db := dbMysql.DB(); db != nil {
						name := fmt.Sprintf("name_%d_%d", idx, j)
						remark := fmt.Sprintf("remark_%d_%d", idx, j)
						if _, err := db.Exec(`insert into tmptab values(?, ?)`, name, remark); err != nil {
							t.Logf("Insert: (%s, %s), error: %v\n", name, remark, err)
						}

						retname, retremark := "", ""
						if err := db.QueryRow(`select name,remark from tmpdb.tmptab where name=?`, name).Scan(&retname, &retremark); err != nil || !(name == retname && remark == retremark) {
							t.Logf("Find: (%s, %s), error: %v, equal: %v\n", retname, retremark, err, name == retname && remark == retremark)
						}
					}
				}()
			}
		}()
	}

	wg.Wait()
}
