package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"time"
)

type MysqlConf struct {
	ConnString        string `json:"ConnString"` //user:password@tcp(localhost:5555)/dbname
	EncryptKey        string `json:"EncryptKey"`
	PoolSize          int    `json:"PoolSize"`
	IdleSize          int    `json:"IdleSize"`
	KeepaliveInterval int    `json:"KeepaliveInterval"`
	keepaliveInterval time.Duration
}

type Mysql struct {
	db     *gorm.DB
	ticker *time.Ticker
}

func (msql *Mysql) OrmDB() *gorm.DB {
	return msql.db
}

func (msql *Mysql) DB() *sql.DB {
	return msql.db.DB()
}

func (msql *Mysql) Close() error {
	return msql.db.Close()
}

func NewMysql(conf MysqlConf) *Mysql {
	if conf.ConnString == "" {
		logFatal("NewMysql Failed: invalid ConnString")
	}

	if conf.KeepaliveInterval > 0 {
		conf.keepaliveInterval = time.Second * time.Duration(conf.KeepaliveInterval)
	} else {
		conf.keepaliveInterval = time.Second * 300
	}

	logInfo("NewMysql Connect To Mysql ...")

	// db, err := sql.Open("mysql", conf.ConnString)
	db, err := gorm.Open("mysql", conf.ConnString)
	if err != nil {
		logFatal("NewMysql sql.Open Failed: %v", err)
	}

	err = db.DB().Ping()
	if err != nil {
		logFatal("NewMysql Ping() Failed: %v", err)
	}

	if conf.PoolSize > 0 {
		db.DB().SetMaxOpenConns(conf.PoolSize)
	}
	if conf.IdleSize > 0 {
		db.DB().SetMaxIdleConns(conf.IdleSize)
	}

	msql := &Mysql{db, time.NewTicker(conf.keepaliveInterval)}

	safeGo(func() {
		for {
			if _, ok := <-msql.ticker.C; !ok {
				break
			}
			if err := db.DB().Ping(); err != nil {
				logDebug("Mysql Ping: %v", err)
			}
		}
	})

	logInfo("NewMysql Connect To Mysql Success")

	return msql
}

func ClearTransaction(tx *sql.Tx) error {
	err := tx.Rollback()
	if err != nil && err != sql.ErrTxDone {
		logError("ClearTransaction failed: %v\n", err)
	}
	return err
}
