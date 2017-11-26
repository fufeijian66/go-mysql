package main

import (
	"context"
	"flag"
	"fmt"
	_"os"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	_"os"
)

var host = flag.String("host", "192.168.18.17", "MySQL地址")
var port = flag.Int("port", 30010, "端口")
var user = flag.String("user", "root", "用户名")
var password = flag.String("password", "123456", "密码")

var flavor = flag.String("flavor", "mysql", "mysql or mariadb")

var file = flag.String("file", "", "binlog文件")
var pos = flag.Int("pos", 1933749, "Binlog偏移值")

var semiSync = flag.Bool("semisync", false, "Support semi sync")
var backupPath = flag.String("backup_path", "", "备份存储目录")

var rawMode = flag.Bool("raw", false, "raw模式存储")

func main() {
	flag.Parse()

	cfg := replication.BinlogSyncerConfig{
		ServerID: 101,
		Flavor:   *flavor,
		Host:            *host,
		Port:            uint16(*port),
		User:            *user,
		Password:        *password,
		RawModeEnabled:  *rawMode,
		SemiSyncEnabled: *semiSync,
	}
	b := replication.NewBinlogSyncer(cfg)
	pos := mysql.Position{*file, uint32(*pos)}
	if len(*backupPath) > 0 {
		// Backup will always use RawMode.
		err := b.StartBackup(*backupPath, pos, 0)
		if err != nil {
			fmt.Printf("备份出错: %v\n", errors.ErrorStack(err))
			return
		}
	} else {
		s, err := b.StartSync(pos)
		if err != nil {
			fmt.Printf("同步出错: %v\n", errors.ErrorStack(err))
			return
		}
		for {
			e, err := s.GetEvent(context.Background())
			if err != nil {
				fmt.Printf("获取事件出错: %v\n", errors.ErrorStack(err))
				return
			}
			switch (e.Header.EventType){
			case replication.WRITE_ROWS_EVENTv2:
				fmt.Println(e.Header.EventType)
				fmt.Println(e.Header.LogPos)
				fmt.Println(e.Json())
			case replication.DELETE_ROWS_EVENTv2:
				fmt.Println(e.Header.EventType)
				fmt.Println(e.Header.LogPos)

				fmt.Println(e.Json())
			case replication.UPDATE_ROWS_EVENTv2:
				fmt.Println(e.Header.EventType)
				fmt.Println(e.Header.LogPos)
				fmt.Println(e.Json())
			}
			//e.Header.Dump(os.Stdout)
			//fmt.Println(e.Json())
			//e.Event.Dump(os.Stdout)
			//e.Dump(os.Stdout)
		}
	}

}
