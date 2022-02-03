/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// testCmd represents the test command
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn := fmtDSN(host, port, user, password)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return err
		}

		db.SetMaxIdleConns(maxConn)
		db.SetMaxOpenConns(maxConn)

		tb := "test.tk_test"
		str := fmt.Sprintf("create table if not exists %s(id int primary key auto_increment, v int);", tb)
		_, err = db.Exec(str)
		if err != nil {
			return err
		}

		execfn := func(db *sql.DB, id int) error {
			tx, err := db.Begin()
			if err != nil {
				return errors.Annotate(err, "failed to begin txn")
			}
			str := fmt.Sprintf("replace into %s(id, v) values(?, ?);", tb)
			_, err = tx.Exec(str, id, id)
			if err != nil {
				return err
			}

			str = fmt.Sprintf("replace into %s(id, v) values(?, ?);", tb)
			_, err = tx.Exec(str, id, id)
			if err != nil {
				return errors.Annotate(err, "failed to exec statement")
			}

			err = tx.Commit()
			if err != nil {
				return errors.Annotate(err, "failed to commit txn")
			}
			return nil
		}

		var totalCount int64
		var failCount int64

		for i := 0; i < maxConn; i++ {
			id := i
			go func(db *sql.DB) {
				for {
					err := execfn(db, id)
					atomic.AddInt64(&totalCount, 1)
					if err != nil {
						log.Error("failed to run txn", zap.String("error", err.Error()))
						atomic.AddInt64(&failCount, 1)
					}
				}
			}(db)
		}

		var lastTotalCount int64
		var lastFailCount int64
		for range time.Tick(time.Second * 1) {
			tc := atomic.LoadInt64(&totalCount)
			fc := atomic.LoadInt64(&failCount)

			fmt.Printf("totalCount: %d, failCount: %d, totalCountDiff: %d, failCountDiff: %d\n", tc, fc, tc-lastTotalCount, fc-lastFailCount)
		}

		return nil
	},
}

var user string
var password string
var host string
var port int
var maxConn int

func init() {
	rootCmd.AddCommand(testCmd)

	testCmd.Flags().StringVar(&user, "user", "root", "user of db")
	testCmd.Flags().StringVar(&password, "psw", "", "password of db")
	testCmd.Flags().StringVar(&host, "host", "127.0.0.1", "host of db")
	testCmd.Flags().IntVar(&port, "port", 4000, "port of db")
	testCmd.Flags().IntVarP(&maxConn, "max-connection", "p", 100, "max connection keep writing")
}

func fmtDSN(host string, port int, user string, psw string) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/test?", user, password, host, port)

	return dsn
}
