package amend

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	// for database/sql
	_ "github.com/go-sql-driver/mysql"
	"github.com/july2993/tk/pkg/diff"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

func getDSN(user string, password string, host string, port int) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/test?interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, host, port)

	return dsn
}

func createDB(user string, password string, host string, port int) (*sql.DB, error) {
	dsn := getDSN(user, password, host, port)
	db, err := sql.Open("mysql", dsn)
	return db, err
}

func AmendCMD() *cobra.Command {
	var (
		user     string
		password string
		host     string
		port     int

		user2     string
		password2 string
		host2     string
		port2     int
	)

	cmd := &cobra.Command{
		Use: "amend",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error

			db1, err := createDB(user, password, host, port)
			if err != nil {
				return err
			}

			db2, err := createDB(user2, password2, host2, port2)
			if err != nil {
				return err
			}

			// start test
			//

			err = testAddColumn3(db1, db2)
			if err != nil {
				return err
			}
			log.Print("testAddColumn3 success")

			err = testAddColumn(db1, db2)
			if err != nil {
				return err
			}
			log.Print("testAddColumn success")

			err = testAddColumn2(db1, db2)
			if err != nil {
				return err
			}
			log.Print("testAddColumn2 success")

			err = testAddColumn4(db1, db2)
			if err != nil {
				return err
			}
			log.Print("testAddColumn4 success")

			return nil
		},
	}

	cmd.Flags().StringVar(&user, "user", "root", "user of db")
	cmd.Flags().StringVar(&password, "psw", "", "password of db")
	cmd.Flags().StringVar(&host, "host", "127.0.0.1", "host of db")
	cmd.Flags().IntVar(&port, "port", 4000, "port of db")

	cmd.Flags().StringVar(&user2, "user2", "root", "user of db")
	cmd.Flags().StringVar(&password2, "psw2", "", "password of db")
	cmd.Flags().StringVar(&host2, "host2", "127.0.0.1", "host of db")
	cmd.Flags().IntVar(&port2, "port2", 3306, "port of db")

	return cmd
}

func testAddColumn4(db1 *sql.DB, db2 *sql.DB) error {
	mustExec(db1, "drop table if exists accounts;")
	mustExec(db2, "drop table if exists accounts;")

	mustExec(db1, "create table accounts(id int primary key, balance bigint);")
	mustExec(db1, "insert into accounts values(1, 100), (2, 0), (3, 1);")

	txn1, err := db1.Begin()
	if err != nil {
		return err
	}

	mustExec(txn1, "set tidb_enable_amend_pessimistic_txn = 1;")

	mustExec(db1, "alter table accounts add column bb int default 10;")
	mustExec(db1, "insert into accounts values(100,100,100);")
	mustExec(db1, "alter table accounts drop column bb;")
	mustExec(db1, "alter table accounts add column bb int default 99;")

	mustExec(txn1, "update accounts set balance = id where 1;")

	err = txn1.Commit()
	if err != nil {
		return err
	}

	err = checkData(time.Minute, db1, db2)
	if err != nil {
		return err
	}

	return nil
}

func testAddColumn3(db1 *sql.DB, db2 *sql.DB) error {
	mustExec(db1, "drop table if exists accounts;")
	mustExec(db2, "drop table if exists accounts;")

	mustExec(db1, "create table accounts(id int primary key, balance bigint);")
	mustExec(db1, "insert into accounts values(1, 100), (2, 0), (3, 1);")

	txn1, err := db1.Begin()
	if err != nil {
		return err
	}

	mustExec(txn1, "set tidb_enable_amend_pessimistic_txn = 1;")

	mustExec(db1, "alter table accounts add column bb int not null;")
	mustExec(db1, "alter table accounts add column cc int not null;")

	mustExec(txn1, "update accounts set balance = id where 1;")

	err = txn1.Commit()
	if err != nil {
		return err
	}

	err = checkData(time.Minute, db1, db2)
	if err != nil {
		return err
	}

	return nil
}

func testAddColumn2(db1 *sql.DB, db2 *sql.DB) error {
	mustExec(db1, "drop table if exists accounts;")
	mustExec(db2, "drop table if exists accounts;")

	mustExec(db1, "create table accounts(id int primary key, balance bigint, aa int);")
	mustExec(db1, "insert into accounts values(1, 100, 1), (2, 0, 2), (3, 1, 3);")

	txn1, err := db1.Begin()
	if err != nil {
		return err
	}

	mustExec(txn1, "set tidb_enable_amend_pessimistic_txn = 1;")

	mustExec(db1, "alter table accounts add column bb int default 10;")
	mustExec(db1, "alter table accounts add column cc int default 20;")

	mustExec(txn1, "update accounts set balance = id where 1;")

	err = txn1.Commit()
	if err != nil {
		return err
	}

	err = checkData(time.Minute, db1, db2)
	if err != nil {
		return err
	}

	return nil
}

func testAddColumn(db1 *sql.DB, db2 *sql.DB) error {
	mustExec(db1, "drop table if exists t;")
	mustExec(db2, "drop table if exists t;")

	mustExec(db1, "create table t (id int primary key, c_str varchar(20));")
	mustExec(db1, "insert into t values (1, '0001'), (2, '0002'), (3, null), (4, '0003'), (5, null);")

	txn1, err := db1.Begin()
	if err != nil {
		return err
	}

	mustExec(txn1, "set tidb_enable_amend_pessimistic_txn = 1;")
	mustExec(txn1, "insert into t values (6, '0004');")
	mustExec(txn1, "insert into t values (7, null);")

	mustExec(db1, "alter table t add c_str_new varchar(20);")

	mustExec(txn1, "update t set c_str = '0005' where id = 1;")
	mustExec(txn1, "update t set c_str = null where id = 2;")
	mustExec(txn1, "update t set c_str = '0006' where id = 3;")
	mustExec(txn1, "delete from t where id = 4;")
	mustExec(txn1, "delete from t where id = 5;")
	err = txn1.Commit()
	if err != nil {
		return err
	}

	err = checkData(time.Minute, db1, db2)
	if err != nil {
		return err
	}

	return nil
}

type executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func mustExec(db executor, sql string, args ...interface{}) {
	_, err := db.ExecContext(context.Background(), sql, args...)
	if err != nil {
		log.Fatal(err)
	}
}

func checkData(timeout time.Duration, db1 *sql.DB, db2 *sql.DB) error {
	start := time.Now()
	df := diff.New(nil, db1, db2)

	for {
		equal, err := df.Equal()
		if err != nil {
			return errors.Trace(err)
		}

		if equal {
			return nil
		}

		if time.Since(start) > timeout {
			return errors.Errorf("failed to check equal")
		}

		time.Sleep(time.Second * 10)
	}
}
