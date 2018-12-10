package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"github.com/kshvakov/clickhouse"
	"time"
)

func dbtest(){
	connect, err := sql.Open("clickhouse", "tcp://192.168.2.201:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}
//	_, err = connect.Exec(`
//		select * from log1
//	`)

	rows, err := connect.Query("SELECT count(*) FROM log1")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

}

func isquery(qt string) int{
	result := 0
	if(qt == "q"){
		result = 1
	}
	return result
}

func main()  {

	var(
		importFileName	string
		i	int
	)

	connect, err := sql.Open("clickhouse", "tcp://192.168.2.201:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}

	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO log1 (source_ip, source_port, dest_ip, dest_port, query_id, " +
			"query_name, query_type, query_answer, query_result, is_query, q_datetime, q_ts) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	)

	defer stmt.Close()

	flag.StringVar(&importFileName, "t", "rs_query.log", "import file name with full path")

	flag.Parse()

	file, err:= os.Open(importFileName)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	i = 0
	for scanner.Scan() {
		i++
		if i == 100 {
			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}
			i = 0
		}
		eles := strings.Split(scanner.Text(), "|")

		tm1, _ := time.Parse("20060102150405", eles[8][0:14])
		tm2, _ := time.Parse("20060102150405.000", eles[8])

		fmt.Println(eles[8][0:14])

		s_port, _ := strconv.Atoi(eles[1])
		d_port, _ := strconv.Atoi(eles[3])
		q_id, _ := strconv.Atoi(eles[4])
		q_result, _ := strconv.Atoi(eles[9])

		if _, err := stmt.Exec(
			eles[0],
			s_port,
			eles[2],
			d_port,
			q_id,
			eles[5],
			eles[6],
			eles[7],
			q_result,
			isquery(eles[10]),
			tm1,
			tm2.UnixNano()/1000000); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	stmt.Close()

	connect.Close()

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	//dbtest()
}