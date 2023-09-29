package main

import (
	"context"
	"database/sql"
	"fmt"
	bytehouse "github.com/bytehouse-cloud/driver-go"
	_ "github.com/bytehouse-cloud/driver-go/sql" //this is required, otherwise "bytehouse" driver will not be registered
	"golang.org/x/sync/errgroup"
	"log"
	"time"
)

func main() {
	host := "<<hostname>>"
	port := "<<port>>"
	apiToken := "<<YOUR_API_TOKEN>>"

	dsn := fmt.Sprintf("tcp://%s:%s?secure=true&user=bytehouse&password=%s", host, port, apiToken)
	queryString := "select * from numbers(100)"
	pool, err := sql.Open("bytehouse", dsn)
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer pool.Close()

	//setup the database configuration here
	pool.SetMaxOpenConns(10)
	pool.SetConnMaxIdleTime(time.Minute)
	pool.SetConnMaxLifetime(time.Minute * 5)
	pool.SetMaxIdleConns(10)

	threadCount := 10
	eg := errgroup.Group{}

	for i := 0; i < threadCount; i++ {
		i := i
		eg.Go(func() error {
			threadID := fmt.Sprintf("thread-%v", i)
			//we need to create bytehouse.NewQueryContext() to assign a query ID and add query settings
			queryCtx := bytehouse.NewQueryContext(context.Background())
			//set the query ID here, duplicate query IDs will be rejected
			queryCtx.SetQueryID(fmt.Sprintf("my_query_id%v", time.Now().String()))
			if err := queryCtx.AddQuerySetting("max_block_size", "2000"); err != nil {
				log.Fatalf("thread %v failed to add query setting err = %v", threadID, err)
				return err
			}

			if err := pool.Ping(); err != nil {
				log.Fatalf("thread %v failed to ping err = %v", threadID, err)
				return err
			}
			rows, err := pool.QueryContext(queryCtx, queryString)
			if err != nil {
				log.Fatalf("thread %v failed to query err = %v", threadID, err)
				return err
			}

			//we must close the row
			defer rows.Close()
			var num int64

			for rows.Next() {
				if err := rows.Scan(&num); err != nil {
					log.Fatalf("thread %v failed to scan row err = %v", threadID, err)
					return err
				}

				fmt.Printf("\nID:%v Value: %v", threadID, num)
			}
			fmt.Printf("\n%v Done!", threadID)
			return nil
		})
	}

	//wait for all threads to finish
	if err := eg.Wait(); err != nil {
		log.Fatalf("one of the threads have an error:= %v", err)
	}

	fmt.Println("\nAll threads done!")
}
