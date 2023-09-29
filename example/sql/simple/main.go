package main

import (
	"context"
	"database/sql"
	"fmt"
	bytehouse "github.com/bytehouse-cloud/driver-go"
	_ "github.com/bytehouse-cloud/driver-go/sql" //this is required, otherwise "bytehouse" driver will not be registered
	"log"
)

func main() {

	host := "<<hostname>>"
	port := "<<port>>"
	apiToken := "<<YOUR_API_TOKEN>>"

	dsn := fmt.Sprintf("tcp://%s:%s?secure=true&user=bytehouse&password=%s", host, port, apiToken)

	db, err := sql.Open("bytehouse", dsn)
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping, error = %v", err)
	}
	ctx := context.Background()
	queryCtx := bytehouse.NewQueryContext(ctx)
	queryCtx.SetQueryID("my_query_id")
	if err := queryCtx.AddQuerySetting("max_block_size", "2000"); err != nil {
		log.Fatalf("failed to add query setting, error = %v", err)
	}

	rows, err := db.QueryContext(queryCtx, "select * from numbers(100)")
	if err != nil {
		log.Fatalf("failed to query err = %v", err)
	}

	//must close the row
	defer rows.Close()
	var num int64

	rowNum := 1
	for rows.Next() {
		if err := rows.Scan(&num); err != nil {
			log.Fatalf("failed to scan row error = %v", err)
		}

		fmt.Printf("\nRow %v: %v", rowNum, num)
		rowNum++
	}

}
