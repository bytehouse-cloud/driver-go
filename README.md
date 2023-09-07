# ByteHouse Driver Go: High Performance Golang Driver and SDK for connecting to ByteHouse

## Usage Guide

### Connect to ByteHouse

#### 1. Connect with API Key

*For ByteHouse Global/China version, users can create and download credentials from [console](https://console.bytehouse.cloud/account/details)

*For ByteHouse Volcano Cloud Version, users need to create and download credentials from Volcano Cloud's [Account Details page](https://console.volcengine.com/auth/login?redirectURI=%2Fbytehouse%2Fregion%3Abytehouse%2Bcn-beijing%2Faccount%2Fdetails) 

Create the API Key and save it in the local environment. 

<img width="830" alt="Screenshot 2023-08-31 at 6 01 19 PM" src="https://github.com/rexlionz/driver-go/assets/87936033/d23777f2-1aa9-4e37-a1bd-5580ed80c7d3">

### Test Connectivity

```go
db, err := sql.Open("bytehouse", "tcp://{HOST}:{PORT}?secure=true&user=bytehouse&password={API_KEY}")

//If user wishes to specify the database in url
db, err := sql.Open("bytehouse", "tcp://{HOST}:{PORT}?secure=true&user=bytehouse&password={API_KEY}&database={DATABASE}")

if err != nil {
    fmt.Printf("error = %v", err)
    return
}
defer db.Close()
```

Replace the Host:Port and API key placeholders in the dsn below as specified in the [ByteHouse doc](https://docs.bytehouse.cloud/en/bytehouse/docs/supported-regions-and-providers).

### DDL

All DDL queries should be done with db.ExecContext

```go
package main
import (
    "context"
    "fmt"
    "github.com/bytehouse-cloud/driver-go/sdk"
)
func main() {
    dsn := fmt.Sprintf("tcp://{HOST}:{PORT}?secure=true&user=bytehouse&password={API_KEY}")
//If user wishes to specify the database in url
    dsn := fmt.Sprintf("tcp://{HOST}:{PORT}?secure=true&user=bytehouse&password={API_KEY}&database={DATABASE}")

    ctx := context.Background()

    g, err := sdk.Open(ctx, dsn)
    if err != nil {
       panic(err)
    }

    if err := g.Ping(); err != nil {
       panic(err)
    }

    if qs, err := g.QueryContext(ctx, "CREATE DATABASE my_db"); err != nil || qs.Exception() != nil {
       if err != nil {
          panic(err)
       }
       if qs.Exception() != nil {
          panic(qs.Exception())
       }
    }

    if qs, err := g.QueryContext(ctx,
       `CREATE TABLE my_db.animal 
                (
                    dog Int64,
                    cat Int64
                )
                ENGINE=CnchMergeTree ORDER BY dog`); err != nil || qs.Exception() != nil {
       if err != nil {
          panic(err)
       }
       if qs.Exception() != nil {
          panic(qs.Exception())
       }
    }
}
```

### Data Insertion

You can specify the columns to be inserted, if no column is specified, all columns will be chosen

- with select columns `INSERT INTO sample_table (col1, col2) VALUES`
- without selected columns `INSERT INTO sample_table VALUES`

#### Single Row

```go
package main
import (
    "context"
    "fmt"
    "github.com/bytehouse-cloud/driver-go/sdk"
)
func main() {
    dsn := fmt.Sprintf("tcp://{HOST}:{PORT}?secure=true&user=bytehouse&password={API_KEY}")
//If user wishes to specify the database in url
    dsn := fmt.Sprintf("tcp://{HOST}:{PORT}?secure=true&user=bytehouse&password={API_KEY}&database={DATABASE}")

    ctx := context.Background()

    g, err := sdk.Open(ctx, dsn)
    if err != nil {
       panic(err)
    }

    if err := g.Ping(); err != nil {
       panic(err)
    }

    if err := g.SendInsertQuery(ctx, "INSERT INTO my_db.animal VALUES (1,2), (3,4)"); err != nil {
       panic(err)
    }

}
```

#### Batch insertion

```go
package main

import (
        "context"
        "database/sql"
        "fmt"

        "github.com/bytehouse-cloud/driver-go"
        "github.com/bytehouse-cloud/driver-go/sdk"
        sql2 "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
        db, err := sql.Open("bytehouse", "tcp://gateway.aws-ap-southeast-1.bytehouse.cloud:19000?secure=true&user=bytehouse&password=HhPNNvXncU:S82kyc44Il")
        if err != nil {
                fmt.Printf("error = %v", err)
                return
        }
        defer db.Close()

        // set the insert block size if needed
        ctx := bytehouse.NewQueryContext(context.Background())
        batchSize := 1000
        if err != ctx.AddQuerySetting(bytehouse.InsertBlockSize, batchSize) {
                panic(err)
        }

        if err = sql2.RunConn(ctx, db, func(conn sdk.Conn) error {
                stmt, err := conn.PrepareContext(ctx, "INSERT INTO sample_table VALUES (?, ?)")
                if err != nil {
                        return err
                }

                for i := 0; i < 1e7; i++ {
                        if err := stmt.ExecContext(ctx, 1, 2); err != nil {
                                return err
                        }
                }

                return stmt.Close() // Remember to close the stmt! This step is a must for the query to go through!
        }); err != nil {
                fmt.Printf("error = %v", err)
        }

}
```

#### Insert from select

You can insert from SELECT statements. Output from select statement with be inserted into your table

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
	db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	ctx := context.Background()
	// Note first return value is sql.Result, which can be discarded since it is not implemented in the driver
	if _, err := db.ExecContext(ctx, "INSERT INTO sample_table SELECT * FROM sample_table"); err != nil {
		fmt.Printf("error = %v", err)
	}
}
```

#### Insertion from local file

##### CSV

Following shows how it can be done with csv file format

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "os"

    "github.com/bytehouse-cloud/driver-go/sdk"
    _ "github.com/bytehouse-cloud/driver-go/sql"
    driverSql "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
    db, err := sql.Open("bytehouse", "tcp://gateway.aws-ap-southeast-1.bytehouse.cloud:19000?secure=true&user=bytehouse&password=HhPNNvXncU:S82kyc44Il")
    if err != nil {
        fmt.Printf("error = %v", err)
        return
    }
    defer db.Close()

    ctx := context.Background()

    file, err := os.Open("./testdata/insert.csv") // path to your .csv file
    if err != nil {
        fmt.Printf("error = %v", err)
        return
    }
    defer file.Close()

    if err = driverSql.RunConn(ctx, db, func(conn sdk.Conn) error {
     _,e :=conn.InsertFromReader(ctx, "INSERT INTO sample_table FORMAT CSV", file)
        return e
     }); err != nil {
         fmt.Printf("error = %v", err)
     }
}
```

Example CSV Format

Format should not have headers

```
1,2
1,2
```

Using custom delimiter for your csv file rather than default `,`

Add to query setting map your custom delimiter The setting name is `format_csv_delimiter`

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "os"

    "github.com/bytehouse-cloud/driver-go/sdk"
    _ "github.com/bytehouse-cloud/driver-go/sql"
    driverSql "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
    db, err := sql.Open("bytehouse", "tcp://gateway.aws-ap-southeast-1.bytehouse.cloud:19000?secure=true&user=bytehouse&password=HhPNNvXncU:S82kyc44Il")
    if err != nil {
        fmt.Printf("error = %v", err)
        return
    }
    defer db.Close()

    ctx := context.Background()

    file, err := os.Open("./testdata/insert_with_pipes.csv")
    if err != nil {
        fmt.Printf("error = %v", err)
        return
    }
    defer file.Close()

    if err = driverSql.RunConn(ctx, db, func(conn sdk.Conn) error {
 _,e :=conn.InsertFromReader(ctx, "INSERT INTO sample_table FORMAT CSV", file)
 return e
        }); err != nil {
 fmt.Printf("error = %v", err)
        }
}
```

##### CSVWithNames

Use format if your csv file has column headers. Note that this options simply skip the first line of your CSV We do not
read your CSV column headers and match them to the corresponding row You have to make sure that your CSV column ordering
is the same as that defined in your table

```go
package main

import (
        "context"
        "database/sql"
        "fmt"
        "os"

        "github.com/bytehouse-cloud/driver-go/sdk"
        _ "github.com/bytehouse-cloud/driver-go/sql"
        driverSql "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
        db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
        if err != nil {
                fmt.Printf("error = %v", err)
                return
        }
        defer db.Close()

        ctx := context.Background()

        file, err := os.Open("./testdata/insert_with_names.csv")
        if err != nil {
                fmt.Printf("error = %v", err)
                return
        }
        defer file.Close()

        if err = driverSql.RunConn(ctx, db, func(conn sdk.Conn) error {
     _,e :=conn.InsertFromReader(ctx, "INSERT INTO sample_table FORMAT CSV", file)
     return e
            }); err != nil {
     fmt.Printf("error = %v", err)
            }
}
```

Example CSVWithNames Format

- Note: contents of the first line doesn't matter as it will be skipped

```
a, b 
1, 2
1, 2
```

##### JSON

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "os"

    "github.com/bytehouse-cloud/driver-go/sdk"
    _ "github.com/bytehouse-cloud/driver-go/sql"
    driverSql "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
    db, err := sql.Open("bytehouse", "tcp://gateway.aws-ap-southeast-1.bytehouse.cloud:19000?secure=true&user=bytehouse&password=HhPNNvXncU:S82kyc44Il")
    if err != nil {
        fmt.Printf("error = %v", err)
        return
    }
    defer db.Close()

    ctx := context.Background()

    file, err := os.Open("insert.json")
    if err != nil {
        fmt.Printf("error = %v", err)
        return
    }
    defer file.Close()

    if err = driverSql.RunConn(ctx, db, func(conn sdk.Conn) error {
        _,e :=conn.InsertFromReader(ctx, "INSERT INTO sample_table FORMAT JSON", file)
        return e
        }); err != nil {
        fmt.Printf("error = %v", err)
    }
}
```

Example JSON Format

- JSON field name must match with your clickhouse table field name
- Example: for data below your table should be of this structure `a Int, b Int`

```json
{
  "data": [
    {
      "a": 1,
      "b": 2
    },
    {
      "a": 1,
      "b": 2
    }
  ]
}
```

### Select

#### To Golang struct

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
	db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	// Use your own types here depending on your table
	type sample struct {
		cat int
		dog int
	}

	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "SELECT * FROM sample_table LIMIT 5")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}

	out := make([]sample, 5)
	i := 0
	for rows.Next() {
		if err := rows.Scan(&out[i].dog, &out[i].cat); err != nil {
			fmt.Printf("error = %v", err)
		}
		i++
	}

	fmt.Println(out)

	// Remember to close your rows when you are done! This is a must!
	if err := rows.Close(); err != nil {
		fmt.Printf("error = %v", err)
	}
}
```

##### Single Row

If you are selecting just a single row, you can use db.QueryRowContext which is much more convenient!
Make sure that your query only returns one row!

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
	db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	// Use your own types here depending on your table value 
	type sample struct {
		dog int
		cat int
	}

	sampleData := &sample{}
	ctx := context.Background()
	row := db.QueryRowContext(ctx, "SELECT * FROM sample_table LIMIT 1")

	if err := row.Scan(&sampleData.dog, &sampleData.dog); err != nil {
		fmt.Printf("error = %v", err)
		return
	}

	fmt.Printf("%+v\n", sampleData)
}
```

#### To local file

- Use this when you want to export your query results into a file

##### CSV

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"

	"github.com/bytehouse-cloud/driver-go/sdk"
	_ "github.com/bytehouse-cloud/driver-go/sql"
	driverSql "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
	db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	ctx := context.Background()

	file, err := os.OpenFile("./testdata/select_temp.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if err = driverSql.RunConn(ctx, db, func(conn sdk.Conn) error {
		qr, err := conn.QueryContext(ctx, `
		SELECT * FROM
		sample_table
		`)
		if err != nil {
			return err
		}
		defer qr.Close()

		reader = qr.ExportToReader("CSV")
		return nil

	}); err != nil {
		fmt.Printf("error = %v", err)
		return
	}

	if _, err = io.Copy(file, reader); err != nil {
		fmt.Printf("error = %v", err)
	}
}
```

Output: `1,2 3,4 5,6`

###### JSON

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"

	"github.com/bytehouse-cloud/driver-go/sdk"
	_ "github.com/bytehouse-cloud/driver-go/sql"
	driverSql "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
	db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	ctx := context.Background()

	file, err := os.OpenFile("./testdata/select_temp.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if err = driverSql.RunConn(ctx, db, func(conn sdk.Conn) error {
		qr, err := conn.QueryContext(ctx, "SELECT * FROM sample_table")
		if err != nil {
			return err
		}
		defer qr.Close()

		reader = qr.ExportToReader("JSON")
		return nil

	}); err != nil {
		fmt.Printf("error = %v", err)
		return
	}

	if _, err = io.Copy(file, reader); err != nil {
		fmt.Printf("error = %v", err)
	}
}
```

Output

```json
{
  "meta": [
    {
      "name": "dog",
      "type": "UInt32"
    },
    {
      "name": "cat",
      "type": "UInt32"
    }
  ],
  "data": [
    {
      "dog": 1,
      "cat": 2
    },
    {
      "dog": 3,
      "cat": 4
    },
    {
      "dog": 5,
      "cat": 6
    }
  ],
  "rows": 3
}
```

##### VALUES

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"

	"github.com/bytehouse-cloud/driver-go/sdk"
	_ "github.com/bytehouse-cloud/driver-go/sql"
	driverSql "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
	db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	ctx := context.Background()

	file, err := os.OpenFile("./testdata/select_temp.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if err = driverSql.RunConn(ctx, db, func(conn sdk.Conn) error {
		qr, err := conn.QueryContext(ctx, "SELECT * FROM sample_table")
		if err != nil {
			return err
		}
		defer qr.Close()

		reader = qr.ExportToReader("VALUES")
		return nil

	}); err != nil {
		fmt.Printf("error = %v", err)
		return
	}

	if _, err = io.Copy(file, reader); err != nil {
		fmt.Printf("error = %v", err)
	}
}
```

Output

```
(1, 2),
(3, 4),
(5, 6)
```

### Query with external tables (local file system)

- For more info on external tables: https://clickhouse.tech/docs/en/engines/table-engines/special/external-data/

External tables refer to data you want to reference in your query that is not in your database

External table in file CSV ./test_data/external_table.csv

```
1,4 2,5 3,6
```

Code Example

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/bytehouse-cloud/driver-go/driver/lib/data/column"
	"github.com/bytehouse-cloud/driver-go/sdk"
	sqlDriver "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
	db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	ctx := context.Background()

	// Open file 
	file, err := os.Open("./testdata/external_table.csv")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer file.Close()

	// Run insert query 
	var qr *sdk.QueryResult
	if err = sqlDriver.RunConn(ctx, db, func(conn sdk.Conn) error {
		qr, err =
			conn.QueryContextWithExternalTableReader(
				ctx, // External table name used "fish" must match that in the ExternalTableReader
				"SELECT a, b FROM fish", sdk.NewExternalTableReader(
					// Table name
					"fish",
					// File path
					file,
					// Column names
					[]string{"a", "b"},
					// Column types
					[]column.CHColumnType{column.UINT32, column.UINT32},
					// File format
					"CSV",
				),
			)

		return err

	}); err != nil {
		fmt.Printf("error = %v", err)
		return
	}

	defer qr.Close()
	out := make([][]interface{}, 0, 5)
	for {
		rowValues, ok := qr.NextRow()
		if !ok {
			break
		}

		out = append(out, rowValues)

	}

	fmt.Println(out) // [[1 4] [2 5] [3 6]]
}
```

### Query settings

Usage Example

```go
package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bytehouse-cloud/driver-go"
	_ "github.com/bytehouse-cloud/driver-go/sql"
)

func main() {
	db, err := sql.Open("bytehouse", "tcp://?region=<region>&account=<account>&user=<user>&password=<password>")
	if err != nil {
		fmt.Printf("error = %v", err)
		return
	}
	defer db.Close()

	ctx := context.Background()
	queryCtx := bytehouse.NewQueryContext(ctx)
	if err := queryCtx.AddQuerySetting("Query Setting Name", "Query Setting Value"); err != nil {
		fmt.Printf("error = %v",
			err)
		return
	}

	if _, err := db.ExecContext(queryCtx, "INSERT INTO sample_table VALUES (?, ?)", 1, 2); err != nil {
		fmt.Printf("error = %v", err)
	}
}
```

### Multi threading and Connection Pooling
The SQL interface that Go provides uses a connection pool by default. Connection pool configuration can be customized during runtime.

The returned pool is thread safe so it can be used by multiple goroutines at the same time. We only call the `sql.Open` once.
You must call the `Ping()` method to test for connectivity. `sql.Open` does not establish connections until a request is sent.
```go
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
			
			//add optional query settings
			if err := queryCtx.AddQuerySetting("max_block_size", "2000"); err != nil {
				log.Fatalf("thread %v failed to add query setting err = %v", threadID, err)
				return err
			}

			//test if the connection is valid
			if err := pool.Ping(); err != nil {
				log.Fatalf("thread %v failed to ping err = %v", threadID, err)
				return err
			}
			
			//send the query
			rows, err := pool.QueryContext(queryCtx, queryString)
			if err != nil {
				log.Fatalf("thread %v failed to query err = %v", threadID, err)
				return err
			}

			//we must close the row
			defer rows.Close()
			var num int64

			//iterate and retrieve the query result
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
```