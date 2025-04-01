package postgres

import (
	"database/sql"
	"fmt"
	// "log"
	"testing"

	_"github.com/lib/pq"
	// "github.com/go-redis/redis/v8"
)


// Initialize PostgreSQL connection
func initPostgres() (*sql.DB, error) {
	db, err := sql.Open("postgres", "host=192.168.0.108 port=5432 user=postgres password=123456 dbname=odoodns sslmode=disable")
	if err != nil {
		return nil, err
	}

	return db, nil
}


func TestCreatePGTableAndInsertData(t *testing.T) {
	// Connect to PostgreSQL
	db, err := initPostgres()
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Create table if not exists
	tableQuery := `
		CREATE TABLE IF NOT EXISTS coredhcp_records (
			id SERIAL PRIMARY KEY,
			mac_address VARCHAR(100) NOT NULL UNIQUE,
			ipv4 VARCHAR(100),
			ipv6 VARCHAR(100),
			router VARCHAR(100),
			dns VARCHAR(255),
			lease_time VARCHAR(100),
			t1 VARCHAR(100),
			t2 VARCHAR(100)
		);
	`
	_, err = db.Exec(tableQuery)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
    
	// Data to insert
	Data := map[string]map[string]string{
		"mac:68:a8:6d:57:6c:e7": {
			"ipv6":      "2001:2::4",
			"t1":        "12h",
			"t2":        "24h",
		},
		"mac:3c:07:54:5c:90:65": {
			"ipv4":      "192.168.1.101/24",
			"router":    "192.168.1.1",
			"dns":       "1.1.1.1,8.8.8.8",
			"leaseTime": "12h",
			"ipv6":      "2001:2::3",
			"t1":        "12h",
			"t2":        "24h",
		},
	}
	// Insert data into the table
	insertQuery := `
	INSERT INTO coredhcp_records (
		mac_address, ipv4, ipv6, router, dns, lease_time, t1, t2
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8
	) ON CONFLICT (mac_address) DO UPDATE SET
		ipv4 = EXCLUDED.ipv4,
		ipv6 = EXCLUDED.ipv6,
		router = EXCLUDED.router,
		dns = EXCLUDED.dns,
		lease_time = EXCLUDED.lease_time,
		t1 = EXCLUDED.t1,
		t2 = EXCLUDED.t2;
	`

	for mac, details := range Data {
		ipv4 := details["ipv4"]
		ipv6 := details["ipv6"]
		router := details["router"]
		dns := details["dns"]
		leaseTime := details["leaseTime"]
		t1 := details["t1"]
		t2 := details["t2"]

		_, err = db.Exec(insertQuery, mac, ipv4, ipv6, router, dns, leaseTime, t1, t2)
		if err != nil {
			t.Fatalf("Failed to insert data for MAC %s: %v", mac, err)
		}
	}

	// Verify data insertion
	rowCountQuery := "SELECT COUNT(*) FROM coredhcp_records"
	row := db.QueryRow(rowCountQuery)
	var rowCount int
	if err := row.Scan(&rowCount); err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if rowCount == 0 {
		t.Fatalf("No data inserted into coredhcp_records")
	}

	t.Logf("Test passed: %d rows inserted into coredhcp_records", rowCount)
}
// 可以给coredhcp添加单独的列，此处默认添加options列
func TestAddColumn(t *testing.T) {
	db, err := initPostgres()
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Define the column name and type
	columnName := "options"
	columnType := "JSONB"

	// Check if the column already exists
	columnExistsQuery := `
	SELECT column_name FROM information_schema.columns
	WHERE table_name='coredhcp_records' AND column_name=$1;
	`
	var existingColumn string
	err = db.QueryRow(columnExistsQuery, columnName).Scan(&existingColumn)
	if err == nil {
		t.Logf("Column '%s' already exists, skipping addition.", columnName)
		fmt.Println("e.g.")
		getRows(db, t, 5)
		return
	} else if err != sql.ErrNoRows {
		t.Fatalf("Error checking column existence: %v", err)
	}

	// Add the new column
	alterTableQuery := `ALTER TABLE coredhcp_records ADD COLUMN "` + columnName + `" ` + columnType + `;`
	_, err = db.Exec(alterTableQuery)
	if err != nil {
		t.Fatalf("Failed to add column '%s': %v", columnName, err)
	}

	t.Logf("Successfully added column '%s' to coredhcp_records.", columnName)
    log.Println("e.g.")
	getRows(db, t, 5)
}

func getRows(db *sql.DB, t *testing.T, limit int) {
	query := fmt.Sprintf("SELECT * FROM coredhcp_records LIMIT %d", limit)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to get rows: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if rowCount >= limit {
			break
		}
		err := rows.Scan(valuePtrs...)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		data := make(map[string]interface{})
		for i, colName := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				data[colName] = string(b)
			} else {
				data[colName] = val
			}
		}
		fmt.Printf("Row %d: %v\n", rowCount+1, data)
		rowCount++
	}

	if rowCount == 0 {
		t.Log("No rows found in coredhcp_records.")
	}
}

