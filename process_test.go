package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/jmoiron/sqlx"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestGetData(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"database", "query_count", "query_time", "wait_time", "xact_count",
		"xact_time", "bytes_received", "bytes_sent",
	}).
		AddRow("test_1", 100, 15000, 1200, 1, 15000, 256, 512).
		AddRow("test_2", 150, 18000, 1200, 1, 15400, 2048, 4096).
		AddRow("pgbouncer", 150, 18000, 1200, 1, 15400, 2048, 4096)

	mock.ExpectQuery("SHOW STATS_TOTALS").WillReturnRows(rows)

	sqlxDB := sqlx.NewDb(db, "sqlmock")

	stats, err := getData(sqlxDB)
	assert.Equal(t, nil, err)

	expected := DBStats{
		"test_1": Stats{
			Database:         "test_1",
			QueryCount:       100,
			QueryTime:        15000,
			WaitTime:         1200,
			TransactionCount: 1,
			TransactionTime:  15000,
			BytesReceived:    256,
			BytesSent:        512,
			TimeStamp:        stats["test_1"].TimeStamp,
			IsAggregated:     false,
		},
		"test_2": Stats{
			Database:         "test_2",
			QueryCount:       150,
			QueryTime:        18000,
			WaitTime:         1200,
			TransactionCount: 1,
			TransactionTime:  15400,
			BytesReceived:    2048,
			BytesSent:        4096,
			TimeStamp:        stats["test_2"].TimeStamp,
			IsAggregated:     false,
		},
		"": Stats{
			Database:         "",
			QueryCount:       250,
			QueryTime:        33000,
			WaitTime:         2400,
			TransactionCount: 2,
			TransactionTime:  30400,
			BytesReceived:    2304,
			BytesSent:        4608,
			TimeStamp:        stats[""].TimeStamp,
			IsAggregated:     true,
		}}

	assert.Equal(t, expected, stats)
}

func TestProcessStats(t *testing.T) {
	current := DBStats{
		"test_1": Stats{
			Database:         "test",
			QueryCount:       100,
			QueryTime:        200,
			WaitTime:         300,
			TransactionCount: 400,
			TransactionTime:  500,
			BytesReceived:    600,
			BytesSent:        700,
			TimeStamp:        time.Now(),
		},
		"test_2": Stats{
			Database:         "test_2",
			QueryCount:       100,
			QueryTime:        200,
			WaitTime:         300,
			TransactionCount: 400,
			TransactionTime:  500,
			BytesReceived:    600,
			BytesSent:        700,
			TimeStamp:        time.Now(),
		},
	}

	previous := DBStats{
		"test_1": Stats{
			Database:         "test_1",
			QueryCount:       0,
			QueryTime:        0,
			WaitTime:         0,
			TransactionCount: 0,
			TransactionTime:  0,
			BytesReceived:    0,
			BytesSent:        0,
			TimeStamp:        time.Now(),
		},
		"test_2": Stats{
			Database:         "test_2",
			QueryCount:       0,
			QueryTime:        0,
			WaitTime:         0,
			TransactionCount: 0,
			TransactionTime:  0,
			BytesReceived:    0,
			BytesSent:        0,
			TimeStamp:        time.Now(),
		},
	}

	result := processStats(previous, current)
	assert.Equal(t, 6, len(result))
}
