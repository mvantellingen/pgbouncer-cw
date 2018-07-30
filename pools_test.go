package main

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestGetPoolData(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"database", "user", "cl_active", "cl_waiting", "sv_active", "sv_idle",
		"sv_used", "sv_tested", "sv_login", "maxwait", "maxwait_us", "pool_mode",
	}).
		AddRow("test_1", "client_1", 3, 4, 5, 6, 7, 8, 9, 10, 11, "transaction").
		AddRow("test_2", "client_2", 13, 14, 15, 16, 17, 18, 19, 110, 111, "transaction")
	mock.ExpectQuery("SHOW POOLS").WillReturnRows(rows)

	sqlxDB := sqlx.NewDb(db, "sqlmock")

	stats, err := getPoolData(sqlxDB)
	assert.Equal(t, nil, err)

	expected := DBPools{
		"test_1": Pool{
			Database:       "test_1",
			User:           "client_1",
			ClientsActive:  3,
			ClientsWaiting: 4,
			ServersActive:  5,
			ServersIdle:    6,
			ServersUsed:    7,
			ServersTested:  8,
			ServersLogin:   9,
			MaxWait:        10,
			MaxWaitUs:      11,
			PoolMode:       "transaction",
			TimeStamp:      stats["test_1"].TimeStamp,
			IsAggregated:   false,
		},
		"test_2": Pool{
			Database:       "test_2",
			User:           "client_2",
			ClientsActive:  13,
			ClientsWaiting: 14,
			ServersActive:  15,
			ServersIdle:    16,
			ServersUsed:    17,
			ServersTested:  18,
			ServersLogin:   19,
			MaxWait:        110,
			MaxWaitUs:      111,
			PoolMode:       "transaction",
			TimeStamp:      stats["test_2"].TimeStamp,
			IsAggregated:   false,
		},
		"": Pool{
			Database:       "",
			User:           "",
			ClientsActive:  16,
			ClientsWaiting: 18,
			ServersActive:  20,
			ServersIdle:    22,
			ServersUsed:    24,
			ServersTested:  26,
			ServersLogin:   28,
			MaxWait:        120,
			MaxWaitUs:      122,
			PoolMode:       "",
			TimeStamp:      stats[""].TimeStamp,
			IsAggregated:   true,
		}}

	assert.Equal(t, expected, stats)
}
