package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessStats(t *testing.T) {
	metadata.detailedMonitoring = true
	current := statusPoint{
		stats: DBStats{
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
		},
		pools: DBPools{
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
			},
		},
	}

	previous := statusPoint{
		stats: DBStats{
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
		},
		pools: DBPools{
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
			},
		},
	}

	result := processStats(previous, current)
	assert.Equal(t, 8, len(result))
}
