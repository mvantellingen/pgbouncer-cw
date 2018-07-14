package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"

	"github.com/stretchr/testify/assert"
)

func TestStatsDelta(t *testing.T) {

	current := Stats{
		Database:         "test",
		QueryCount:       497,
		QueryTime:        1083859,
		WaitTime:         59013,
		TransactionCount: 497,
		TransactionTime:  1083859,
		BytesReceived:    691490,
		BytesSent:        578486,
		TimeStamp:        time.Now(),
	}

	previous := Stats{
		Database:         current.Database,
		QueryCount:       491,
		QueryTime:        1072520,
		WaitTime:         59013,
		TransactionCount: 491,
		TransactionTime:  1072520,
		BytesReceived:    682412,
		BytesSent:        570992,
		TimeStamp:        current.TimeStamp.Add(0 - time.Duration(1300*time.Millisecond)),
	}

	expected := Stats{
		Database:         current.Database,
		QueryCount:       4.615384615384615,
		QueryTime:        1.8898333333333333,
		WaitTime:         0,
		TransactionCount: 4.615384615384615,
		TransactionTime:  1.8898333333333333,
		BytesReceived:    6983.076923076923,
		BytesSent:        5764.615384615385,
		TimeStamp:        current.TimeStamp,
	}

	delta := current.calculatePerSecond(previous)

	assert.Equal(t, expected, delta)
}

func TestStatsDeltaNoActivity(t *testing.T) {

	current := Stats{
		Database:         "test",
		QueryCount:       120,
		QueryTime:        120 * 1000,
		WaitTime:         120 * 1000,
		TransactionCount: 220,
		TransactionTime:  1200 * 1000,
		BytesReceived:    1000,
		BytesSent:        2400,
		TimeStamp:        time.Now(),
	}

	previous := Stats{
		Database:         current.Database,
		QueryCount:       120,
		QueryTime:        120 * 1000,
		WaitTime:         120 * 1000,
		TransactionCount: 220,
		TransactionTime:  1200 * 1000,
		BytesReceived:    1000,
		BytesSent:        2400,
		TimeStamp:        current.TimeStamp.Add(0 - time.Duration(60*time.Second)),
	}

	expected := Stats{
		Database:         current.Database,
		QueryCount:       0,
		QueryTime:        0,
		WaitTime:         0,
		TransactionCount: 0,
		TransactionTime:  0,
		BytesReceived:    0,
		BytesSent:        0,
		TimeStamp:        current.TimeStamp,
	}

	delta := current.calculatePerSecond(previous)

	assert.Equal(t, expected, delta)
}

func TestStatsAdd(t *testing.T) {

	current := Stats{
		Database:         "test",
		QueryCount:       100,
		QueryTime:        200,
		WaitTime:         300,
		TransactionCount: 400,
		TransactionTime:  500,
		BytesReceived:    600,
		BytesSent:        700,
		TimeStamp:        time.Now(),
	}

	other := Stats{
		Database:         "test",
		QueryCount:       100,
		QueryTime:        200,
		WaitTime:         300,
		TransactionCount: 400,
		TransactionTime:  500,
		BytesReceived:    600,
		BytesSent:        700,
		TimeStamp:        time.Now(),
	}

	current.add(other)

	expected := Stats{
		Database:         "test",
		QueryCount:       200,
		QueryTime:        400,
		WaitTime:         600,
		TransactionCount: 800,
		TransactionTime:  1000,
		BytesReceived:    1200,
		BytesSent:        1400,
		TimeStamp:        current.TimeStamp,
	}
	assert.Equal(t, expected, current)
}

func TestStatsEmpty(t *testing.T) {

	current := Stats{
		Database:         "test",
		QueryCount:       0,
		QueryTime:        0,
		WaitTime:         0,
		TransactionCount: 0,
		TransactionTime:  0,
		BytesReceived:    0,
		BytesSent:        0,
		TimeStamp:        time.Now(),
	}

	assert.True(t, current.isEmpty())
}

func TestStatsAddMetricData(t *testing.T) {
	instance := Stats{
		Database:         "test",
		QueryCount:       497,
		QueryTime:        1083859,
		WaitTime:         59013,
		TransactionCount: 497,
		TransactionTime:  1083859,
		BytesReceived:    691490,
		BytesSent:        578486,
		TimeStamp:        time.Now(),
	}

	metrics := []cloudwatch.MetricDatum{}
	instance.addMetricData(metrics)
}

func TestStatsAddMetricDataAggregated(t *testing.T) {
	instance := Stats{
		Database:         "test",
		QueryCount:       497,
		QueryTime:        1083859,
		WaitTime:         59013,
		TransactionCount: 497,
		TransactionTime:  1083859,
		BytesReceived:    691490,
		BytesSent:        578486,
		TimeStamp:        time.Now(),
		IsAggregated:     true,
	}

	metrics := []cloudwatch.MetricDatum{}
	instance.addMetricData(metrics)
}

func TestStatsAddMetricDataEmpty(t *testing.T) {
	instance := Stats{
		Database:         "test",
		QueryCount:       0,
		QueryTime:        0,
		WaitTime:         0,
		TransactionCount: 0,
		TransactionTime:  0,
		BytesReceived:    0,
		BytesSent:        0,
		TimeStamp:        time.Now(),
		IsAggregated:     true,
	}

	metrics := []cloudwatch.MetricDatum{}
	instance.addMetricData(metrics)
}
