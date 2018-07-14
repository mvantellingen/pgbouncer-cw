package main

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
)

type Stats struct {
	Database         string  `db:"database"`
	QueryCount       float64 `db:"query_count"`
	QueryTime        float64 `db:"query_time"`
	WaitTime         float64 `db:"wait_time"`
	TransactionCount float64 `db:"xact_count"`
	TransactionTime  float64 `db:"xact_time"`
	BytesReceived    float64 `db:"bytes_received"`
	BytesSent        float64 `db:"bytes_sent"`
	TimeStamp        time.Time
	IsAggregated     bool
}

type DBStats map[string]Stats

func (s *Stats) calculatePerSecond(p Stats) Stats {
	duration := s.TimeStamp.Sub(p.TimeStamp).Nanoseconds()
	queryCount := s.QueryCount - p.QueryCount
	transactionCount := s.TransactionCount - p.TransactionCount

	result := Stats{
		Database:         s.Database,
		QueryCount:       calcDurationDelta(s.QueryCount, p.QueryCount, duration),
		WaitTime:         calcDurationDelta(s.WaitTime, p.WaitTime, duration),
		TransactionCount: calcDurationDelta(s.TransactionCount, p.TransactionCount, duration),
		TimeStamp:        s.TimeStamp,
		BytesReceived:    calcDurationDelta(s.BytesReceived, p.BytesReceived, duration),
		BytesSent:        calcDurationDelta(s.BytesSent, p.BytesSent, duration),

		IsAggregated: s.IsAggregated,
	}
	// Calculate the times based on the number of hits, conver to milliseconds.
	if queryCount > 0 {
		result.QueryTime = ((s.QueryTime - p.QueryTime) / queryCount) / 1000
	}
	if transactionCount > 0 {
		result.TransactionTime = ((s.TransactionTime - p.TransactionTime) / transactionCount) / 1000
	}
	return result
}

func calcDurationDelta(cur float64, prev float64, duration int64) float64 {
	return ((cur - prev) / float64(duration)) * float64(time.Second)
}

func (s *Stats) add(o Stats) {
	s.QueryCount += o.QueryCount
	s.QueryTime += o.QueryTime
	s.WaitTime += o.WaitTime
	s.TransactionCount += o.TransactionCount
	s.TransactionTime += o.TransactionTime
	s.BytesReceived += o.BytesReceived
	s.BytesSent += o.BytesSent
}

func (s *Stats) isEmpty() bool {
	return s.QueryCount == 0 && s.TransactionCount == 0 && s.WaitTime == 0
}

func (s *Stats) addMetricData(dest []cloudwatch.MetricDatum) []cloudwatch.MetricDatum {
	if s.isEmpty() {
		return dest
	}

	items := map[string]struct {
		value float64
		unit  cloudwatch.StandardUnit
	}{
		"QueryCount": {s.QueryCount, cloudwatch.StandardUnitCountSecond},
		"QueryTime":  {s.QueryTime, cloudwatch.StandardUnitMilliseconds},
		"WaitTime":   {s.WaitTime, cloudwatch.StandardUnitMilliseconds},
	}

	if s.IsAggregated {
		dimension := cloudwatch.Dimension{
			Name:  stringPtr("Across all instances"),
			Value: stringPtr("instances"),
		}
		for _, key := range []string{"QueryCount", "QueryTime", "WaitTime"} {
			dest = append(dest, s.createMetricDatum(key, items[key].value, items[key].unit, dimension))
		}

		dimension = cloudwatch.Dimension{
			Name:  stringPtr("InstanceId"),
			Value: stringPtr(metadata.InstanceID),
		}
		for _, key := range []string{"QueryCount", "QueryTime", "WaitTime"} {
			dest = append(dest, s.createMetricDatum(key, items[key].value, items[key].unit, dimension))
		}
	} else {
		dimension := cloudwatch.Dimension{
			Name:  stringPtr("Database"),
			Value: stringPtr(s.Database),
		}

		for _, key := range []string{"QueryCount", "QueryTime", "WaitTime"} {
			dest = append(dest, s.createMetricDatum(key, items[key].value, items[key].unit, dimension))
		}
	}
	return dest
}

func (s *Stats) createMetricDatum(
	name string,
	value float64,
	unit cloudwatch.StandardUnit,
	dimension cloudwatch.Dimension,
) cloudwatch.MetricDatum {
	return cloudwatch.MetricDatum{
		MetricName: &name,
		Dimensions: []cloudwatch.Dimension{dimension},
		Timestamp:  &s.TimeStamp,
		Unit:       unit,
		Value:      &value,
	}
}
