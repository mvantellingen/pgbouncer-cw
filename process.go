package main

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/jmoiron/sqlx"
)

type statsContext struct {
	current  DBStats
	previous DBStats
}

func getData(db *sqlx.DB) (DBStats, error) {
	var stats []Stats
	err := db.Select(&stats, `SHOW STATS_TOTALS`)
	if err != nil {
		return nil, err
	}

	dbStats := make(DBStats)
	total := Stats{IsAggregated: true, TimeStamp: time.Now()}
	for _, item := range stats {
		if item.Database == "pgbouncer" {
			continue
		}

		item.TimeStamp = time.Now()
		total.add(item)
		dbStats[item.Database] = item
	}
	dbStats[total.Database] = total

	db.Close()
	return dbStats, nil
}

func processStats(previous DBStats, current DBStats) []cloudwatch.MetricDatum {
	deltas := make(DBStats)

	for database, stats := range current {
		if _, ok := previous[database]; !ok {
			continue
		}
		deltas[database] = stats.calculatePerSecond(previous[database])
	}

	var metrics []cloudwatch.MetricDatum
	for _, stats := range deltas {
		metrics = stats.addMetricData(metrics)
	}
	return metrics
}

func collectStats(databaseURL string, namespace string, stats *statsContext, svc *cloudwatch.CloudWatch) {
	db, err := newDB(databaseURL)
	if err != nil {
		log.Print("Error connecting to database:", err)
		return
	}
	stats.current, err = getData(db)
	if err != nil {
		log.Print("Error connecting to database:", err)
		return
	}
	db.Close()

	if stats.previous != nil && stats.current != nil {
		metrics := processStats(stats.previous, stats.current)
		pushMetrics(svc, namespace, metrics)
	}

	stats.previous = stats.current
	stats.current = nil
}

func pushMetrics(svc *cloudwatch.CloudWatch, namespace string, metrics []cloudwatch.MetricDatum) {
	log.Printf(
		"Pushing %d metrics to CloudWatch Metrics (InstanceID '%s')\n",
		len(metrics), metadata.InstanceID)

	for i := 0; i < len(metrics); i += 20 {
		request := svc.PutMetricDataRequest(&cloudwatch.PutMetricDataInput{
			MetricData: metrics[i:min(i+20, len(metrics))],
			Namespace:  &namespace,
		})
		_, err := request.Send()
		if err != nil {
			log.Println(err)
		}
	}
}
