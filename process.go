package main

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/jmoiron/sqlx"
)

type statusLog struct {
	current  *statusPoint
	previous *statusPoint
}

type statusPoint struct {
	stats DBStats
	pools DBPools
}

func getData(db *sqlx.DB) (*statusPoint, error) {
	status := statusPoint{}
	stats, err := getStatsData(db)
	if err != nil {
		return nil, err
	}
	status.stats = stats

	if metadata.detailedMonitoring {
		pools, err := getPoolData(db)
		if err != nil {
			return nil, err
		}
		status.pools = pools
	}
	return &status, err
}

func processStats(previous statusPoint, current statusPoint) []cloudwatch.MetricDatum {

	var metrics []cloudwatch.MetricDatum

	// Generate metrics for delta of stats
	deltas := current.stats.getDelta(previous.stats)
	for _, stats := range deltas {
		metrics = stats.addMetricData(metrics)
	}

	// Generate metrics for pools
	if metadata.detailedMonitoring {
		for _, pool := range current.pools {
			metrics = pool.addMetricData(metrics)
		}
	}
	return metrics
}

func collectStats(databaseURL string, namespace string, status *statusLog, svc *cloudwatch.CloudWatch) {
	db, err := newDB(databaseURL)
	if err != nil {
		log.Print("Error connecting to database:", err)
		return
	}
	status.current, err = getData(db)
	if err != nil {
		log.Print("Error connecting to database:", err)
		return
	}
	db.Close()

	if status.previous != nil && status.current != nil {
		metrics := processStats(*status.previous, *status.current)
		pushMetrics(svc, namespace, metrics)
	}

	status.previous = status.current
	status.current = nil
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
