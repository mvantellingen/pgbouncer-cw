package main

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/jmoiron/sqlx"
)

type Pool struct {
	Database       string  `db:"database"`
	User           string  `db:"user"`
	ClientsActive  float64 `db:"cl_active"`
	ClientsWaiting float64 `db:"cl_waiting"`
	ServersActive  float64 `db:"sv_active"`
	ServersIdle    float64 `db:"sv_idle"`
	ServersUsed    float64 `db:"sv_used"`
	ServersTested  float64 `db:"sv_tested"`
	ServersLogin   float64 `db:"sv_login"`
	MaxWait        float64 `db:"maxwait"`
	MaxWaitUs      float64 `db:"maxwait_us"`
	PoolMode       string  `db:"pool_mode"`
	TimeStamp      time.Time
	IsAggregated   bool
}

type DBPools map[string]Pool

func (p *Pool) add(o Pool) {
	p.ClientsActive += o.ClientsActive
	p.ClientsWaiting += o.ClientsWaiting
	p.ServersActive += o.ServersActive
	p.ServersIdle += o.ServersIdle
	p.ServersUsed += o.ServersUsed
	p.ServersTested += o.ServersTested
	p.ServersLogin += o.ServersLogin
	p.MaxWait += o.MaxWait
	p.MaxWaitUs += o.MaxWaitUs
}

func getPoolData(db *sqlx.DB) (DBPools, error) {
	var pools []Pool
	err := db.Select(&pools, `SHOW POOLS`)
	if err != nil {
		return nil, err
	}

	dbPools := make(DBPools)
	total := Pool{IsAggregated: true, TimeStamp: time.Now()}
	for _, item := range pools {
		total.add(item)
		item.TimeStamp = time.Now()
		dbPools[item.Database] = item
	}
	dbPools[total.Database] = total
	return dbPools, nil
}

func (p *Pool) addMetricData(dest []cloudwatch.MetricDatum) []cloudwatch.MetricDatum {

	items := map[string]struct {
		value float64
		unit  cloudwatch.StandardUnit
	}{
		"ServersIdle":   {p.ServersIdle, cloudwatch.StandardUnitCount},
		"ServersActive": {p.ServersActive, cloudwatch.StandardUnitCount},
	}

	if p.IsAggregated {
		dimension := cloudwatch.Dimension{
			Name:  stringPtr("Across all instances"),
			Value: stringPtr("instances"),
		}
		for key, item := range items {
			dest = append(dest, p.createMetricDatum(key, item.value, item.unit, dimension))
		}

		dimension = cloudwatch.Dimension{
			Name:  stringPtr("InstanceId"),
			Value: stringPtr(metadata.InstanceID),
		}
		for key, item := range items {
			dest = append(dest, p.createMetricDatum(key, item.value, item.unit, dimension))
		}
	} else {
		dimension := cloudwatch.Dimension{
			Name:  stringPtr("Database"),
			Value: stringPtr(p.Database),
		}

		for key, item := range items {
			dest = append(dest, p.createMetricDatum(key, item.value, item.unit, dimension))
		}
	}
	return dest
}

func (p *Pool) createMetricDatum(
	name string,
	value float64,
	unit cloudwatch.StandardUnit,
	dimension cloudwatch.Dimension,
) cloudwatch.MetricDatum {
	return cloudwatch.MetricDatum{
		MetricName: &name,
		Dimensions: []cloudwatch.Dimension{dimension},
		Timestamp:  &p.TimeStamp,
		Unit:       unit,
		Value:      &value,
	}
}
