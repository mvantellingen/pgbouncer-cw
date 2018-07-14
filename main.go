package main

import (
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/aws/ec2metadata"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type instanceMetadata struct {
	InstanceID string
	Region     string
}

var metadata instanceMetadata

func newDB(url string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", url)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func getInstanceMetadata(cfg aws.Config) {
	log.Println("Retrieving instance metadata")
	svc := ec2metadata.New(cfg)
	result, err := svc.GetInstanceIdentityDocument()
	if err != nil {
		log.Println("Unable to retrieve ec2 instance metadata")
		metadata.InstanceID = ""
		return
	}

	metadata.InstanceID = result.InstanceID
	metadata.Region = result.Region
}

func main() {
	// Parse the command line arguments
	instanceID := flag.String("instance-id", "", "Override default instance id.")
	region := flag.String("region", "", "Override default AWS region.")
	databaseURL := flag.String("url", "postgresql://pgbouncer@:6432/pgbouncer?host=/tmp&sslmode=disable", "The URL to the PGBouncerinstance.")
	interval := flag.Int("interval", 60, "Interval between each run.")
	namespace := flag.String("namespace", "PGBouncer", "The CloudWatch namespace")
	flag.Parse()

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}

	if *instanceID != "" {
		metadata.InstanceID = *instanceID
	} else {
		getInstanceMetadata(cfg)
	}

	if *region != "" {
		metadata.Region = *region
	}

	cfg.Region = metadata.Region
	cwService := cloudwatch.New(cfg)
	stats := statsContext{}
	log.Println("Running")
	for {
		collectStats(*databaseURL, *namespace, &stats, cwService)
		time.Sleep(time.Duration(*interval) * time.Second)
	}
}
