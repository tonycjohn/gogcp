package main

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
)

func newDataset(datasetName string, location string) {
	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		panic(err)
	}
	defer bigqueryClient.Close()

	startTime := time.Now()

	//location:="us-east1"
	//location := "us"
	if err = bigqueryClient.Dataset(datasetName).Create(ctx, &bigquery.DatasetMetadata{Location: location}); err != nil {
		infoLogger.Println(err)
		if strings.Contains(err.Error(), "Error 409: Already Exists") {
			infoLogger.Println("DATASET ", datasetName, " EXISTS")
			return
		}
	}
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	infoLogger.Println("CREATED DATASET ", datasetName, " IN:", duration)
}
