package main

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"
)

func newTable(datasetID string, tableID string, tableSource string) {

	infoLogger.Println("STARTED CREATING TABLE: ", datasetID, tableID, tableSource)

	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		infoLogger.Panicln(err)
	}
	defer bigqueryClient.Close()

	startTime := time.Now()

	//create table with empty schema
	tableRef := bigqueryClient.Dataset(datasetID).Table(tableID)
	if err = tableRef.Create(ctx, &bigquery.TableMetadata{}); err != nil {
		infoLogger.Panicln(err)
	}

	gcsRef := bigquery.NewGCSReference(tableSource)
	gcsRef.SourceFormat = bigquery.Parquet
	gcsRef.AutoDetect = true

	loader := bigqueryClient.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.SchemaUpdateOptions = []string{"ALLOW_FIELD_RELAXATION"}

	job, err := loader.Run(ctx)
	if err != nil {
		infoLogger.Panicln(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		infoLogger.Panicln(err)
	}
	if status.Err() != nil {
		infoLogger.Println(tableID, "--", status.Err().Error())
	}
	endtime := time.Now()
	duration := endtime.Sub(startTime)

	infoLogger.Println("CREATED TABLE IN: ", duration, datasetID, tableID, tableSource)

	wg.Done()

}
