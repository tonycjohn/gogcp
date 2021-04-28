package main

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"
)

func newView(datasetID string, viewID string, query string) {

	infoLogger.Println("STARTED CREATING VIEW: ", datasetID, viewID)

	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		infoLogger.Panicln(err)
	}
	defer bigqueryClient.Close()

	startTime := time.Now()

	//create View with query
	meta := &bigquery.TableMetadata{ViewQuery: query}
	tableRef := bigqueryClient.Dataset(datasetID).Table(viewID)
	if err = tableRef.Create(ctx, meta); err != nil {
		infoLogger.Panicln(err)
	}
	endtime := time.Now()
	duration := endtime.Sub(startTime)

	infoLogger.Println("CREATED VIEW IN: ", duration, datasetID, viewID)

	wg.Done()

}
