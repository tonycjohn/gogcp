package main

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"
)

func newTableFromHivePartition(datasetID string, tableID string, tableSource string, tableSourceUriPrefix string, clusterKeys []string, partitionKey string, partitionRangeStart int64, partitionRangeEnd int64, partitionRangeInterval int64) {

	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		infoLogger.Panicln(err)
	}
	defer bigqueryClient.Close()

	startTime := time.Now()

	originalMeta := &bigquery.TableMetadata{}

	infoLogger.Println("STARTED CREATING TABLE:", datasetID, tableID, tableSource, " CLUSTER KEYS:", clusterKeys, " PARTITION KEY:", partitionKey)

	var relaxedSchema bigquery.Schema

	for _, originalSchema := range originalMeta.Schema {
		originalSchema.Required = false
		relaxedSchema = append(relaxedSchema, originalSchema)
	}

	newTableMetadata := &bigquery.TableMetadata{
		Schema: relaxedSchema,
	}

	if len(clusterKeys) > 0 {
		newTableMetadata.Clustering = &bigquery.Clustering{
			Fields: clusterKeys,
		}
	}
	if len(partitionKey) > 0 {
		newTableMetadata.RangePartitioning = &bigquery.RangePartitioning{
			Field: partitionKey,
			Range: &bigquery.RangePartitioningRange{
				Start:    partitionRangeStart,
				End:      partitionRangeEnd,
				Interval: partitionRangeInterval,
			},
		}
	}

	/*newTableMetadata.ExternalDataConfig = &bigquery.ExternalDataConfig{
		SourceFormat:        bigquery.Parquet,
		SourceURIs:          []string{tableSource},
		AutoDetect:          true,
		IgnoreUnknownValues: false,
		MaxBadRecords:       0,
		Options:             nil,
		HivePartitioningOptions: &bigquery.HivePartitioningOptions{
			Mode:                   bigquery.AutoHivePartitioningMode,
			SourceURIPrefix:        "gs://nlsn-dataproc-createdb/hive-warehouse/pqm/RMS_9000_PREAGG_US_240_UAT_20210302.db/trag_aggregated_data",
			RequirePartitionFilter: false,
		},
	} */

	newTableRef := bigqueryClient.Dataset(datasetID).Table(tableID)
	if err = newTableRef.Create(ctx, newTableMetadata); err != nil {
		infoLogger.Panicln(tableID, err)
	}

	//Load new table files
	gcsRef := bigquery.NewGCSReference(tableSource)
	gcsRef.SourceFormat = bigquery.Parquet
	loader := bigqueryClient.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	//loader.WriteDisposition = bigquery.WriteTruncate
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
