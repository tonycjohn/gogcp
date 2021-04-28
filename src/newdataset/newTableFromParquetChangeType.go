package main

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"
)

func newTableFromParquetChangeType(datasetID string, tableID string, tableSource string, clusterKeys []string, partitionKey string, partitionRangeStart int64, partitionRangeEnd int64, partitionRangeInterval int64) {

	tempTableID := tableID + "TEMP"

	infoLogger.Println("STARTED CREATING TEMP TABLE: ", datasetID, tempTableID, tableSource)

	parquetFile := getParquetFileName(tableSource)
	infoLogger.Println("PARQUET FILE FOR TEMP TABLE: ", parquetFile)

	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		infoLogger.Panicln(err)
	}
	defer bigqueryClient.Close()

	startTime := time.Now()

	tableMetadata := &bigquery.TableMetadata{}

	tableRef := bigqueryClient.Dataset(datasetID).Table(tempTableID)
	if err = tableRef.Create(ctx, tableMetadata); err != nil {
		infoLogger.Panicln(err)
	}

	//Load one parquet file to get the schema.
	gcsRef := bigquery.NewGCSReference(parquetFile)
	gcsRef.SourceFormat = bigquery.Parquet
	loader := bigqueryClient.Dataset(datasetID).Table(tempTableID).LoaderFrom(gcsRef)

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

	infoLogger.Println("STARTED CREATING TABLE:", datasetID, tableID, tableSource, " CLUSTER KEYS:", clusterKeys, " PARTITION KEY:", partitionKey)

	//Relax Table Schema
	var relaxedSchema bigquery.Schema
	originalMeta, err := tableRef.Metadata(ctx)
	if err != nil {
		infoLogger.Panicln(err)
	}
	for _, originalSchema := range originalMeta.Schema {
		infoLogger.Println("Temp Schema - Name:", originalSchema.Name, ", Required:", originalSchema.Required, ", Type:", originalSchema.Type)
		originalSchema.Required = false
		var fieldType bigquery.FieldType
		fieldType = "INT64"
		originalSchema.Type = fieldType
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

	newTableRef := bigqueryClient.Dataset(datasetID).Table(tableID)
	if err = newTableRef.Create(ctx, newTableMetadata); err != nil {
		infoLogger.Panicln(tableID, err)
	}

	//Load new table files
	gcsRef = bigquery.NewGCSReference(tableSource)
	gcsRef.SourceFormat = bigquery.Parquet
	loader = bigqueryClient.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	//loader.WriteDisposition = bigquery.WriteTruncate
	loader.SchemaUpdateOptions = []string{"ALLOW_FIELD_RELAXATION"}

	job, err = loader.Run(ctx)
	if err != nil {
		infoLogger.Panicln(err)
	}
	status, err = job.Wait(ctx)
	if err != nil {
		infoLogger.Panicln(err)
	}
	if status.Err() != nil {
		infoLogger.Println(tableID, "--", status.Err().Error())
	}

	relaxMeta, err := newTableRef.Metadata(ctx)
	if err != nil {
		infoLogger.Panicln(err)
	}
	for _, newSchema := range relaxMeta.Schema {
		infoLogger.Println("New Schema - Name:", newSchema.Name, ", Required:", newSchema.Required, ", Type:", newSchema.Type)
	}

	endtime := time.Now()
	duration := endtime.Sub(startTime)

	infoLogger.Println("CREATED TABLE IN: ", duration, datasetID, tableID, tableSource)
	err = tableRef.Delete(ctx)
	if err != nil {
		infoLogger.Panicln(err)
	}
	infoLogger.Println("DELETED TEMP TABLE: ", tempTableID)

	wg.Done()

}

/*func getParquetFileName(tableSource string) (parquetFile string) {
	input := strings.SplitAfter(tableSource, "gs://")
	input = strings.Split(input[1], "/")
	var prefix string
	for i := 1; i < len(input)-1; i++ {
		prefix = prefix + input[i] + "/"
	}
	infoLogger.Println(prefix)

	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	storageBucketHandle := storageClient.Bucket(input[0])
	storageQuery := storage.Query{
		Delimiter:   "/",
		Prefix:      prefix,
		Versions:    false,
		StartOffset: "",
		EndOffset:   "",
	}
	storageObjects := storageBucketHandle.Objects(ctx, &storageQuery)
	for {
		storageObject, err := storageObjects.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
		if strings.Contains(storageObject.Name, ".snappy.parquet") {
			parquetFileName := "gs://" + input[0] + "/" + storageObject.Name
			return parquetFileName
		}
	}
	return ""

}*/
