package main

import (
	"context"
	"time"

	"cloud.google.com/go/bigquery"
)

func newTableFromSchema(datasetID string, tableID string, tableSource string) {

	/*type tableModel struct {
		month_id       bigquery.NullInt64
		month_sdesc    bigquery.NullString
		month_ldesc    bigquery.NullString
		month_end_date bigquery.NullDateTime
		display_order  bigquery.NullInt64
		month_of_year  bigquery.NullInt64
		quarter_id     bigquery.NullInt64
		year_id        bigquery.NullInt64
		month_ya       bigquery.NullInt64
		month_2ya      bigquery.NullInt64
		month_3ya      bigquery.NullInt64
		month_prior    bigquery.NullInt64
	}*/

	var fieldSchema bigquery.FieldSchema
	fieldSchema.Name = "month_id"
	fieldSchema.Required = false
	fieldSchema.Type = bigquery.IntegerFieldType

	var fieldSchemas []bigquery.FieldSchema
	fieldSchemas = append(fieldSchemas, fieldSchema)

	//tableSchema := bigquery.Schema{*fieldSchemas}

	infoLogger.Println("STARTED CREATING TABLE: ", datasetID, tableID, tableSource)

	parquetFile := getParquetFileName(tableSource)
	infoLogger.Println("Parquet File Name: ", parquetFile)

	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		infoLogger.Panicln(err)
	}
	defer bigqueryClient.Close()

	startTime := time.Now()

	tableMetadata := &bigquery.TableMetadata{}

	tableRef := bigqueryClient.Dataset(datasetID).Table(tableID)
	if err = tableRef.Create(ctx, tableMetadata); err != nil {
		infoLogger.Panicln(err)
	}

	//Load one parquet file to get the schema.
	gcsRef := bigquery.NewGCSReference(parquetFile)
	gcsRef.SourceFormat = bigquery.Parquet
	//gcsRef.AutoDetect = true
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

	//Relax Table Schema
	var relaxedSchema bigquery.Schema
	originalMeta, err := tableRef.Metadata(ctx)
	if err != nil {
		infoLogger.Panicln(err)
	}

	for _, originalSchema := range originalMeta.Schema {
		infoLogger.Println("Parquet Schema Required Value :", originalSchema.Name, originalSchema.Required, originalSchema.Type)
		originalSchema.Required = false
		relaxedSchema = append(relaxedSchema, originalSchema)
	}

	newTableMetadata := bigquery.TableMetadataToUpdate{
		Schema: relaxedSchema,
	}

	for _, s := range newTableMetadata.Schema {
		infoLogger.Println("New Schema Required Value :", s.Name, s.Required, s.Type)
	}

	tableRef.Update(ctx, newTableMetadata, originalMeta.ETag)

	//Load all other files
	gcsRef = bigquery.NewGCSReference(tableSource)
	gcsRef.SourceFormat = bigquery.Parquet
	loader = bigqueryClient.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate

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

	endtime := time.Now()
	duration := endtime.Sub(startTime)

	infoLogger.Println("CREATED TABLE IN: ", duration, datasetID, tableID, tableSource)

	wg.Done()

}
