package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func newStorageBucket(bucketName string, location string) {
	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	storageAttributes := storage.BucketAttrs{}
	storageAttributes.Location = location
	//storageAttributes.Location = "us-east1"

	bucket := storageClient.Bucket(bucketName)
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err = bucket.Create(ctx, projectID, &storageAttributes); err != nil {
		panic(err)
	}

	//***test looking for  specific files in Storage Bucket
	//pdm-pqm-testbucket/pdm_us_aod_kao_rms_200002.db/lu_per_periodicity
	tableSource := "gs://pdm-pqm-testbucket/pdm_us_unfi_rms_cps_1000572.db/lu_prod_product/*.parquet"
	input := strings.SplitAfter(tableSource, "gs://")
	input = strings.Split(input[1], "/")
	prefix := input[1] + "/" + input[2] + "/"
	fmt.Println(prefix)

	storageBucketHandle := storageClient.Bucket("pdm-pqm-testbucket")
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
			fmt.Println(storageObject.Name)
			break
		}
	}

	//***test traversing Storage Bucket
	/*it := storageClient.Buckets(ctx, projectID)
	for {
		bucket, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Println(bucket.Name)
		bucktHandle := storageClient.Bucket(bucket.Name)
		storageQuery := storage.Query{
			Delimiter:   "/",
			Prefix:      "pdm",
			Versions:    false,
			StartOffset: "",
			EndOffset:   "",
		}
		bucketObjects := bucktHandle.Objects(ctx, &storageQuery)
		for {
			bucketObject, err := bucketObjects.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				panic(err)
			}
			fmt.Println(bucketObject.Name)
		}
	}*/

}
