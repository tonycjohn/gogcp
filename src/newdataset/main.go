package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

const (
	projectID      = "nlsn-connect-data-eng-poc"
	configFileName = "\\newDatasetConfig.json"
)

var wg = sync.WaitGroup{}
var infoLogger *log.Logger

//Table represents each BQ table
type Table struct {
	TableID             string   `json:"tableId"`
	TableSource         string   `json:"tableSource"`
	ClusterKeys         []string `json:"clusterkeys"`
	PartitionKey        string   `json:"partitionkey"`
	PartitionRangeStart int64    `json:"partitionrangestart"`
	PartitionRangeEnd   int64    `json:"partitionrangeend"`
	PartitionInterval   int64    `json:"partitioninterval"`
}

//View represents each BQ view
type View struct {
	ViewID string `json:"viewId"`
	Query  string `json:"query"`
}

//NewDatasetConfig represents the dataset and tables in BQ
type NewDatasetConfig struct {
	DatasetID string  `json:"datasetId"`
	Location  string  `json:"location"`
	Tables    []Table `json:"tables"`
	Views     []View  `json:"views"`
}

func init() {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	logFileName := wd + "\\gcplog.txt"
	logFile, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	infoLogger = log.New(logFile, "INFO: ", log.Ldate|log.Ltime|log.LUTC|log.Lmicroseconds|log.Lshortfile)
}

func main() {

	fmt.Println("Hello! GCP")

	var config NewDatasetConfig
	wd, err := os.Getwd()
	if err != nil {
		infoLogger.Panicln(err)
	}

	configFile, err := os.Open(wd + configFileName)
	if err != nil {
		infoLogger.Panicln(err)
	}
	defer configFile.Close()

	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&config)
	if err != nil {
		infoLogger.Panicln(err)
	}

	//storage test
	//newStorageBucket("location_test_bucket_8", config.Location)
	//end storage test

	newDataset(config.DatasetID, config.Location)

	for _, table := range config.Tables {
		wg.Add(1)
		//go newTableFromParquet(config.DatasetID, table.TableID, table.TableSource, table.ClusterKeys, table.PartitionKey, table.PartitionRangeStart, table.PartitionRangeEnd, table.PartitionInterval)
		go newTableFromHivePartition(config.DatasetID, table.TableID, table.TableSource, "", table.ClusterKeys, table.PartitionKey, table.PartitionRangeStart, table.PartitionRangeEnd, table.PartitionInterval)
	}
	wg.Wait()

	for _, view := range config.Views {
		wg.Add(1)
		go newView(config.DatasetID, view.ViewID, view.Query)
	}

	wg.Wait()

}
