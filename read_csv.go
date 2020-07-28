package main

import (
	"fmt"
	"encoding/csv"
	"io"
	"log"
	"os"
)


func main() {

	// Open the file
	csvfile, err := os.Open("tweet_data.csv")
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)

	r.FieldsPerRecord = -1

	// Iterate through the records
	for {
		// Read each record from csv
		record, err := r.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(record[0], record[1])
	}

}
