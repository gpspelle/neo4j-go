package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
	"unicode"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func isMn(r rune) bool {
    return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

func read_csv(filename string) *csv.Reader {

	// Open the file
	csvfile, err := os.Open(filename)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)

	r.FieldsPerRecord = -1

	return r
}

func process_array_string(x string) []string {

	x = strings.Replace(x, "[", "", -1)
	x = strings.Replace(x, "]", "", -1)
	x = strings.Replace(x, "'", "", -1)
	x = strings.Replace(x, " ", "", -1)
	splited := strings.Split(x, ",")

	lower_case := []string{}

	for _, s := range splited {
		lower_case = append(lower_case, strings.ToLower(s))
	}

	return lower_case
}

func main() {

	t := transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC)
	output_file, err := os.Create("clean_hashtag_data.csv")
	csvwriter := csv.NewWriter(output_file)

	if err != nil {
		log.Fatalln("Failed to create csv file", err)
	}
	r := read_csv("tweet_data.csv")

	header_line, err := r.Read() // skip header

	if err == io.EOF {
		log.Fatalln("Empty csv", err)
	}

	if err != nil {
		log.Fatal(err)
	}

	write_line := []string{}

	for _, header := range header_line {
		write_line = append(write_line, header)
	}

	err = csvwriter.Write(write_line)

	if err != nil {
		log.Fatalln("Failed to write header", err)
	}

	csvwriter.Flush()

	// Iterate through the records
	index := 0
	for {
		index += 1

		// Read each record from csv
		record, err := r.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err)
		}

		hashtag_string := record[2]
		hashtags := process_array_string(hashtag_string)

		new_hashtags := []string{}
		for _, hashtag := range hashtags {
			result, _, _ := transform.String(t, hashtag)
			new_hashtags = append(new_hashtags, result)
		}

		write_line := []string{}
		for index_r, data := range record {

			if index_r != 2 {
				write_line = append(write_line, data) // middle normal one
			} else { // middle, but hashtags
				hashtag_column := ""

				for index_h, new_hashtag := range new_hashtags {
					if index_h != len(new_hashtags) - 1 {
						hashtag_column += "'"+new_hashtag+"',"
					} else {
						hashtag_column += "'"+new_hashtag+"'"
					}
				}

				write_line = append(write_line, hashtag_column)
			}

		}

		err = csvwriter.Write(write_line)

		if err != nil {
			log.Fatalln("Failed to write a data line", err)
		}

		csvwriter.Flush()

	}

	output_file.Close()


}
