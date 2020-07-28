package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
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

	session, driver, err := open_connection("neo4j", "neo4j")

	if err != nil {
		log.Fatalln("Failed to connect to neo4j", err)
	}


	sess := *session
	dri := *driver

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

		err = write_node(sess, record)

		if err != nil {
			log.Fatalln("Failed to write in the database", err)
		}

		if index % 500 == 0 {
			fmt.Printf("Created %d nodes\n", index)
		}
	}

	fmt.Printf("Created %d nodes\n", index)


	err = delete_all(sess)

	if err != nil {
		log.Fatalln("Failed to delete nodes", err)
	} else {
		fmt.Printf("Created %d nodes\n", index)
	}

	sess.Close()
	// handle driver lifetime based on your application lifetime requirements
	// driver's lifetime is usually bound by the application lifetime, which usually implies one driver instance per application
	dri.Close()

}

func open_connection(user string, passwd string) (*neo4j.Session, *neo4j.Driver, error) {

	configForNeo4j40 := func(conf *neo4j.Config) { conf.Encrypted = false }
	driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth(user, passwd, ""), configForNeo4j40)
	if err != nil {
		return nil, nil, err
	}

	// For multidatabase support, set sessionConfig.DatabaseName to requested database
	sessionConfig := neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}
	session, err := driver.NewSession(sessionConfig)

	if err != nil {
		return nil, nil, err
	}


	return &session, &driver, nil

}

func write_node (session neo4j.Session, node []string ) error {
	result, err := session.Run("CREATE (n:Item { id: $id, name: $name }) RETURN n.id, n.name", map[string]interface{}{
		"id":   1,
		"name": "Item 1",
	})

	if err != nil {
		return err
	}

	return result.Err()

}

func delete_all_nodes(session neo4j.Session) error {

	result, err := session.Run("MATCH (n) DELETE n", map[string]interface{}{})

	if err != nil {
		return err
	}

	return result.Err()
}

func delete_all(session neo4j.Session) error {

	result, err := session.Run("MATCH (n) DETACH DELETE n", map[string]interface{}{})

	if err != nil {
		return err
	}

	return result.Err()
}

