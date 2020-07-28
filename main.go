package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
)

func process_hashtag_string(hashtag string) []string {

	hashtag = strings.Replace(hashtag, "[", "", -1)
	hashtag = strings.Replace(hashtag, "]", "", -1)
	hashtag = strings.Replace(hashtag, "'", "", -1)
	hashtag = strings.Replace(hashtag, " ", "", -1)

	return strings.Split(hashtag, ",")
}

func read_csv(filename string) *csv.Reader {

	// Open the file
	csvfile, err := os.Open(filename)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)

	fmt.Printf("%T\n", r)

	r.FieldsPerRecord = -1

	return r
}

func main() {


	r := read_csv("tweet_data.csv")
	session, driver, err := open_connection("neo4j", "neo4j")

	if err != nil {
		log.Fatalln("Failed to connect to neo4j", err)
	}

	sess := *session
	dri := *driver

	hashtag_map := make(map[string]bool)
	user_map := make(map[string]bool)

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

		tweet_id := record[0]
		err = add_tweet_node(sess, tweet_id)

		if err != nil {
			log.Fatalln("Failed to write tweet in the database", err)
		}

		user_id := record[1]
		_, found := user_map[user_id]  // found == true

		// Only add never added nodes
		if !found {
			user_map[user_id] = true
			err = add_user_node(sess, user_id)

			if err != nil {
				log.Fatalln("Failed to write user in the database", err)
			}
		}


		hashtag_string := record[2]
		hashtags := process_hashtag_string(hashtag_string)

		for _, hashtag := range hashtags {

			if len(hashtag) == 0 {
				continue;
			}
			_, found = hashtag_map[hashtag]  // found == true

			// Only add never added nodes
			if !found {
				hashtag_map[hashtag] = true
				err = add_hashtag_node(sess, hashtag)

				if err != nil {
					log.Fatalln("Failed to write hashtag in the database", err)
				}

			}

			err = add_hashuser_relation(sess, user_id, hashtag)

			if err != nil {
				log.Fatalln("Failed to write user-hashtag relation in the database", err)
			}

		}

		if index % 500 == 0 {
			fmt.Printf("Created %d tweet nodes\n", index)
		}
	}

	fmt.Printf("Created %d nodes\n", index)


	/*err = delete_all(sess)

	if err != nil {
		log.Fatalln("Failed to delete nodes", err)
	}
	*/

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

func add_tweet_node (session neo4j.Session, tweet_id string) error {


	attributes := map[string]interface{} {"tweet_id": tweet_id}
	result, err := session.Run("CREATE (n:Tweet { tweet_id: $tweet_id })", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_user_node (session neo4j.Session, user_id string) error {


	attributes := map[string]interface{} {"user_id": user_id}
	result, err := session.Run("CREATE (n:User { user_id: $user_id })", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_hashtag_node (session neo4j.Session, hashtag string) error {

	attributes := map[string]interface{} {"hashtag": hashtag}
	result, err := session.Run("CREATE (n:Hashtag { hashtag: $hashtag })", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_hashuser_relation(session neo4j.Session, user_id string, hashtag string) error {

	attributes := map[string]interface{} {"user_id": user_id, "hashtag": hashtag}
	result, err := session.Run("MATCH (a:User),(b:Hashtag) WHERE a.user_id = $user_id AND b.hashtag = $hashtag CREATE (a)-[r:POST]->(b)", attributes)

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

