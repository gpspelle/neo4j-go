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

func process_array_string(x string) []string {

	x = strings.Replace(x, "[", "", -1)
	x = strings.Replace(x, "]", "", -1)
	x = strings.Replace(x, "'", "", -1)
	x = strings.Replace(x, " ", "", -1)

	return strings.Split(x, ",")
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
	url_map := make(map[string]bool)

	_, err = r.Read()

	if err == io.EOF {
		log.Fatalln("Empty csv", err)
	}

	if err != nil {
		log.Fatal(err)
	}

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
			err = add_user_node(sess, user_id, false)

			if err != nil {
				log.Fatalln("Failed to write user in the database", err)
			}
		}


		hashtag_string := record[2]
		hashtags := process_array_string(hashtag_string)

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

			err = add_userhashtag_relation(sess, user_id, hashtag)

			if err != nil {
				log.Fatalln("Failed to write user-hashtag relation in the database", err)
			}

			err = add_tweethashtag_relation(sess, tweet_id, hashtag)

			if err != nil {
				log.Fatalln("Failed to write tweet-hashtag relation in the database", err)
			}

		}

		url_string := record[3]
		urls := process_array_string(url_string)

		for _, url := range urls {

			if len(url) == 0 {
				continue;
			}
			_, found = url_map[url]  // found == true

			// Only add never added nodes
			if !found {
				url_map[url] = true
				err = add_url_node(sess, url)

				if err != nil {
					log.Fatalln("Failed to write url in the database", err)
				}

			}
		}


		mention_string := record[4]
		mentions := process_array_string(mention_string)

		for _, mention := range mentions {
			_, found := user_map[mention]  // found == true

			if len(mention) == 0 {
				continue;
			}

			// Only add never added nodes
			if !found {
				user_map[mention] = true
				err = add_user_node(sess, mention, true)

				if err != nil {
					log.Fatalln("Failed to write user in the database", err)
				}
			} else {
				// if the user was added and mentioned need to change its attribute
				err = set_mention(sess, user_id)

				if err != nil {
					log.Fatalln("Failed to set mention in the database", err)
				}
			}

			err = add_useruser_relation(sess, user_id, mention)

			if err != nil {
				log.Fatalln("Failed to write user-user relation in the database", err)
			}
		}

		for _, url := range urls {
			if len(url) == 0 {
				continue;
			}
			for _, hashtag := range hashtags {
				if len(hashtag) == 0 {
					continue;
				}
				err = add_urlhashtag_relation(sess, url, hashtag)

				if err != nil {
					log.Fatalln("Failed to write url-hashtag relation in the database", err)
				}
			}

		}

		for _, hashtag := range hashtags {
			if len(hashtag) == 0 {
				continue;
			}
			for _, url := range urls {
				if len(url) == 0 {
					continue;
				}
				err = add_hashtagurl_relation(sess, hashtag, url)

				if err != nil {
					log.Fatalln("Failed to write hashtag-url relation in the database", err)
				}
			}
		}

		err = add_tweetuser_relation(sess, tweet_id, user_id)

		if err != nil {
			log.Fatalln("Failed to write tweet-user relation in the database", err)
		}

		err = add_usertweet_relation(sess, user_id, tweet_id)

		if err != nil {
			log.Fatalln("Failed to write user-tweet relation in the database", err)
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

func add_url_node (session neo4j.Session, url string) error {


	attributes := map[string]interface{} {"url": url}
	result, err := session.Run("CREATE (n:Url { url: $url })", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_user_node (session neo4j.Session, user_id string, mention bool) error {


	attributes := map[string]interface{} {"user_id": user_id, "mention": mention}
	result, err := session.Run("CREATE (n:User { user_id: $user_id, mention: $mention })", attributes)

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

func add_userhashtag_relation(session neo4j.Session, user_id string, hashtag string) error {

	attributes := map[string]interface{} {"user_id": user_id, "hashtag": hashtag}
	result, err := session.Run("MATCH (a:User),(b:Hashtag) WHERE a.user_id = $user_id AND b.hashtag = $hashtag CREATE (a)-[r:has_interest]->(b)", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_useruser_relation(session neo4j.Session, user_id_0 string, user_id_1 string) error {

	attributes := map[string]interface{} {"user_id_0": user_id_0, "user_id_1": user_id_1}
	result, err := session.Run("MATCH (a:User),(b:User) WHERE a.user_id = $user_id_0 AND b.user_id = $user_id_1 CREATE (a)-[r:mention]->(b)", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_urlhashtag_relation(session neo4j.Session, url string, hashtag string) error {

	attributes := map[string]interface{} {"url": url, "hashtag": hashtag}
	result, err := session.Run("MATCH (a:Url),(b:Hashtag) WHERE a.url = $url AND b.hashtag = $hashtag CREATE (a)-[r:relates]->(b)", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_hashtagurl_relation(session neo4j.Session, hashtag string, url string) error {

	attributes := map[string]interface{} {"hashtag": hashtag, "url": url}
	result, err := session.Run("MATCH (a:Hashtag),(b:Url) WHERE a.hashtag = $hashtag AND b.url = $url CREATE (a)-[r:relates]->(b)", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_usertweet_relation(session neo4j.Session, user_id string, tweet_id string) error {


	attributes := map[string]interface{} {"user_id": user_id, "tweet_id": tweet_id}
	result, err := session.Run("MATCH (a:User),(b:Tweet) WHERE a.user_id = $user_id AND b.tweet_id = $tweet_id CREATE (a)-[r:tweet]->(b)", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_tweethashtag_relation(session neo4j.Session, tweet_id string, hashtag string) error {


	attributes := map[string]interface{} {"tweet_id": tweet_id, "hashtag": hashtag}
	result, err := session.Run("MATCH (a:Tweet),(b:Hashtag) WHERE a.tweet_id = $tweet_id AND b.hashtag = $hashtag CREATE (a)-[r:contain]->(b)", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_tweetuser_relation(session neo4j.Session, tweet_id string, user_id string) error {

	attributes := map[string]interface{} {"tweet_id": tweet_id, "user_id": user_id}
	result, err := session.Run("MATCH (a:Tweet),(b:User) WHERE a.tweet_id = $tweet_id AND b.user_id = $user_id CREATE (a)-[r:mention]->(b)", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func set_mention(session neo4j.Session, user_id string) error {

	attributes := map[string]interface{} {"user_id": user_id}
	result, err := session.Run("MATCH (n { user_id: $user_id }) SET n.mention = true", attributes)

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

