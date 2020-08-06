package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
	"time"
	"strconv"
	"reflect"
)

func convert_month(month int) string {

	if month == 1 {
		return "January"
	} else if month == 2 {
		return "February"
	} else if month == 3 {
		return "March"
	} else if month == 4 {
		return "April"
	} else if month == 5 {
		return "May"
	} else if month == 6 {
		return "June"
	} else if month == 7 {
		return "July"
	} else if month == 8 {
		return "August"
	} else if month == 9 {
		return "September"
	} else if month == 10 {
		return "October"
	} else if month == 11 {
		return "November"
	} else {
		return "December"
	}
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

func add_node_multi_attributes (session neo4j.Session, node_type string, node_att []string, node_att_value []interface{}) error {

	line := " { "
	for ind, att := range node_att {

		att_str := fmt.Sprintf("%v", node_att_value[ind])
		data_type := reflect.TypeOf(node_att_value[ind]).Kind()

		if data_type == reflect.String {
			line = line + att + ": " + "'"+att_str+"'" + ", "
		} else {
			line = line + att + ": " + att_str + ", "
		}
	}

	line = line[:len(line)-2]
	line = line + " })"
	result, err := session.Run("CREATE (n:" + node_type + line, nil)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_node(session neo4j.Session, node_type string, node_att string, node_att_value string) error {

	result, err := session.Run("CREATE (n:" + node_type + " { " + node_att + ": " + node_att_value + " })", nil)

	if err != nil {
		return err
	}

	return result.Err()
}

func add_relation(session neo4j.Session, left_type string, left_att string, left_att_value string, right_type string, right_att string, right_att_value string, relation_name string) error {

	result, err := session.Run("MATCH (a:" + left_type + "),(b:" + right_type + ") WHERE a." + left_att + " = " + left_att_value + " AND b." + right_att + " = " + right_att_value + " CREATE (a)-[r:" + relation_name + "]->(b)", nil)

	if err != nil {
		return err
	}

	return result.Err()
}

func set_attribute(session neo4j.Session, node_id string, node_id_value string, node_att string, node_att_value string) error {

	attributes := map[string]interface{} {"node_id_value": node_id_value, "node_att_value" : node_att_value}
	result, err := session.Run("MATCH (n { " + node_id + ": $node_id_value }) SET n." + node_att + " = $node_att_value", attributes)

	if err != nil {
		return err
	}

	return result.Err()
}

func delete_all(session neo4j.Session) error {

	result, err := session.Run("MATCH (n) DETACH DELETE n", nil)

	if err != nil {
		return err
	}

	return result.Err()
}

func execute_query(session neo4j.Session, query string) error {

	result, err := session.Run(query, nil)

	if err != nil {
		return err
	}

	return result.Err()
}

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


	start := time.Now()
	r := read_csv("clean_hashtag_data.csv")
	session, driver, err := open_connection("neo4j", "neo4j")

	if err != nil {
		log.Fatalln("Failed to connect to neo4j", err)
	}

	sess := *session
	dri := *driver

	hashtag_map := make(map[string]bool)
	user_map := make(map[string]bool)
	url_map := make(map[string]bool)

	_, err = r.Read() // skip header

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

		timestamp := record[5]
		i, err := strconv.ParseInt(timestamp, 10, 64)

		if err != nil {
			log.Fatal(err)
		}

		tm := time.Unix(i, 0)
		day := tm.Day()
		month := tm.Month()
		month_int := int(month)
		month_str := convert_month(month_int)
		year := tm.Year()
		hour := tm.Hour()
		minute := tm.Minute()
		second := tm.Second()

		tweet_id := record[0]

		tweet_id_int, err := strconv.ParseInt(tweet_id, 10, 64)

		if err != nil {
			log.Fatalln("Failed to convert tweet_id to int")
		}

		att_type := []string {"tweet_id", "day", "month", "year", "hour", "minute", "second"}
		att_val := []interface{} {tweet_id_int, day, month_str, year, hour, minute, second}

		err = add_node_multi_attributes(sess, "Tweet", att_type, att_val)

		if err != nil {
			log.Fatalln("Failed to write tweet in the database", err)
		}

		user_id := record[1]
		_, found := user_map[user_id]  // found == true

		// Only add never added nodes
		if !found {
			user_map[user_id] = true
			att_type := []string {"user_id", "mentioned"}
			att_vals := []interface{} {user_id, "false"}
			err = add_node_multi_attributes(sess, "User", att_type, att_vals)

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
				err = add_node(sess, "Hashtag", "hashtag", "'"+hashtag+"'")

				if err != nil {
					log.Fatalln("Failed to write hashtag in the database", err)
				}

			}

			err = add_relation(sess, "Tweet", "tweet_id", tweet_id, "Hashtag", "hashtag", "'"+hashtag+"'", "contains")

			if err != nil {
				log.Fatalln("Failed to write tweet-hashtag relation in the database", err)
			}

			err = add_relation(sess, "Hashtag", "hashtag", "'"+hashtag+"'", "Tweet", "tweet_id", tweet_id, "contained")

			if err != nil {
				log.Fatalln("Failed to write hashtag-tweet relation in the database", err)
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
				err = add_node(sess, "Url", "url", "'"+url+"'")

				if err != nil {
					log.Fatalln("Failed to write url in the database", err)
				}

			}

			err = add_relation(sess, "Tweet", "tweet_id", tweet_id, "Url", "url", "'"+url+"'", "contains")

			if err != nil {
				log.Fatalln("Failed to write tweet-url relation in the database", err)
			}

			err = add_relation(sess, "Url", "url", "'"+url+"'", "Tweet", "tweet_id", tweet_id, "contained")

			if err != nil {
				log.Fatalln("Failed to write url-tweet relation in the database", err)
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
				att_type_ := []string {"user_id", "mentioned"}
				att_vals_ := []interface{} {user_id, "true"}
				err = add_node_multi_attributes(sess, "User", att_type_, att_vals_)

				if err != nil {
					log.Fatalln("Failed to write user in the database", err)
				}
			} else {
				// if the user was added and mentioned need to change its attribute
				err = set_attribute(sess, "user_id", mention, "mentioned", "true")

				if err != nil {
					log.Fatalln("Failed to set mention in the database", err)
				}
			}

			err = add_relation(sess, "Tweet", "tweet_id", tweet_id, "User", "user_id", mention, "mentions")

			if err != nil {
				log.Fatalln("Failed to write tweet-user relation in the database", err)
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
				err = add_relation(sess, "Url", "url", "'"+url+"'", "Hashtag", "hashtag", "'"+hashtag+"'", "relates")

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
				err = add_relation(sess, "Hashtag", "hashtag", "'"+hashtag+"'", "Url", "url", "'"+url+"'", "relates")

				if err != nil {
					log.Fatalln("Failed to write hashtag-url relation in the database", err)
				}
			}
		}

		err = add_relation(sess, "User", "user_id", user_id, "Tweet", "tweet_id", tweet_id, "tweets")

		if err != nil {
			log.Fatalln("Failed to write user-tweet relation in the database", err)
		}

		err = add_relation(sess, "Tweet", "tweet_id", tweet_id, "User", "user_id", user_id, "tweeted")

		if err != nil {
			log.Fatalln("Failed to write tweet-user relation in the database", err)
		}

		if index % 500 == 0 {
			fmt.Printf("Created %d tweet nodes\n", index)
		}
	}

	fmt.Printf("Created %d nodes\n", index)
	fmt.Println("Removing duplicate relations and updating some stats")

	err = execute_query(sess, "MATCH (A)-[r:relates]->(B) WITH A, COLLECT(r) as oldRels, B, COUNT(r) as W FOREACH(r IN oldRels | DELETE r) WITH A, W, B CREATE (A)-[O:relates {occurrences:W}]->(B)")

	if err != nil {
		log.Fatalln("Failed to merge relates relation", err)
	}


	err = execute_query(sess, "MATCH (A:Tweet)-[r:contains]->(B:Hashtag) WITH B, COUNT(r) as W SET B.presence = W")

	if err != nil {
		log.Fatalln("Failed to set Hashtag presence", err)
	}

	err = execute_query(sess, "MATCH (A:Tweet)-[r:mentions]->(B:User) WITH B, COUNT(r) as W SET B.popularity = W")

	if err != nil {
		log.Fatalln("Failed to set User popularity", err)
	}

	err = execute_query(sess, "MATCH (A:Tweet)-[r:contains]->(B:Url) WITH B, COUNT(r) as W SET B.popularity = W")

	if err != nil {
		log.Fatalln("Failed to set Url popularity", err)
	}


	sess.Close()
	dri.Close()

	elapsed := time.Since(start)
	fmt.Printf("Time elapsed: %s\n", elapsed)

}

