package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gocolly/colly"
	neo4j "github.com/neo4j/neo4j-go-driver/neo4j"
)

func getConnection(driver neo4j.Driver) neo4j.Session {
	// conn, err := pool.OpenPool()
	// if err != nil {
	// 	log.Panic("Unable to open a connection to storage")
	// }
	// return conn
	session, _ := driver.Session(neo4j.AccessModeWrite)
	//defer session.Close()
	return session
}

func connect(sourceURL string, targetURL string, depth int, driver neo4j.Driver) {
	sess := getConnection(driver)
	defer sess.Close()

	_, err := sess.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(
			"MATCH (source:Page {url: $sourceUrl}) MERGE (target:Page {url: $targetUrl}) MERGE (source)-[r:LINK]->(target) return r",
			map[string]interface{}{
				"sourceUrl": sourceURL,
				"targetUrl": targetURL,
				"depth":     depth,
			})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return result.Record().GetByIndex(0), nil
		}

		return nil, result.Err()
	})

	if err != nil {
		log.Panic("Failed to create link data")
	}
}

func merge(absoluteURL string, depth int, driver neo4j.Driver) {
	sess := getConnection(driver)
	defer sess.Close()

	_, err := sess.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(
			"MERGE (s:Page {url: $url}) return s",
			map[string]interface{}{
				"url":   absoluteURL,
				"depth": depth,
			})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return result.Record().GetByIndex(0), nil
		}

		return nil, result.Err()
	})

	// result, err := sess.Run(`
	// MERGE (s:Page {url: {url}})
	// return s`,
	// 	map[string]interface{}{
	// 		"url":   absoluteURL,
	// 		"depth": depth,
	// 	})
	if err != nil {
		fmt.Printf("Cek Error : %v", err)
		log.Panic("Failed to merge page")
	}
	//fmt.Printf("Created Data from Page : %v \n", result)
	// var record *neo4j.Record
	// for result.NextRecord(&record) {
	// 	fmt.Printf("Created Data from Page : %s \n", record.Values[0].(string))
	// 	fmt.Printf("Created Data from Page : %s \n", record.Values[0].(string))
	// }
}

func main() {

	driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "admin", "system"), func(c *neo4j.Config) {
		c.Encrypted = false
	})

	//driver, err := neo.NewClosableDriverPool("bolt://neo4j:admin@localhost:7687", 20)

	if err != nil {
		log.Panic("Unable to establish connection to neo4j")
	}
	defer driver.Close()

	c := colly.NewCollector(
		colly.AllowedDomains("scrum.org"),
		colly.MaxDepth(2),
		colly.Async(true),
	)
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 1,
		RandomDelay: 5 * time.Second,
	})
	//buat ambil atribut href di website
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		targetLink := e.Request.AbsoluteURL(e.Attr("a[href]"))
		connect(e.Request.URL.String(), targetLink, e.Request.Depth, driver)
		c.Visit(targetLink)
	})

	c.OnResponse(func(r *colly.Response) {
		merge(r.Request.URL.String(), r.Request.Depth, driver)
		fmt.Printf("Just got response for path %s\n", r.Request.URL.EscapedPath())
	})

	c.Visit("https://scrum.org/")
	c.Wait()

}
