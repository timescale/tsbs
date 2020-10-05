package main

import (
	"bufio"
	"log"
	"strings"
	"sync"
)

type dbCreator struct {
	br     *bufio.Reader
	db     string
	tags   map[string]columnSchema
	tables map[string]tableSchema
}

type tableSchema struct {
	name string
	cols []columnSchema
}

type columnSchema struct {
	name    string
	colType string
}

func (d *dbCreator) Init() {
	d.tags = map[string]columnSchema{}
	d.tables = map[string]tableSchema{}

	d.readDataHeader(d.br)
}

func (d *dbCreator) readDataHeader(br *bufio.Reader) {
	// First N lines are header, with the first line containing the tags
	// and their names, the second through N-1 line containing the column
	// names, and last line being blank to separate from the data
	i := 0
	for {
		if i == 0 {
			tagString, err := br.ReadString('\n')
			if err != nil {
				log.Fatalf("input has wrong header format: %v", err)
			}
			// Convert the tag types into columns
			tagString = strings.TrimSpace(tagString)
			tags := strings.Split(tagString, ",")
			for _, t := range tags[1:] {
				nt := strings.Split(t, " ")
				d.tags[nt[0]] = columnSchema{
					name:    nt[0],
					colType: serializedTypeToHyprcubdType(nt[1]),
				}
			}

		} else {

			line, err := br.ReadString('\n')
			if err != nil {
				log.Fatalf("input has wrong header format: %v", err)
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				break
			}
			parts := strings.Split(line, ",")

			table := tableSchema{
				name: parts[0],
				cols: []columnSchema{},
			}

			for _, colName := range parts[1:] {
				table.cols = append(table.cols, columnSchema{
					name:    colName,
					colType: "FLOAT",
				})
			}

			d.tables[parts[0]] = table
		}
		i++
	}
}

func (d *dbCreator) DBExists(dbName string) bool {
	dbs, err := showDatabases()
	if err != nil {
		panic(err)
	}
	for _, db := range dbs {
		if db == dbName {
			return true
		}
	}
	return false
}

func (d *dbCreator) CreateDB(dbName string) error {
	d.db = dbName
	_, err := runQuery(QueryRequest{
		Query: "create database " + dbName,
	})
	return err
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	_, err := runQuery(QueryRequest{
		Query: "drop database " + dbName,
	})
	if err != nil {
		log.Println(err)
	}
	return err
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	var wg sync.WaitGroup

	for _, t := range d.tables {
		var stmt strings.Builder
		stmt.WriteString("create table " + t.name + " (time time, ")

		if len(d.tags) > 0 {
			stmt.WriteString("tags JSON")
		}

		for _, c := range t.cols {
			stmt.WriteString(", " + c.name + " " + c.colType)
		}

		stmt.WriteString(")")
		log.Println(stmt.String())

		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			_, err := runQuery(QueryRequest{
				Database: dbName,
				Query:    stmt.String(),
			})
			if err != nil {
				log.Println(stmt.String())
			}
		}(stmt.String())
	}

	log.Println("Waiting for tables to be created...")
	wg.Wait()

	return nil
}
