package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
)

type dbCreator struct {
	daemonURL string
}

func (d *dbCreator) Init() {
	d.daemonURL = daemonURLs[0] // pick first one since it always exists
}

func (d *dbCreator) DBExists(dbName string) bool {
	dbs, err := d.listDatabases()
	if err != nil {
		log.Fatal(err)
	}

	for _, db := range dbs {
		if db == loader.DatabaseName() {
			return true
		}
	}
	return false
}

func (d *dbCreator) listDatabases() ([]string, error) {
	client := http.Client{}
	u := fmt.Sprintf("%s/query?q=show%%20databases", d.daemonURL)
	req, err := http.NewRequest("GET", u, nil)
	if authToken != "" {
		req.Header = http.Header{
			headerAuthorization: []string{fmt.Sprintf("Token %s", authToken)},
		}
	}
	resp, err := client.Do(req)

	if err != nil {
		return nil, fmt.Errorf("listDatabases error: %s", err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Do ad-hoc parsing to find existing database names:
	// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]}%
	type listingType struct {
		Results []struct {
			Series []struct {
				Values [][]string
			}
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	if len(listing.Results) > 0 {
		for _, nestedName := range listing.Results[0].Series[0].Values {
			name := nestedName[0]
			// the _internal database is skipped:
			if name == "_internal" {
				continue
			}
			ret = append(ret, name)
		}
	}
	return ret, nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	u := fmt.Sprintf("%s/query?q=drop+database+%s", d.daemonURL, dbName)
	client := http.Client{}
	req, err := http.NewRequest("POST", u, nil)
	if authToken != "" {
		req.Header = http.Header{
			"Content-Type":      []string{"text/plain"},
			headerAuthorization: []string{fmt.Sprintf("Token %s", authToken)},
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("drop db error: %s", err.Error())
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("drop db returned non-200 code: %d", resp.StatusCode)
	}
	time.Sleep(time.Second)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	u, err := url.Parse(d.daemonURL)
	if err != nil {
		return err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("consistency", "all")
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s WITH REPLICATION %d", dbName, replicationFactor))
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if authToken != "" {
		req.Header = http.Header{
			headerAuthorization: []string{fmt.Sprintf("Token %s", authToken)},
		}
	}
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad db create")
	}

	time.Sleep(time.Second)
	return nil
}
