package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

type dbCreator struct {
	daemonURL string
}

type BucketInfo struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

func (d *dbCreator) Init() {
	d.daemonURL = daemonURLs[0] // pick first one since it always exists
}

func (d *dbCreator) DBExists(dbName string) bool {
	dbs, err := d.listDatabases()
	if err != nil {
		log.Fatal(err)
	}

	// check if the database exists
	for _, db := range dbs {
		if db.Name == dbName {
			return true
		}
	}
	return false
}

func (d *dbCreator) listDatabases() ([]BucketInfo, error) {

	client := &http.Client{}

	u := fmt.Sprintf("%s/api/v2/buckets", d.daemonURL)
	showDatabaseReq, showDatabaseErr := http.NewRequest("GET", u, nil)
	if showDatabaseErr != nil {
		return nil, fmt.Errorf("listDatabases error adding authentication token: %s", showDatabaseErr.Error())
	}

	// set authentication token
	showDatabaseReq.Header.Set("Authorization", "Token "+token)

	resp, err := client.Do(showDatabaseReq)
	if err != nil {
		return nil, fmt.Errorf("listDatabases error: %s", err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Do ad-hoc parsing to find existing buckets:
	// {
	//   "buckets": [
	//     {
	//       "createdAt": "2022-03-15T17:22:33.72617939Z",
	//       "description": "System bucket for monitoring logs",
	//       "id": "77ca9dace40a9bfc",
	//       "labels": [],
	//       "links": {
	//         "labels": "/api/v2/buckets/77ca9dace40a9bfc/labels",
	//         "members": "/api/v2/buckets/77ca9dace40a9bfc/members",
	//         "org": "/api/v2/orgs/INFLUX_ORG_ID",
	//         "owners": "/api/v2/buckets/77ca9dace40a9bfc/owners",
	//         "self": "/api/v2/buckets/77ca9dace40a9bfc",
	//         "write": "/api/v2/write?org=ORG_ID&bucket=77ca9dace40a9bfc"
	//       },
	//       "name": "_monitoring",
	//       "orgID": "INFLUX_ORG_ID",
	//       "retentionRules": [
	//         {
	//           "everySeconds": 604800,
	//           "type": "expire"
	//         }
	//       ],
	//       "schemaType": "implicit",
	//       "type": "system",
	//       "updatedAt": "2022-03-15T17:22:33.726179487Z"
	//     }
	//   ],
	//   "links": {
	//     "self": "/api/v2/buckets?descending=false&limit=20&name=_monitoring&offset=0&orgID=ORG_ID"
	//   }
	// }

	type listingType struct {
		Buckets []struct {
			Name           string `json:"name"`
			ID             string `json:"id"`
			OrgID          string `json:"orgID"`
			Type           string `json:"type"`
			CreatedAt      string `json:"createdAt"`
			UpdatedAt      string `json:"updatedAt"`
			RetentionRules []struct {
				EverySeconds int    `json:"everySeconds"`
				Type         string `json:"type"`
			} `json:"retentionRules"`
			Description string `json:"description"`
			Labels      []struct {
				ID         string `json:"id"`
				Name       string `json:"name"`
				OrgID      string `json:"orgID"`
				Type       string `json:"type"`
				CreatedAt  string `json:"createdAt"`
				UpdatedAt  string `json:"updatedAt"`
				Properties struct {
					Description string `json:"description"`
					Name        string `json:"name"`
				} `json:"properties"`
				Links struct {
					Self    string `json:"self"`
					Members string `json:"members"`
					Owners  string `json:"owners"`
					Labels  string `json:"labels"`
					Org     string `json:"org"`
					Buckets string `json:"buckets"`
				} `json:"links"`
			} `json:"labels"`
			Links struct {
				Self    string `json:"self"`
				Members string `json:"members"`
				Owners  string `json:"owners"`
				Labels  string `json:"labels"`
				Org     string `json:"org"`
				Buckets string `json:"buckets"`
				Write   string `json:"write"`
			} `json:"links"`
		} `json:"buckets"`
		Links struct {
			Self string `json:"self"`
		} `json:"links"`
	}

	// unmarshal the JSON response into the struct
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, fmt.Errorf("listDatabases error unmarshalling JSON: %s", err.Error())
	}

	ret := []BucketInfo{}
	for _, bucket := range listing.Buckets {
		// if the bucket is not a system bucket, add it to the list
		if bucket.Type != "system" {
			bucketInfo := BucketInfo{
				Name: bucket.Name,
				ID:   bucket.ID,
			}
			ret = append(ret, bucketInfo)
		}
	}

	return ret, nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {

	// check if the database exists
	dbs, err := d.listDatabases()
	if err != nil {
		log.Fatal(err)
	}

	bucketId := ""

	for _, db := range dbs {
		if db.Name == dbName {
			bucketId = db.ID
			break
		}
	}

	if bucketId == "" {
		return nil
	}

	u := fmt.Sprintf("%s/api/v2/buckets/%s", d.daemonURL, bucketId)
	req, err := http.NewRequest("DELETE", u, nil)
	if err != nil {
		return fmt.Errorf("drop db error adding authentication token: %s", err.Error()) // TODO verify this is the right error
	}

	// set authentication token
	req.Header.Set("Authorization", "Token "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("drop db error: %s", err.Error())
	}
	// return code 204 means success
	if resp.StatusCode != 204 {
		return fmt.Errorf("drop db returned non-204 code: %d", resp.StatusCode)
	}
	time.Sleep(time.Second)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {

	u := fmt.Sprintf("%s/api/v2/buckets", d.daemonURL)

	req, err := http.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}
	// set the content type to JSON
	req.Header.Set("Content-Type", "application/json")
	// set the request body
	reqBody := fmt.Sprintf(`{"name":"%s","orgID":"%s","type":"user","retentionRules":[],"description":"tsbs load test"}`, dbName, org)
	// set authorization token
	req.Header.Add("Authorization", "Token "+token)
	req.Header.Set("Accept", "application/json")

	// set the request body
	req.Body = io.NopCloser(strings.NewReader(reqBody))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	// does the body need to be read into the void?

	// return code 201 means success
	if resp.StatusCode != 201 {
		return fmt.Errorf("bad db create")
	}

	time.Sleep(time.Second)
	return nil
}
