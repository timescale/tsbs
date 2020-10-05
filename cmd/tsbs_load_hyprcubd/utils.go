package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func serializedTypeToHyprcubdType(serializedType string) string {
	switch serializedType {
	case "string":
		return "STRING"
	case "float32":
		return "FLOAT"
	case "float64":
		return "FLOAT"
	case "int64":
		return "INT"
	case "int32":
		return "INT"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}

type QueryRequest struct {
	Database string `json:"db"`
	Query    string `json:"query"`
}

type QueryResponse struct {
	RowsAffected int             `json:"rows_affected,omitempty"`
	Result       [][]interface{} `json:"result,omitempty"`
	Duration     int64           `json:"duration"`
	Error        string          `json:"error"`
}

type ShowDatabasesResponse struct {
	Databases []string `json:"databases"`
	Error     string   `json:"error"`
}

func runQuery(qr QueryRequest) (*QueryResponse, error) {
	data, err := json.Marshal(&qr)
	if err != nil {
		return nil, err
	}

	tries := 0
retry:
	tries++
	if tries > 2 {
		return nil, fmt.Errorf("Too many retries")
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/query", host), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+hyprToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println(err)
		goto retry
	}
	defer resp.Body.Close()

	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		goto retry
	}

	if resp.StatusCode != 200 {
		log.Println("Got status ", resp.StatusCode)
		if resp.StatusCode == 502 {
			goto retry
		}
		log.Println(string(out))
		goto retry
		// return nil, fmt.Errorf("received %d", resp.StatusCode)
	}

	var qresp QueryResponse
	err = json.Unmarshal(out, &qresp)
	if err != nil {
		return nil, err
	}
	if len(qresp.Error) > 0 {
		return nil, fmt.Errorf(qresp.Error)
	}

	return &qresp, nil
}

func showDatabases() ([]string, error) {
	data, err := json.Marshal(&QueryRequest{
		Query: "show databases",
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/query", host), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+hyprToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	out, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		log.Println(string(out))
		return nil, fmt.Errorf("received %d from hyprcubd", resp.StatusCode)
	}

	var qresp ShowDatabasesResponse
	err = json.Unmarshal(out, &qresp)
	if err != nil {
		return nil, err
	}
	if len(qresp.Error) > 0 {
		return nil, fmt.Errorf(qresp.Error)
	}

	return qresp.Databases, nil
}
