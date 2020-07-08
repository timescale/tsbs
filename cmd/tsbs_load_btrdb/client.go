package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const URLTemplate = "%s%s"
const InsertURI = "/data/add/"

type btrdbClient struct {
	client  *http.Client
	baseUrl string
}

func NewBTrDBClient() *btrdbClient {
	return &btrdbClient{
		client: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		},
		baseUrl: baseUrl,
	}
}

func (c *btrdbClient) insert(insert *insertion) error {
	if insert == nil {
		return nil
	}
	URL := fmt.Sprintf(URLTemplate, c.baseUrl, InsertURI)
	data, err := json.Marshal(insert)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", URL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err = ioutil.ReadAll(resp.Body); err != nil {
		return err
	}
	return nil
}
