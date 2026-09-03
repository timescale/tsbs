package zigtsdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// tsbsMetricPrefixes are the measurement names TSBS's devops/cpu-only use
// cases produce; after expansion every zigtsdb metric starts with one of them.
// RemoveOldDB never touches metrics outside this set.
var tsbsMetricPrefixes = []string{
	"cpu_", "mem_", "disk_", "diskio_", "kernel_", "net_", "nginx_", "postgresl_", "redis_",
}

// dbCreator talks to one node. zigtsdb has no database abstraction: "the
// database exists" means TSBS metrics are already present.
type dbCreator struct {
	url    string
	client *http.Client
}

func (d *dbCreator) Init() {
	d.url = strings.TrimSuffix(d.url, "/")
	// No keep-alive: zigtsdb serves one connection per HTTP thread, and an
	// idle creator connection would hold a thread the workers need.
	d.client = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := d.client.Get(d.url + "/health")
	if err != nil {
		fatal("zigtsdb health check failed: %s", err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fatal("zigtsdb health check returned HTTP %d", resp.StatusCode)
	}
}

func (d *dbCreator) DBExists(_ string) bool {
	metrics, err := d.listTSBSMetrics()
	if err != nil {
		fatal("SHOW METRICS failed: %s", err)
		return false
	}
	return len(metrics) > 0
}

func (d *dbCreator) RemoveOldDB(_ string) error {
	metrics, err := d.listTSBSMetrics()
	if err != nil {
		return err
	}
	for _, m := range metrics {
		req, err := http.NewRequest(http.MethodDelete, d.url+"/query?metric="+url.QueryEscape(m), nil)
		if err != nil {
			return err
		}
		resp, err := d.client.Do(req)
		if err != nil {
			return err
		}
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("DELETE metric %s returned HTTP %d: %s", m, resp.StatusCode, body)
		}
	}
	return nil
}

func (d *dbCreator) CreateDB(_ string) error {
	return nil
}

type showMetricsResponse struct {
	Status  string   `json:"status"`
	Metrics []string `json:"metrics"`
	Error   string   `json:"error"`
}

func (d *dbCreator) listTSBSMetrics() ([]string, error) {
	resp, err := d.client.Get(d.url + "/query?zql=" + url.QueryEscape("SHOW METRICS"))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("SHOW METRICS returned HTTP %d: %s", resp.StatusCode, body)
	}
	var r showMetricsResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("SHOW METRICS: invalid JSON %q: %s", body, err)
	}
	if r.Error != "" {
		return nil, fmt.Errorf("SHOW METRICS: %s", r.Error)
	}
	var out []string
	for _, m := range r.Metrics {
		if hasTSBSPrefix(m) {
			out = append(out, m)
		}
	}
	return out, nil
}

func hasTSBSPrefix(metric string) bool {
	for _, p := range tsbsMetricPrefixes {
		if strings.HasPrefix(metric, p) {
			return true
		}
	}
	return false
}
