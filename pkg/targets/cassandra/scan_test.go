package cassandra

import (
	"testing"
)

func TestSingleMetricToInsertStatement(t *testing.T) {
	cases := []struct {
		desc                  string
		inputCSV              string
		outputInsertStatement string
	}{
		{
			desc:                  "A properly formatted CSV line should result in a properly formatted CQL INSERT statement",
			inputCSV:              "series_double,cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production,usage_guest_nice,2016-01-01,1451606400000000000,38.2431182911542820",
			outputInsertStatement: "INSERT INTO series_double(series_id, timestamp_ns, value) VALUES('cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production#usage_guest_nice#2016-01-01', 1451606400000000000, 38.2431182911542820)",
		},
		{
			desc:                  "A properly formatted CSV line with an arbitrary number of tags should result in a properly formatted CQL INSERT statement",
			inputCSV:              "series_bigint,redis,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production,port=6379,server=redis_1,used_cpu_user,2016-01-01,1451606400000000000,388",
			outputInsertStatement: "INSERT INTO series_bigint(series_id, timestamp_ns, value) VALUES('redis,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production,port=6379,server=redis_1#used_cpu_user#2016-01-01', 1451606400000000000, 388)",
		},
	}

	for _, c := range cases {
		output := singleMetricToInsertStatement(c.inputCSV)
		if output != c.outputInsertStatement {
			t.Errorf("%s \nOutput incorrect: \nWant: %s \nGot: %s", c.desc, c.outputInsertStatement, output)
		}
	}
}
