// tsbs_load_zigtsdb loads a zigtsdb node with data from stdin or a file.
//
// The data file is in InfluxDB line-protocol format (`--format influx` or
// `--format zigtsdb` produce identical bytes); each multi-field line is
// expanded into one zigtsdb line per field at load time.
package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/zigtsdb"
)

func initProgramOptions() (*zigtsdb.SpecificConfig, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := zigtsdb.NewTarget()

	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	urls := viper.GetString("urls")
	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	conf := &zigtsdb.SpecificConfig{
		ServerURLs:   strings.Split(urls, ","),
		IngestPath:   viper.GetString("ingest-path"),
		MaxBodyBytes: viper.GetInt("max-body-bytes"),
		MaxRetries:   viper.GetInt("max-retries"),
	}
	// Every point of a batch is a full line; hashing workers by host is
	// unnecessary because zigtsdb derives series identity from the line itself.
	loaderConf.HashWorkers = false
	loader := load.GetBenchmarkRunner(loaderConf)
	return conf, loader, &loaderConf
}

func main() {
	conf, loader, loaderConf := initProgramOptions()

	benchmark, err := zigtsdb.NewBenchmark(conf, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
