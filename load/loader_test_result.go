package load

const LoaderTestResultVersion = "0.1"

// LoaderTestResult aggregates the results of an insert or load benchmark in a common format across targets
type LoaderTestResult struct {
	// Format Configs
	ResultFormatVersion string `json:"ResultFormatVersion"`

	// RunnerConfig Configs
	RunnerConfig BenchmarkRunnerConfig `json:"RunnerConfig"`

	// Run info
	StartTime      int64 `json:"StartTime`
	EndTime        int64 `json:"EndTime"`
	DurationMillis int64 `json:"DurationMillis"`

	// Totals
	Totals map[string]interface{} `json:"Totals"`
}
