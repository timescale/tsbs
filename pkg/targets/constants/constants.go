package constants

// Formats supported for generation
const (
	FormatCassandra       = "cassandra"
	FormatClickhouse      = "clickhouse"
	FormatInflux          = "influx"
	FormatMongo           = "mongo"
	FormatSiriDB          = "siridb"
	FormatTimescaleDB     = "timescaledb"
	FormatAkumuli         = "akumuli"
	FormatCrateDB         = "cratedb"
	FormatPrometheus      = "prometheus"
	FormatVictoriaMetrics = "victoriametrics"
	FormatTimestream      = "timestream"
)

func SupportedFormats() []string {
	return []string{
		FormatCassandra,
		FormatClickhouse,
		FormatInflux,
		FormatMongo,
		FormatSiriDB,
		FormatTimescaleDB,
		FormatAkumuli,
		FormatCrateDB,
		FormatPrometheus,
		FormatVictoriaMetrics,
		FormatTimestream,
	}
}
