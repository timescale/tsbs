package victoriametrics

// VictoriaMetrics don't have a database abstraction
type dbCreator struct{}

func (d *dbCreator) Init() {}

func (d *dbCreator) DBExists(dbName string) bool { return true }

func (d *dbCreator) CreateDB(dbName string) error { return nil }

func (d *dbCreator) RemoveOldDB(dbName string) error { return nil }
