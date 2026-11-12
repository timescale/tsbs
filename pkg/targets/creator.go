package targets

// DBCreator is an interface for a benchmark to do the initial setup of a database
// in preparation for running a benchmark against it.
type DBCreator interface {
	// Init should set up any connection or other setup for talking to the DB, but should NOT create any databases
	Init()

	// DBExists checks if a database with the given name currently exists.
	DBExists(dbName string) bool

	// CreateDB creates a database with the given name.
	CreateDB(dbName string) error

	// RemoveOldDB removes an existing database with the given name.
	RemoveOldDB(dbName string) error
}

// DBCreatorCloser is a DBCreator that also needs a Close method to cleanup any connections
// after the benchmark is finished.
type DBCreatorCloser interface {
	DBCreator

	// Close cleans up any database connections
	Close()
}

// DBCreatorPost is a DBCreator that also needs to do some initialization after the
// database is created (e.g., only one client should actually create the DB, so
// non-creator clients should still set themselves up for writing)
type DBCreatorPost interface {
	DBCreator

	// PostCreateDB does further initialization after the database is created
	PostCreateDB(dbName string) error
}
