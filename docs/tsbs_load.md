# Supplemental Guide for `tsbs_load` 

The `tsbs_load` executable can benchmark data ingestion
for all the implemented databases.

## Generating a config file

`tsbs_load` uses YAML files to specify the configuration for 
running the load benchmark.

The config file is separated in two top-level sections:
```yaml
data-source:
  ...
loader: 
  ...
```
* `data-source` contains the configuration for where to 
read the data from (`type: SIMULATOR` or `type: FILE`)
  * For `SIMULATOR` the configuration specifies the time range to be simulated,
  the use-case, scale and other properties that regard the data
  * For `FILE` the configuration only specifies the location of the pre-generated
  file with `tsbs_generate_data`
* `loader` contains the configuration for the loading the data. Two sub-sections are
important here `db-specific` and `runner`
  * The `db-specific` configuration varies depending of the target database
  and for TimescaleDB contains information about user, password, ssl mode, while
  for influx it contains information about backoff interval, replication factor etc.
  * The `runner` configuration specifies the number of concurrent workers to use,
  batch size, hashing and so on
  
To generate an example configuration file for a specific database run
```shell script
$ tsbs_load config --target=<db-name> --data-source=[FILE|SIMULATOR]
```
specifying db-name to one of the implemented databases and data-source to
FILE or SIMULATOR

⚠️ **The generated config file will be populated with the default values for each property.**

The generated config file is saved in `./config.yaml`

### Sample config files

You can find sample YAML configuration files for TimescaleDB in the 
[sample-configs](https://github.com/timescale/tsbs/tree/master/docs/sample-configs) directory. Both single and multi-node examples are provided
for `FILE` and `SIMULATOR` modes.

## On the fly simulation and load with `data-source: SIMULATOR`

When you run `tsbs_generate_data` a simulator is created for 
the selected use case and the simulated data points are serialized
to a file. `tsbs_load` utilizes the same simulators but the 
simulated points are directly piped to the worker clients that send batches
of data to the databases. 

You can notice that the same properties you configure in the YAML file
are the same flags that you need to specify when running `tsbs_generate_data`.

You can run `tsbs_load` with 
```shell script
$ tsbs_load load <db_name> --config=./path-to-config.yaml
```
Where `<db_name>` is one of the implemented databases or you can run 
```shell script
$ tsbs_load load --help
```
for a list of the available databases.

## Information about a property and overriding

The generated yaml file with `tsbs_load config` does not contain
information about what each of the properties represents. You can easily discover
more details about each property by running:
 
```shell script
$ tsbs_load load --help
```
This will list all the available flags configurable for all databases. These flags
include the flags for `data-source` and `loader.runner`. The `--loader.runner.db-name` flag
corresponds to the property:
```yaml
loader:
  runner:
    db-name: some-db
```
in the YAML config file. With the type, description, and default 
value next to the flag name as :

```string, Name of database (default "benchmark")```

### Information about database specific flags

Some of the properties are only valid for specific databases. These 
properties go under the `loader.db-specific` section. To view information
about them you can run:
```shell script
$ tsbs_load load <db_name> --help
```

For example for timescaledb, you can see the following:
```shell script
$ tsbs_load load timescaledb --help
...
--loader.db-specific.chunk-time 
    duration
    Duration that each chunk should represent, e.g., 12h (default 12h0m0s)
...
```

### Overriding values

* Each property has a default value, used if not otherwise overridden
* An entry in the config YAML file overrides the default value
* A flag passed at runtime overrides an entry in the YAML file