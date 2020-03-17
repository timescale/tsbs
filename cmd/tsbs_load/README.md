# How to use tsbs_load

* `$ tsbs_load` 
  * see available commands and global flags
  * available commands: help, config, load
* `$ tsbs_load config`
  * generates an example config file with default values for each specific target
  * see available flags with `$ tsbs_load config --help`:
    * `--data-source` where to load the data from
    * `--target` where to load data into
    * for valid values execute the command
* `$ tsbs_load load [target]` e.g. `$ tsbs_load load prometheus`
  * loads the data into the target database
  * default config is loaded from `./config.yaml`
  * each property can be overridden by the flags available
  * execute `$tsbs_load load [target] --help` to see target specific flags 
  and their description and default values
  * execute `$ tsbs_load load` or `$ tsbs_load load --help` to see available targets
    and description of flags that are common for all target databases (batch size, 
    target db name, number of workers etc)
  * e.g: `--loader.db-specific.adapter-write-url` overwrites the property 
  in the config file for where is the prometheus adapter listening
  * **flags overide values in the config.yaml file**