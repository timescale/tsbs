package main

import (
	"bytes"
	"github.com/spf13/viper"
)

func main() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	//err := viper.ReadInConfig()
	yamlExample := `
dataSource:
  type: FILE
  file:
    location: 1
`
	err := viper.ReadConfig(bytes.NewBuffer([]byte(yamlExample)))
	if err != nil {
		panic(err)
	}
	topLevel := viper.GetViper()
	conf := LoadConfig{}
	if err := topLevel.UnmarshalExact(&conf); err != nil {
		panic(err)
	}
	if err := conf.Validate(); err != nil {
		panic(err)
	}
}
