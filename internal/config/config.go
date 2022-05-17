package config

import (
	"strings"

	logger "github.com/api7/gopkg/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var (
	// the Config for etcd adapter
	Config config
)

// Init load and unmarshal config file
func Init(configFile string) ([]logger.Option, error) {
	if configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}
	viper.AddConfigPath("config")

	// read configuration file
	viper.SetEnvPrefix("EA")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	err := viper.ReadInConfig()
	if err != nil {
		return nil, errors.Wrap(err, "Config file load failed")
	}

	// parse configuration
	err = viper.Unmarshal(&Config)
	if err != nil {
		return nil, errors.Wrap(err, "Config file unmarshal failed")
	}

	opts := []logger.Option{}
	if Config.Log.Level != "" {
		opts = append(opts, logger.WithLogLevel(Config.Log.Level))
	}

	return opts, nil
}
