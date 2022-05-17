package config

type server struct {
	Host string    `mapstructure:"host"`
	Port string    `mapstructure:"port"`
	TLS  serverTLS `mapstructure:"tls"`
}

type serverTLS struct {
	Cert string `mapstructure:"cert"`
	Key  string `mapstructure:"key"`
}

type log struct {
	Level string `mapstructure:"level"`
}

type mysqlConfig struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
}

type tidbConfig struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	Database string `mapstructure:"database"`
}

type datasource struct {
	Type  string      `mapstructure:"type"`
	MySQL mysqlConfig `mapstructure:"mysql"`
	TiDB  tidbConfig  `mapstructure:"tidb"`
}

type config struct {
	Server     server     `mapstructure:"server"`
	Log        log        `mapstructure:"log"`
	DataSource datasource `mapstructure:"datasource"`
}
