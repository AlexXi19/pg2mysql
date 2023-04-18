package pg2mysql

type Config struct {
	MySQL struct {
		Database string            `yaml:"database"`
		Username string            `yaml:"username"`
		Password string            `yaml:"password"`
		Host     string            `yaml:"host"`
		Port     int               `yaml:"port"`
		Params   map[string]string `yaml:"params" default:"{}"`
	}

	PostgreSQL struct {
		Database string `yaml:"database"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		SSLMode  string `yaml:"ssl_mode"`
	} `yaml:"postgresql"`
}
