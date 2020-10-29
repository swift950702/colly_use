package cfgstruct

type RedisSt struct {
	Hostport string `json:"hostport" ini:"hostport" yaml:"hostport"`
	Poolsize int    `json:"poolsize" ini:"poolsize" yaml:"poolsize"`
	Auth     string `json:"auth" ini:"auth" yaml:"auth"`
	Timeout  int    `json:"timeout" ini:"timeout" yaml:"timeout"`
}
