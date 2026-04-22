package config

type Config struct {
	Server     ServerConfig
	Storage    StorageConfig
	Search     SearchConfig
	Versioning VersioningConfig
	Auth       AuthConfig
}

type ServerConfig struct {
	Host string
	Port int
}

type StorageConfig struct {
	Root string
}

type SearchConfig struct {
	Engine string // grep | sqlite | bleve | vector
}

type VersioningConfig struct {
	Strategy string // git | cow | none
}

type AuthConfig struct {
	Type   string // none | apikey | oidc
	APIKey string
}
