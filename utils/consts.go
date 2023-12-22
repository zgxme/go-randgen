package utils

const AggTypePlaceholder = "/*agg_type_placeholder*/"

const (
	KeyTypeAggregate = "AGGREGATE"
	KeyTypeDuplicate = "DUPLICATE"

	// May cause duplicate value error. Unsupported now.
	// KeyTypeUnique = "UNIQUE"
)

var SupportedKeyTypes = []string{
	KeyTypeAggregate,
	KeyTypeDuplicate,
}

const DORIS = "doris"
const SQLITE3 = "sqlite3"
const POSTGRE = "postgres"
const MYSQL = "mysql"
const DEFAULT = "default"

const DodirTmpl = "doris_tmp"
const DefaultTmpl = "default_tmp"

var DbmsType = map[string]string{
	DORIS:   DodirTmpl,
	SQLITE3: DefaultTmpl,
	POSTGRE: DefaultTmpl,
	MYSQL:   DefaultTmpl,
	DEFAULT: DefaultTmpl,
}
