package utils

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
