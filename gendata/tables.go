package gendata

import (
	"bytes"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/pingcap/go-randgen/utils"
	"github.com/samber/lo"
	lua "github.com/yuin/gopher-lua"
)

type Tables struct {
	*options
}

var defaultTablesTmpl = mustParse("tables", "create table {{.tname}} (\n"+
	"`pk` int primary key%s\n"+
	") {{.charsets}} {{.partitions}}")

var dorisTablesTmpl = mustParse("tables", "create table {{.tname}} (\n"+
	"`pk` int%s\n"+
	") engine=olap\n"+
	"distributed by hash(pk) buckets 10\n"+
	"{{.partitions}}\n"+
	"properties(\n"+
	"	'replication_num' = '1')")

var tablesTmplData = map[string]*template.Template{
	utils.DodirTmpl:   dorisTablesTmpl,
	utils.DefaultTmpl: defaultTablesTmpl,
}

var (
	tablesTmpl = defaultTablesTmpl
	DBMS       = utils.MYSQL
)

func InitTmpl(dbms string) {
	DBMS = dbms
	if val, ok := utils.DbmsType[dbms]; ok {
		tablesTmpl = tablesTmplData[val]
	} else {
		log.Println("dbms not exist, set tablesTmpl default")
	}
}

// support vars
var tablesVars = []*varWithDefault{
	{
		"rows",
		[]string{"0", "1", "2", "10", "100"},
	},
	{
		"charsets",
		[]string{"undef"},
	},
	{
		"partitions",
		[]string{"undef"},
	},
}

// process function
var tableFuncs = map[string]func(string, *tableStmt) (string, error){
	"rows": func(text string, stmt *tableStmt) (s string, e error) {
		rows, err := strconv.Atoi(text)
		if err != nil {
			return "", err
		}

		stmt.rowNum = rows
		return "", nil
	},
	"charsets": func(text string, stmt *tableStmt) (s string, e error) {
		if text == "undef" {
			return "", nil
		}
		return fmt.Sprintf("character set %s", text), nil
	},
	"partitions": func(text string, stmt *tableStmt) (s string, e error) {
		if text == "undef" {
			return "", nil
		}

		// Doris does not support hash partition.
		if DBMS != utils.DORIS {
			num, err := strconv.Atoi(text)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("\npartition by hash(pk)\npartitions %d", num), nil
		}

		stmt.partitionFields = parsePartitionFields(text)

		return "PARTITION BY " + text, nil
	},
}

var partition_field_re = regexp.MustCompile(`(?i:range|list)\s*\((.*)\)\s*\(`)

// RANGE(col1, col2) -> [col1, col2]
func parsePartitionFields(partitions string) []string {
	matches := partition_field_re.FindStringSubmatch(partitions)
	if len(matches) != 2 {
		log.Fatalln("partition fields not found")
	}

	return lo.Map(strings.Split(matches[1], ","), func(field string, _ int) string { return strings.Trim(field, "` ") })
}

func newTables(l *lua.LState) (*Tables, error) {
	o, err := newOptions(tablesTmpl, l, "tables", tablesVars)

	if err != nil {
		return nil, err
	}

	return &Tables{o}, nil
}

func (t *Tables) gen() ([]*tableStmt, error) {
	tnamePrefix := "table"

	buf := &bytes.Buffer{}
	m := make(map[string]string)
	stmts := make([]*tableStmt, 0, t.numbers)

	tableTmpM := make(map[string]int)

	err := t.traverse(func(cur []string) error {
		buf.Reset()
		buf.WriteString(tnamePrefix)
		stmt := &tableStmt{}
		for i := range cur {
			// current field name: fields[i]
			// current field value: curr[i]
			field := t.fields[i]
			buf.WriteString("_" + cur[i])
			target, err := tableFuncs[field](cur[i], stmt)
			if err != nil {
				return err
			}
			m[field] = target
		}

		tname := buf.String()

		if v, ok := tableTmpM[tname]; !ok {
			tableTmpM[tname] = 1
		} else {
			nv := v + 1
			tableTmpM[tname] = nv
			tname = tname + strconv.Itoa(nv)
		}
		stmt.name = tname

		m["tname"] = tname

		stmt.format = t.format(m)

		stmts = append(stmts, stmt)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return stmts, nil
}

type tableStmt struct {
	// create statement without field part
	format string
	// table name
	name   string
	rowNum int
	// generate by wrapInTable
	ddl             string
	partitionFields []string
}

func (t *tableStmt) wrapInTable(fieldStmts []string) {
	buf := &bytes.Buffer{}
	buf.WriteString(",\n")
	buf.WriteString(strings.Join(fieldStmts, ",\n"))
	t.ddl = fmt.Sprintf(t.format, buf.String())
}
