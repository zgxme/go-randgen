package gendata

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
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

const (
	KeyTypeAggregate = "AGGREGATE"
	KeyTypeDuplicate = "DUPLICATE"

	// May cause duplicate value error. Unsupported now.
	// KeyTypeUnique = "UNIQUE"
)

var (
	defaultTablesTmpl = mustParse("tables", "create table {{.tname}} (\n"+
		"%s\n"+
		") {{.charsets}} {{.partitions}}")
	dorisTablesTmpl = mustParse("tables", "create table {{.tname}} (\n"+
		"%s\n"+
		") engine=olap\n"+
		"distributed by hash(pk) buckets 10\n"+
		"{{.keys}}\n"+
		"{{.partitions}}\n"+
		"properties('replication_num' = '1')")
	tablesTmplData = map[string]*template.Template{
		utils.DodirTmpl:   dorisTablesTmpl,
		utils.DefaultTmpl: defaultTablesTmpl,
	}
	tablesTmpl        = defaultTablesTmpl
	DBMS              = utils.MYSQL
	SupportedKeyTypes = []string{KeyTypeAggregate, KeyTypeDuplicate}
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
	{
		"keys",
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

		stmt.partitionFields = parseListFields(text)

		return "PARTITION BY " + text, nil
	},
	"keys": func(key string, stmt *tableStmt) (string, error) {
		key = strings.TrimSpace(key)
		if key == "undef" {
			if len(stmt.partitionFields) == 0 {
				return "", nil
			}

			// Must have keys if partitions exist.
			// Randomly choose a key type here.
			key = SupportedKeyTypes[rand.Intn(len(SupportedKeyTypes))] + " Key"
		}

		// Default key column is `pk`.
		if !strings.Contains(key, "(") {
			key = fmt.Sprintf("%s(pk)", key)
		}

		stmt.keyFields = parseListFields(key)
		keyType, err := getKeyType(key)
		if err != nil {
			return "", err
		}

		// prepend partition fields to the key fields
		stmt.keyFields = append(stmt.partitionFields, stmt.keyFields...)

		return fmt.Sprintf("%s KEY(%s)", keyType, strings.Join(stmt.keyFields, ", ")), nil
	},
}

func getKeyType(text string) (string, error) {
	text = strings.ToUpper(text)
	for _, keyType := range SupportedKeyTypes {
		if strings.HasPrefix(text, keyType) {
			return keyType, nil
		}
	}
	return "", fmt.Errorf("unsupported key type: %s, expect one of %v", text, SupportedKeyTypes)
}

var list_fields_re = regexp.MustCompile(`^(?i:RANGE|LIST|AGGREGATE\s+KEY|DUPLICATE\s+KEY)\s*\(([^()]*)\)`)

// RANGE(col1, col2) -> [col1, col2]
func parseListFields(list string) []string {
	list = strings.TrimSpace(list)
	matches := list_fields_re.FindStringSubmatch(list)
	if len(matches) != 2 {
		log.Fatalln("list fields not found in:", list)
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

	tableName := &bytes.Buffer{}
	m := make(map[string]string)
	stmts := make([]*tableStmt, 0, t.numbers)

	tableTmpM := make(map[string]int)

	err := t.traverse(func(cur []string) error {
		tableName.Reset()
		tableName.WriteString(tnamePrefix)
		stmt := &tableStmt{}
		for i := range cur {
			// current field name: fields[i]
			// current field value: curr[i]
			field := t.fields[i]
			curname := cur[i]
			if field == "partitions" || field == "keys" {
				curname = field + strconv.Itoa(i)
			}
			tableName.WriteString("_" + curname)

			target, err := tableFuncs[field](cur[i], stmt)
			if err != nil {
				return err
			}
			m[field] = target
		}

		tname := tableName.String()

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
	ddl string
	// the cols used in partition
	partitionFields []string
	// the cols used in key
	keyFields []string
}

func (t *tableStmt) wrapInTable(fieldStmts []string) {
	buf := &bytes.Buffer{}
	buf.WriteString(strings.Join(fieldStmts, ",\n"))
	t.ddl = fmt.Sprintf(t.format, buf.String())
}
