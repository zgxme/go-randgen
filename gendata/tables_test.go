package gendata

import (
	"strings"
	"testing"

	"github.com/pingcap/go-randgen/utils"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func init() {
	InitTmpl(utils.DORIS)
}

func TestTables(t *testing.T) {
	zzScript := `
tables = {
    rows = {10, 20, 30},
    charsets = {'utf8', 'utf8mb4', 'undef'},
    partitions = {"Range ( col1, col2 ) (PARTITION p0 VALUES LESS THAN ('2020-05-25')),PARTITION p1 VALUES [('2020-05-25'), ('2020-05-26'))))", 'undef'},
}
`
	l, err := runLua(zzScript)
	assert.Equal(t, nil, err)

	tables, err := newTables(l)
	assert.Equal(t, nil, err)

	stmts, err := tables.gen()
	assert.Equal(t, nil, err)

	assert.Len(t, lo.Filter(stmts, func(s *tableStmt, _ int) bool {
		return len(s.partitionFields) == 2 && strings.Contains(s.format, "PARTITION BY Range")
	}), 9)

	assert.Equal(t, tables.numbers, len(stmts))

	/*	for _, stmt := range stmts {
		fmt.Println("==========")
		fmt.Println(stmt.format)
		fmt.Println(stmt.rowNum)
	}*/
}

func TestParsePartitionFields(t *testing.T) {
	partitions := `
	Range ( col1, ` + "`col2`" + ` )(PARTITION p0 VALUES LESS THAN ('2020-05-25')),
		PARTITION p1 VALUES [('2020-05-25'), ('2020-05-26')))
	)
	`

	assert.Equal(t, []string{"col1", "col2"}, parsePartitionFields(partitions))
}
