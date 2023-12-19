package gendata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFields(t *testing.T) {
	zzScript := `
fields = {
    types = {'bigint(2)', 'float', 'double', 'enum'},
    sign = {'signed', 'unsigned'},
    keys = {'undef', 'key'},
    null = {'not null', 'null'}
}
`
	l, err := runLua(zzScript)
	assert.Equal(t, nil, err)

	fields, err := newFields(l)
	assert.Equal(t, nil, err)

	stmts, _, err := fields.gen()
	assert.Equal(t, nil, err)

	assert.Equal(t, 42, len(stmts))

	/*	for _, stmt := range stmts {
		fmt.Println(stmt)
	}*/
}
