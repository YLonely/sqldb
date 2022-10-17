package sqldb

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"

	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"

	"github.com/YLonely/sqldb/internal/sql"
)

type TransactionFunc func(ctx context.Context, run func(context.Context) error) error

type QueryOp string

const (
	OpEq  QueryOp = "="
	OpNe  QueryOp = "!="
	OpGt  QueryOp = ">"
	OpLt  QueryOp = "<"
	OpGte QueryOp = ">="
	OpLte QueryOp = "<="
)

type OpQueryOption struct {
	Column ColumnGetter
	Op     QueryOp
	Value  any
}

func NewOpQueryOption(col ColumnGetter, op QueryOp, v any) OpQueryOption {
	return OpQueryOption{
		Column: col,
		Op:     op,
		Value:  v,
	}
}

func NewEqualOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpEq, v)
}

func NewNotEqualOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpNe, v)
}

func NewGreaterOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpGt, v)
}

func NewLessOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpLt, v)
}

func NewGreaterEqualOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpGte, v)
}

func NewLessEqualOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpLte, v)
}

type FuzzyQueryOption struct {
	Column ColumnGetter
	Values []any
}

type RangeQueryOption struct {
	Column ColumnGetter
	Values []any
}

type FilterOptions struct {
	OpOptions    []OpQueryOption
	FuzzyOptions []FuzzyQueryOption
	InOptions    []RangeQueryOption
	NotInOptions []RangeQueryOption
}

type ListOptions struct {
	FilterOptions
	Offset uint64
	Limit  uint64
}

type UpdateOption struct {
	Column ColumnGetter
	Value  any
}

type Model[T any] interface {
	Create(ctx context.Context, entity *T) error
	Get(ctx context.Context, opts []OpQueryOption) (*T, error)
	List(ctx context.Context, opts ListOptions) ([]*T, uint64, error)
	Update(ctx context.Context, query FilterOptions, opts []UpdateOption) (uint64, error)
	Delete(ctx context.Context, opts FilterOptions) error
}

// ColumnGetter tells the column name of the field in the database.
type ColumnGetter interface {
	GetColumnName() string
}

// columnSetter sets the column name of a filed
type columnSetter interface {
	setColumnName(name string)
}

// Column represents a column of a table.
type Column[T any] struct {
	V    T
	name string
}

func NewColumn[T any](v T) Column[T] {
	return Column[T]{V: v}
}

func (c Column[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.V)
}

func (c *Column[T]) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &c.V)
}

func (c *Column[T]) setColumnName(name string) {
	c.name = name
}

func (c Column[T]) GetColumnName() string {
	return c.name
}

func (c Column[T]) Value() (driver.Value, error) {
	return driver.DefaultParameterConverter.ConvertValue(c.V)
}

func (c *Column[T]) Scan(src any) error {
	return sql.ConvertAssign(&c.V, src)
}

func (c Column[T]) CreateClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(c.V).(schema.CreateClausesInterface); ok {
		return fc.CreateClauses(f)
	}
	return nil
}

func (c Column[T]) QueryClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(c.V).(schema.QueryClausesInterface); ok {
		return fc.QueryClauses(f)
	}
	return nil
}

func (c Column[T]) UpdateClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(c.V).(schema.UpdateClausesInterface); ok {
		return fc.UpdateClauses(f)
	}
	return nil
}

func (c Column[T]) DeleteClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(c.V).(schema.DeleteClausesInterface); ok {
		return fc.DeleteClauses(f)
	}
	return nil
}

type NameColumnFunc func(target reflect.StructField, parents ...reflect.StructField) string

func populateColumns(obj any, nameColumn NameColumnFunc, parents ...reflect.StructField) error {
	rv := reflect.ValueOf(obj)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("object provided is not a ptr or it's nil")
	}
	e := rv.Elem()
	if e.Kind() != reflect.Struct {
		return errors.New("object provided is not the reference of a struct")
	}
	for i := 0; i < e.NumField(); i++ {
		fieldAddr := e.Field(i).Addr()
		field := e.Type().Field(i)
		if setter, ok := fieldAddr.Interface().(columnSetter); ok {
			setter.setColumnName(nameColumn(field, parents...))
		} else {
			if err := populateColumns(fieldAddr.Interface(), nameColumn, append(parents, field)...); err != nil {
				return err
			}
		}
	}
	return nil
}

type ColumnHint[T any] struct {
	Columns *T
}

func NewModel[T any](f NameColumnFunc) ColumnHint[T] {
	m := new(T)
	if err := populateColumns(m, f); err != nil {
		panic(err)
	}
	return ColumnHint[T]{
		Columns: m,
	}
}
