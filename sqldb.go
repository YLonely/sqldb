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

// A TransactionFunc starts a transaction.
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

// OpQueryOption specifies the query option with query operator.
type OpQueryOption struct {
	Column ColumnGetter
	Op     QueryOp
	Value  any
}

// NewOpQueryOption creates an OpQueryOption
func NewOpQueryOption(col ColumnGetter, op QueryOp, v any) OpQueryOption {
	return OpQueryOption{
		Column: col,
		Op:     op,
		Value:  v,
	}
}

// NewEqualOption creates an OpQueryOption with OpEq
func NewEqualOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpEq, v)
}

// NewNotEqualOption creates an OpQueryOption with OpNe
func NewNotEqualOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpNe, v)
}

// NewGreaterOption creates an OpQueryOption with OpGt
func NewGreaterOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpGt, v)
}

// NewLessOption creates an OpQueryOption with OpLt
func NewLessOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpLt, v)
}

// NewGreaterEqualOption creates an OpQueryOption with OpGte
func NewGreaterEqualOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpGte, v)
}

// NewLessEqualOption creates an OpQueryOption with OpLte
func NewLessEqualOption(col ColumnGetter, v any) OpQueryOption {
	return NewOpQueryOption(col, OpLte, v)
}

// FuzzyQueryOption specifics the query option which does fuzzy query.
type FuzzyQueryOption struct {
	Column ColumnGetter
	Values []any
}

// RangeQueryOption specifies the query option which does range query.
type RangeQueryOption struct {
	Column ColumnGetter
	Values []any
}

// FilterOptions contains options related to data filtering.
type FilterOptions struct {
	OpOptions    []OpQueryOption
	FuzzyOptions []FuzzyQueryOption
	InOptions    []RangeQueryOption
	NotInOptions []RangeQueryOption
}

type SortOrder string

const (
	SortOrderAscending  SortOrder = "asc"
	SortOrderDescending SortOrder = "desc"
)

type SortOption struct {
	Column ColumnGetter
	Order  SortOrder
}

// ListOptions contains options and parameters related to data listing.
type ListOptions struct {
	FilterOptions
	Offset      uint64
	Limit       uint64
	SortOptions []SortOption
}

// UpdateOption specifies an update operation which updates the `Column` with `Value`
type UpdateOption struct {
	Column ColumnGetter
	Value  any
}

// Model is an interface defines commonly used methods to manipulate data.
type Model[T any] interface {
	// Columns returns a instance of type T,
	// all fields of type sqldb.Column[U] in the instance are populated with corresponding column name.
	Columns() T
	Create(ctx context.Context, entity *T) error
	Get(ctx context.Context, opts []OpQueryOption) (*T, error)
	List(ctx context.Context, opts ListOptions) ([]*T, uint64, error)
	Update(ctx context.Context, query FilterOptions, opts []UpdateOption) (uint64, error)
	Delete(ctx context.Context, opts FilterOptions) error
}

// ColumnGetter returns the column name of the field in the database.
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

// NewColumn creates a new Column of type T.
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

// Value implements the driver Valuer interface.
func (c Column[T]) Value() (driver.Value, error) {
	return driver.DefaultParameterConverter.ConvertValue(c.V)
}

// Scan implements the Scanner interface.
func (c *Column[T]) Scan(src any) error {
	return sql.ConvertAssign(&c.V, src)
}

// CreateClauses implements the CreateClausesInterface interface from GORM.
func (c Column[T]) CreateClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(c.V).(schema.CreateClausesInterface); ok {
		return fc.CreateClauses(f)
	}
	return nil
}

// QueryClauses implements the QueryClausesInterface interface from GORM.
func (c Column[T]) QueryClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(c.V).(schema.QueryClausesInterface); ok {
		return fc.QueryClauses(f)
	}
	return nil
}

// UpdateClauses implements the UpdateClausesInterface interface from GORM.
func (c Column[T]) UpdateClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(c.V).(schema.UpdateClausesInterface); ok {
		return fc.UpdateClauses(f)
	}
	return nil
}

// DeleteClauses implements the DeleteClausesInterface interface from GORM.
func (c Column[T]) DeleteClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(c.V).(schema.DeleteClausesInterface); ok {
		return fc.DeleteClauses(f)
	}
	return nil
}

// A NameFieldFunc gives the target filed a corresponding column name.
type NameFieldFunc func(target reflect.StructField, parents ...reflect.StructField) string

func populateColumns(obj any, nameColumn NameFieldFunc, parents ...reflect.StructField) error {
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

// ColumnHint is supposed to be embedded into structs to implement the Model interface.
type ColumnHint[T any] struct {
	columns *T
}

func (ch ColumnHint[T]) Columns() T {
	return *ch.columns
}

func NewColumnHint[T any](f NameFieldFunc) ColumnHint[T] {
	m := new(T)
	if err := populateColumns(m, f); err != nil {
		panic(err)
	}
	return ColumnHint[T]{
		columns: m,
	}
}
