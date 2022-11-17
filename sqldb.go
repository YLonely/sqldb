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
	"github.com/samber/lo"
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

type OptionInterface interface {
	TargetColumnName() string
	GetValue() any
}

type Option[T any] struct {
	Column Column[T]
	Value  T
}

func NewOption[T any](col Column[T], v T) Option[T] {
	return Option[T]{Column: col, Value: v}
}

func (opt Option[T]) TargetColumnName() string {
	return opt.Column.name
}

func (opt Option[T]) GetValue() any {
	if rt := reflect.TypeOf(opt.Value); rt.Kind() == reflect.Pointer {
		return reflect.ValueOf(opt.Value).Elem().Interface()
	}
	return opt.Value
}

type ValuesOptionInterface interface {
	TargetColumnName() string
	GetValues() []any
}

type ValuesOption[T any] struct {
	Column Column[T]
	Values []T
}

func NewValuesOption[T any](col Column[T], vs []T) ValuesOption[T] {
	return ValuesOption[T]{Column: col, Values: vs}
}

func (opt ValuesOption[T]) TargetColumnName() string {
	return opt.Column.name
}

func (opt ValuesOption[T]) GetValues() []any {
	convert := reflect.TypeOf(opt.Values[0]).Kind() == reflect.Pointer
	return lo.Map(opt.Values, func(v T, _ int) any {
		if convert {
			return reflect.ValueOf(v).Elem().Interface()
		}
		return v
	})
}

type OpQueryOptionInterface interface {
	OptionInterface
	QueryOp() QueryOp
}

// OpQueryOption specifies the query option with query operator.
type OpQueryOption[T any] struct {
	Option[T]
	Op QueryOp
}

// NewOpQueryOption creates an OpQueryOption
func NewOpQueryOption[T any](col Column[T], op QueryOp, v T) OpQueryOption[T] {
	return OpQueryOption[T]{
		Option: Option[T]{
			Column: col,
			Value:  v,
		},
		Op: op,
	}
}

// NewEqualOption creates an OpQueryOption with OpEq
func NewEqualOption[T any](col Column[T], v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpEq, v)
}

// NewNotEqualOption creates an OpQueryOption with OpNe
func NewNotEqualOption[T any](col Column[T], v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpNe, v)
}

// NewGreaterOption creates an OpQueryOption with OpGt
func NewGreaterOption[T any](col Column[T], v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpGt, v)
}

// NewLessOption creates an OpQueryOption with OpLt
func NewLessOption[T any](col Column[T], v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpLt, v)
}

// NewGreaterEqualOption creates an OpQueryOption with OpGte
func NewGreaterEqualOption[T any](col Column[T], v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpGte, v)
}

// NewLessEqualOption creates an OpQueryOption with OpLte
func NewLessEqualOption[T any](col Column[T], v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpLte, v)
}

func (opt OpQueryOption[T]) QueryOp() QueryOp {
	return opt.Op
}

type RangeQueryOptionInterface interface {
	ValuesOptionInterface
}

// RangeQueryOption specifies the query option which does range query.
type RangeQueryOption[T any] struct {
	ValuesOption[T]
}

func NewRangeQueryOption[T any](col Column[T], vs []T) RangeQueryOption[T] {
	return RangeQueryOption[T]{
		ValuesOption: NewValuesOption(col, vs),
	}
}

type FuzzyQueryOptionInterface interface {
	ValuesOptionInterface
}

// FuzzyQueryOption specifics the query option which does fuzzy query.
type FuzzyQueryOption[T any] struct {
	ValuesOption[T]
}

func NewFuzzyQueryOption[T any](col Column[T], vs []T) FuzzyQueryOption[T] {
	return FuzzyQueryOption[T]{
		ValuesOption: NewValuesOption(col, vs),
	}
}

type UpdateOptionInterface interface {
	OptionInterface
}

// UpdateOption specifies an update operation which updates the `Column` with `Value`
type UpdateOption[T any] struct {
	Option[T]
}

func NewUpdateOption[T any](col Column[T], v T) UpdateOption[T] {
	return UpdateOption[T]{
		Option: NewOption(col, v),
	}
}

// FilterOptions contains options related to data filtering.
type FilterOptions struct {
	OpOptions    []OpQueryOptionInterface
	FuzzyOptions []FuzzyQueryOptionInterface
	InOptions    []RangeQueryOptionInterface
	NotInOptions []RangeQueryOptionInterface
}

type SortOrder string

const (
	SortOrderAscending  SortOrder = "asc"
	SortOrderDescending SortOrder = "desc"
)

type SortOptionInterface interface {
	TargetColumnName() string
	SortOrder() SortOrder
}

type SortOption[T comparable] struct {
	Column Column[T]
	Order  SortOrder
}

func NewSortOption[T comparable](col Column[T], order SortOrder) SortOption[T] {
	return SortOption[T]{
		Column: col,
		Order:  order,
	}
}

func (opt SortOption[T]) TargetColumnName() string {
	return opt.Column.name
}

func (opt SortOption[T]) SortOrder() SortOrder {
	return opt.Order
}

// ListOptions contains options and parameters related to data listing.
type ListOptions struct {
	FilterOptions
	Offset      uint64
	Limit       uint64
	SortOptions []SortOptionInterface
}

// Model is an interface defines commonly used methods to manipulate data.
type Model[T any] interface {
	// Columns returns a instance of type T,
	// all fields of type sqldb.Column[U] in the instance are populated with corresponding column name.
	Columns() T
	Create(ctx context.Context, entity *T) error
	Get(ctx context.Context, opts []OpQueryOptionInterface) (*T, error)
	List(ctx context.Context, opts ListOptions) ([]*T, uint64, error)
	Update(ctx context.Context, query FilterOptions, opts []UpdateOptionInterface) (uint64, error)
	Delete(ctx context.Context, opts FilterOptions) error
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
