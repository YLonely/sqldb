package sqldb

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/samber/lo"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"

	"github.com/YLonely/sqldb/internal/sql"
)

type QueryOp string

const (
	OpEq  QueryOp = "="
	OpNe  QueryOp = "!="
	OpGt  QueryOp = ">"
	OpLt  QueryOp = "<"
	OpGte QueryOp = ">="
	OpLte QueryOp = "<="
)

// OptionInterface wraps basic methods of options.
type OptionInterface interface {
	// TargetColumnName returns the name of the column an operation processes against.
	TargetColumnName() ColumnName
	// GetValue returns the value the option carries. It is used by the operation to query or update the target column.
	GetValue() any
}

// Option implements the OptionInterface.
type Option[T any] struct {
	Column ColumnName
	Value  T
}

// NewOption returns an new Option.
func NewOption[T any, C ColumnType[T]](col C, v T) Option[T] {
	return Option[T]{Column: (any)(col).(ColumnGetter).GetColumnName(), Value: v}
}

func (opt Option[T]) TargetColumnName() ColumnName {
	return opt.Column
}

func (opt Option[T]) GetValue() any {
	return opt.Value
}

// ValuesOptionInterface wraps basic method of options which carry multiple values.
type ValuesOptionInterface interface {
	// TargetColumnName returns the name of the column an operation processes against.
	TargetColumnName() ColumnName
	// GetValues returns the values the option carries. Those values are used to query data.
	GetValues() []any
}

// ValuesOption implements the ValuesOptionInterface.
type ValuesOption[T comparable] struct {
	Column ColumnName
	Values []T
}

// NewValuesOption returns a new ValuesOption.
func NewValuesOption[T comparable, C ColumnType[T]](col C, vs []T) ValuesOption[T] {
	return ValuesOption[T]{Column: (any)(col).(ColumnGetter).GetColumnName(), Values: vs}
}

func (opt ValuesOption[T]) TargetColumnName() ColumnName {
	return opt.Column
}

func (opt ValuesOption[T]) GetValues() []any {
	return lo.ToAnySlice(opt.Values)
}

// OpQueryOptionInterface represents a query which use the given query operator to search data.
type OpQueryOptionInterface interface {
	OptionInterface
	QueryOp() QueryOp
}

// OpQueryOption implements the OpQueryOptionInterface.
type OpQueryOption[T comparable] struct {
	Option[T]
	Op QueryOp
}

// NewOpQueryOption creates an OpQueryOption.
func NewOpQueryOption[T comparable, C ColumnType[T]](col C, op QueryOp, v T) OpQueryOption[T] {
	return OpQueryOption[T]{
		Option: Option[T]{
			Column: (any)(col).(ColumnGetter).GetColumnName(),
			Value:  v,
		},
		Op: op,
	}
}

// NewEqualOption creates an OpQueryOption with operator OpEq.
func NewEqualOption[T comparable, C ColumnType[T]](col C, v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpEq, v)
}

// NewNotEqualOption creates an OpQueryOption with operator OpNe.
func NewNotEqualOption[T comparable, C ColumnType[T]](col C, v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpNe, v)
}

// NewGreaterOption creates an OpQueryOption with operator OpGt.
func NewGreaterOption[T comparable, C ColumnType[T]](col C, v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpGt, v)
}

// NewLessOption creates an OpQueryOption with operator OpLt.
func NewLessOption[T comparable, C ColumnType[T]](col C, v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpLt, v)
}

// NewGreaterEqualOption creates an OpQueryOption with operator OpGte.
func NewGreaterEqualOption[T comparable, C ColumnType[T]](col C, v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpGte, v)
}

// NewLessEqualOption creates an OpQueryOption with operator OpLte.
func NewLessEqualOption[T comparable, C ColumnType[T]](col C, v T) OpQueryOption[T] {
	return NewOpQueryOption(col, OpLte, v)
}

func (opt OpQueryOption[T]) QueryOp() QueryOp {
	return opt.Op
}

// RangeQueryOptionInterface represents a query that find data from a given range of values.
type RangeQueryOptionInterface interface {
	ValuesOptionInterface
}

// RangeQueryOption implements the RangeQueryOptionInterface.
type RangeQueryOption[T comparable] struct {
	ValuesOption[T]
}

// NewRangeQueryOption creates a new RangeQueryOption.
func NewRangeQueryOption[T comparable, C ColumnType[T]](col C, vs []T) RangeQueryOption[T] {
	return RangeQueryOption[T]{
		ValuesOption: NewValuesOption(col, vs),
	}
}

// FuzzyQueryOptionInterface represents a query that find data that match given patterns approximately.
type FuzzyQueryOptionInterface interface {
	ValuesOptionInterface
}

// FuzzyQueryOption implements the FuzzyQueryOptionInterface.
type FuzzyQueryOption[T comparable] struct {
	ValuesOption[T]
}

// NewFuzzyQueryOption creates a new FuzzyQueryOption.
func NewFuzzyQueryOption[T comparable, C ColumnType[T]](col C, vs []T) FuzzyQueryOption[T] {
	return FuzzyQueryOption[T]{
		ValuesOption: NewValuesOption(col, vs),
	}
}

// UpdateOptionInterface represents an update operation that updates the target column with given value.
type UpdateOptionInterface interface {
	OptionInterface
}

// UpdateOption implements the UpdateOptionInterface.
type UpdateOption[T any] struct {
	Option[T]
}

// NewUpdateOption creates a new UpdateOption.
func NewUpdateOption[T any, C ColumnType[T]](col C, v T) UpdateOption[T] {
	return UpdateOption[T]{
		Option: NewOption(col, v),
	}
}

// FilterOptions contains options that related to data filtering.
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

// SortOptionInterface represents an sort operation.
type SortOptionInterface interface {
	TargetColumnName() ColumnName
	SortOrder() SortOrder
}

// SortOption implements the SortOptionInterface.
type SortOption[T comparable] struct {
	Column ColumnName
	Order  SortOrder
}

// NewSortOption creates a new SortOption.
func NewSortOption[T comparable, C ColumnType[T]](col C, order SortOrder) SortOption[T] {
	return SortOption[T]{
		Column: (any)(col).(ColumnGetter).GetColumnName(),
		Order:  order,
	}
}

func (opt SortOption[T]) TargetColumnName() ColumnName {
	return opt.Column
}

func (opt SortOption[T]) SortOrder() SortOrder {
	return opt.Order
}

// ListOptions contains options and parameters that related to data listing.
type ListOptions struct {
	FilterOptions
	Offset      uint64
	Limit       uint64
	SortOptions []SortOptionInterface
}

// columnSetter sets the column name of a filed
type columnSetter interface {
	setColumnName(name string)
}

type ColumnGetter interface {
	GetColumnName() ColumnName
}

type ColumnName string

func (cn ColumnName) GetColumnName() ColumnName {
	return cn
}

func (cn *ColumnName) setColumnName(name string) {
	*cn = ColumnName(name)
}

type ColumnValue[T any] struct {
	V T
}

func (cv ColumnValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(cv.V)
}

func (cv *ColumnValue[T]) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &cv.V)
}

// Value implements the driver Valuer interface.
func (cv ColumnValue[T]) Value() (driver.Value, error) {
	return driver.DefaultParameterConverter.ConvertValue(cv.V)
}

// Scan implements the Scanner interface.
func (cv *ColumnValue[T]) Scan(src any) error {
	return sql.ConvertAssign(&cv.V, src)
}

// CreateClauses implements the CreateClausesInterface interface from GORM.
func (cv ColumnValue[T]) CreateClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(cv.V).(schema.CreateClausesInterface); ok {
		return fc.CreateClauses(f)
	}
	return nil
}

// QueryClauses implements the QueryClausesInterface interface from GORM.
func (cv ColumnValue[T]) QueryClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(cv.V).(schema.QueryClausesInterface); ok {
		return fc.QueryClauses(f)
	}
	return nil
}

// UpdateClauses implements the UpdateClausesInterface interface from GORM.
func (cv ColumnValue[T]) UpdateClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(cv.V).(schema.UpdateClausesInterface); ok {
		return fc.UpdateClauses(f)
	}
	return nil
}

// DeleteClauses implements the DeleteClausesInterface interface from GORM.
func (cv ColumnValue[T]) DeleteClauses(f *schema.Field) []clause.Interface {
	if fc, ok := any(cv.V).(schema.DeleteClausesInterface); ok {
		return fc.DeleteClauses(f)
	}
	return nil
}

/*
PtrColumn is used when declaring models with pointer fields, for example:

	type Model struct{
		Name PtrColumn[string]
	}

equals to

	type Model struct{
		Name *string
	}
*/
type PtrColumn[T any] struct {
	ColumnValue[*T]
	ColumnName
}

// NewPtrColumn creates a new PtrColumn of type T.
func NewPtrColumn[T any](v T) PtrColumn[T] {
	return PtrColumn[T]{
		ColumnValue: ColumnValue[*T]{
			V: &v,
		},
	}
}

// Column represents a column of a table.
type Column[T any] struct {
	ColumnValue[T]
	ColumnName
}

// NewColumn creates a new Column of type T.
func NewColumn[T any](v T) Column[T] {
	return Column[T]{
		ColumnValue: ColumnValue[T]{
			V: v,
		},
	}
}

// ColumnType contains valid column types.
type ColumnType[T any] interface {
	Column[T] | PtrColumn[T]
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
