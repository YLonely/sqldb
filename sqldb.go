package sqldb

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/samber/lo"
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

// OptionInterface wraps basic methods of options.
type OptionInterface interface {
	// TargetColumnName returns the name of the column an operation processes against.
	TargetColumnName() string
	// GetValue returns the value the option carries. It is used by the operation to query or update the target column.
	GetValue() any
}

// Option implements the OptionInterface.
type Option[T any, C Column[T] | PtrColumn[T]] struct {
	Column C
	Value  T
}

// NewOption returns an new Option.
func NewOption[T any, C Column[T] | PtrColumn[T]](col C, v T) Option[T, C] {
	return Option[T, C]{Column: col, Value: v}
}

func (opt Option[T, C]) TargetColumnName() string {
	return (any)(opt.Column).(ColumnGetter).GetColumnName()
}

func (opt Option[T, C]) GetValue() any {
	return opt.Value
}

// ValuesOptionInterface wraps basic method of options which carry multiple values.
type ValuesOptionInterface interface {
	// TargetColumnName returns the name of the column an operation processes against.
	TargetColumnName() string
	// GetValues returns the values the option carries. Those values are used to query data.
	GetValues() []any
}

// ValuesOption implements the ValuesOptionInterface.
type ValuesOption[T any, C Column[T] | PtrColumn[T]] struct {
	Column C
	Values []T
}

// NewValuesOption returns a new ValuesOption.
func NewValuesOption[T any, C Column[T] | PtrColumn[T]](col C, vs []T) ValuesOption[T, C] {
	return ValuesOption[T, C]{Column: col, Values: vs}
}

func (opt ValuesOption[T, C]) TargetColumnName() string {
	return (any)(opt.Column).(ColumnGetter).GetColumnName()
}

func (opt ValuesOption[T, C]) GetValues() []any {
	return lo.ToAnySlice(opt.Values)
}

// OpQueryOptionInterface represents a query which use the given query operator to search data.
type OpQueryOptionInterface interface {
	OptionInterface
	QueryOp() QueryOp
}

// OpQueryOption implements the OpQueryOptionInterface.
type OpQueryOption[T any, C Column[T] | PtrColumn[T]] struct {
	Option[T, C]
	Op QueryOp
}

// NewOpQueryOption creates an OpQueryOption.
func NewOpQueryOption[T any, C Column[T] | PtrColumn[T]](col C, op QueryOp, v T) OpQueryOption[T, C] {
	return OpQueryOption[T, C]{
		Option: Option[T, C]{
			Column: col,
			Value:  v,
		},
		Op: op,
	}
}

// NewEqualOption creates an OpQueryOption with operator OpEq.
func NewEqualOption[T any, C Column[T] | PtrColumn[T]](col C, v T) OpQueryOption[T, C] {
	return NewOpQueryOption(col, OpEq, v)
}

// NewNotEqualOption creates an OpQueryOption with operator OpNe.
func NewNotEqualOption[T any, C Column[T] | PtrColumn[T]](col C, v T) OpQueryOption[T, C] {
	return NewOpQueryOption(col, OpNe, v)
}

// NewGreaterOption creates an OpQueryOption with operator OpGt.
func NewGreaterOption[T any, C Column[T] | PtrColumn[T]](col C, v T) OpQueryOption[T, C] {
	return NewOpQueryOption(col, OpGt, v)
}

// NewLessOption creates an OpQueryOption with operator OpLt.
func NewLessOption[T any, C Column[T] | PtrColumn[T]](col C, v T) OpQueryOption[T, C] {
	return NewOpQueryOption(col, OpLt, v)
}

// NewGreaterEqualOption creates an OpQueryOption with operator OpGte.
func NewGreaterEqualOption[T any, C Column[T] | PtrColumn[T]](col C, v T) OpQueryOption[T, C] {
	return NewOpQueryOption(col, OpGte, v)
}

// NewLessEqualOption creates an OpQueryOption with operator OpLte.
func NewLessEqualOption[T any, C Column[T] | PtrColumn[T]](col C, v T) OpQueryOption[T, C] {
	return NewOpQueryOption(col, OpLte, v)
}

func (opt OpQueryOption[T, C]) QueryOp() QueryOp {
	return opt.Op
}

// RangeQueryOptionInterface represents a query that find data from a given range of values.
type RangeQueryOptionInterface interface {
	ValuesOptionInterface
}

// RangeQueryOption implements the RangeQueryOptionInterface.
type RangeQueryOption[T any, C Column[T] | PtrColumn[T]] struct {
	ValuesOption[T, C]
}

// NewRangeQueryOption creates a new RangeQueryOption.
func NewRangeQueryOption[T any, C Column[T] | PtrColumn[T]](col C, vs []T) RangeQueryOption[T, C] {
	return RangeQueryOption[T, C]{
		ValuesOption: NewValuesOption(col, vs),
	}
}

// FuzzyQueryOptionInterface represents a query that find data that match given patterns approximately.
type FuzzyQueryOptionInterface interface {
	ValuesOptionInterface
}

// FuzzyQueryOption implements the FuzzyQueryOptionInterface.
type FuzzyQueryOption[T any, C Column[T] | PtrColumn[T]] struct {
	ValuesOption[T, C]
}

// NewFuzzyQueryOption creates a new FuzzyQueryOption.
func NewFuzzyQueryOption[T any, C Column[T] | PtrColumn[T]](col C, vs []T) FuzzyQueryOption[T, C] {
	return FuzzyQueryOption[T, C]{
		ValuesOption: NewValuesOption(col, vs),
	}
}

// UpdateOptionInterface represents an update operation that updates the target column with given value.
type UpdateOptionInterface interface {
	OptionInterface
}

// UpdateOption implements the UpdateOptionInterface.
type UpdateOption[T any, C Column[T] | PtrColumn[T]] struct {
	Option[T, C]
}

// NewUpdateOption creates a new UpdateOption.
func NewUpdateOption[T any, C Column[T] | PtrColumn[T]](col C, v T) UpdateOption[T, C] {
	return UpdateOption[T, C]{
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
	TargetColumnName() string
	SortOrder() SortOrder
}

// SortOption implements the SortOptionInterface.
type SortOption[T comparable, C Column[T] | PtrColumn[T]] struct {
	Column C
	Order  SortOrder
}

// NewSortOption creates a new SortOption.
func NewSortOption[T comparable, C Column[T] | PtrColumn[T]](col C, order SortOrder) SortOption[T, C] {
	return SortOption[T, C]{
		Column: col,
		Order:  order,
	}
}

func (opt SortOption[T, C]) TargetColumnName() string {
	return (any)(opt.Column).(ColumnGetter).GetColumnName()
}

func (opt SortOption[T, C]) SortOrder() SortOrder {
	return opt.Order
}

// ListOptions contains options and parameters that related to data listing.
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

type ColumnGetter interface {
	GetColumnName() string
}

type ColumnName string

func (cn ColumnName) GetColumnName() string {
	return string(cn)
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

// PtrColumn is used when declaring models with pointer fields, for example:
/*
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
