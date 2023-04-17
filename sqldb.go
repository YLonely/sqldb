package sqldb

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/samber/lo"
	"github.com/samber/mo"
	"gorm.io/gorm/clause"
	gormschema "gorm.io/gorm/schema"

	"github.com/YLonely/sqldb/internal/sql"
)

type FilterOptionType string

const (
	FilterOptionTypeOpQuery    FilterOptionType = "OpQuery"
	FilterOptionTypeRangeQuery FilterOptionType = "RangeQuery"
	FilterOptionTypeFuzzyQuery FilterOptionType = "FuzzyQuery"
)

type FilterOption interface {
	GetFilterOptionType() FilterOptionType
}

type QueryOp string

const (
	OpEq  QueryOp = "="
	OpNe  QueryOp = "!="
	OpGt  QueryOp = ">"
	OpLt  QueryOp = "<"
	OpGte QueryOp = ">="
	OpLte QueryOp = "<="
)

// Option wraps basic methods of options.
type Option interface {
	ColumnNameGetter
	// GetValue returns the value the option carries. It is used by the operation to query or update the target column.
	GetValue() any
}

// option implements the Option interface.
type option[T any] struct {
	name  ColumnName
	value T
}

func newOption[T any](name ColumnName, value T) option[T] {
	return option[T]{
		name:  name,
		value: value,
	}
}

func (opt option[T]) GetColumnName() ColumnName {
	return opt.name
}

func (opt option[T]) GetValue() any {
	return opt.value
}

// ValuesOption wraps basic method of options which carry multiple values.
type ValuesOption interface {
	ColumnNameGetter
	// GetValues returns the values the option carries. Those values are used to query data.
	GetValues() []any
}

// valuesOption implements the ValuesOption interface.
type valuesOption[T any] struct {
	name   ColumnName
	values []T
}

func newValuesOption[T any](name ColumnName, values []T) valuesOption[T] {
	return valuesOption[T]{
		name:   name,
		values: values,
	}
}

func (opt valuesOption[T]) GetColumnName() ColumnName {
	return opt.name
}

func (opt valuesOption[T]) GetValues() []any {
	return lo.ToAnySlice(opt.values)
}

type JoinOptions struct {
	SelectedColumns []ColumnNameGetter
	Conditions      []OpOption
}

func NewJoinOptions(selectedColumns []ColumnNameGetter, conditions ...OpOption) JoinOptions {
	return JoinOptions{
		SelectedColumns: selectedColumns,
		Conditions:      conditions,
	}
}

type OpJoinOption interface {
	GetLeftColumnName() ColumnName
	GetRightColumnName() ColumnName
	QueryOp() QueryOp
}

func newOpJoinOption(left, right ColumnName, op QueryOp) opJoinOption {
	return opJoinOption{
		left:  left,
		right: right,
		op:    op,
	}
}

type opJoinOption struct {
	left, right ColumnName
	op          QueryOp
}

func (opt opJoinOption) GetLeftColumnName() ColumnName {
	return opt.left
}

func (opt opJoinOption) GetRightColumnName() ColumnName {
	return opt.right
}

func (opt opJoinOption) QueryOp() QueryOp {
	return opt.op
}

// OpQueryOption represents a query which use the given query operator to search data.
type OpQueryOption interface {
	Option
	QueryOp() QueryOp
}

// opQueryOption implements the OpQueryOption interface.
type opQueryOption[T any] struct {
	option[T]
	op QueryOp
}

func newOpQueryOption[T any](name ColumnName, v T, op QueryOp) opQueryOption[T] {
	return opQueryOption[T]{
		option: newOption(name, v),
		op:     op,
	}
}

func (opt opQueryOption[T]) QueryOp() QueryOp {
	return opt.op
}

func (opt opQueryOption[T]) GetFilterOptionType() FilterOptionType {
	return FilterOptionTypeOpQuery
}

type OpOption struct {
	mo.Either[OpJoinOption, OpQueryOption]
}

func newOpOption[T any](v any, op QueryOp, name ColumnName) OpOption {
	var (
		rv         = reflect.ValueOf(v)
		columnType bool
	)
	if valuer, ok := v.(interface{ reflectValue() reflect.Value }); ok {
		rv = valuer.reflectValue()
		columnType = true
	}
	dest := reflect.TypeOf(*new(T))
	if !rv.CanConvert(dest) {
		panic(fmt.Sprintf("Value of type %s can not convert to type %s", rv.Type().String(), dest.String()))
	}
	if columnType {
		rightName := v.(ColumnNameGetter).GetColumnName()
		return OpOption{
			Either: mo.Left[OpJoinOption, OpQueryOption](newOpJoinOption(name, rightName, op)),
		}
	}
	return OpOption{
		Either: mo.Right[OpJoinOption, OpQueryOption](newOpQueryOption(name, rv.Convert(dest).Interface().(T), op)),
	}
}

func (opt OpOption) GetFilterOptionType() FilterOptionType {
	return opt.MustRight().(FilterOption).GetFilterOptionType()
}

// RangeQueryOption represents a query that find data from a given range of values.
type RangeQueryOption interface {
	ValuesOption
	Exclude() bool
}

// rangeQueryOption implements the RangeQueryOption interface.
type rangeQueryOption[T any] struct {
	valuesOption[T]
	exclude bool
}

func newRangeQueryOption[T any](name ColumnName, values []T, exclude bool) rangeQueryOption[T] {
	return rangeQueryOption[T]{
		valuesOption: newValuesOption(name, values),
		exclude:      exclude,
	}
}

func (opt rangeQueryOption[T]) Exclude() bool {
	return opt.exclude
}

func (opt rangeQueryOption[T]) GetFilterOptionType() FilterOptionType {
	return FilterOptionTypeRangeQuery
}

// FuzzyQueryOption represents a query that find data that match given patterns approximately.
type FuzzyQueryOption interface {
	ValuesOption
}

// fuzzyQueryOption implements the FuzzyQueryOption.
type fuzzyQueryOption[T any] struct {
	valuesOption[T]
}

func newFuzzyQueryOption[T any](name ColumnName, values []T) fuzzyQueryOption[T] {
	return fuzzyQueryOption[T]{
		valuesOption: newValuesOption(name, values),
	}
}

func (opt fuzzyQueryOption[T]) GetFilterOptionType() FilterOptionType {
	return FilterOptionTypeFuzzyQuery
}

// UpdateOption represents an update operation that updates the target column with given value.
type UpdateOption interface {
	Option
}

// updateOption implements the UpdateOption.
type updateOption[T any] struct {
	option[T]
}

func newUpdateOption[T any](name ColumnName, value T) updateOption[T] {
	return updateOption[T]{
		option: newOption(name, value),
	}
}

type SortOrder string

const (
	SortOrderAscending  SortOrder = "asc"
	SortOrderDescending SortOrder = "desc"
)

// SortOption represents an sort operation.
type SortOption interface {
	ColumnNameGetter
	GetSortOrder() SortOrder
}

// sortOption implements the SortOptionInterface.
type sortOption struct {
	name  ColumnName
	order SortOrder
}

func (opt sortOption) GetColumnName() ColumnName {
	return opt.name
}

func (opt sortOption) GetSortOrder() SortOrder {
	return opt.order
}

// ListOptions contains options and parameters that related to data listing.
type ListOptions struct {
	Offset      uint64
	Limit       uint64
	SortOptions []SortOption
}

// columnNameSetter sets the column name of a filed
type columnNameSetter interface {
	setColumnName(table, name string)
}

type ColumnNameGetter interface {
	GetColumnName() ColumnName
}

type ColumnName struct {
	table string
	Name  string
}

func (cn ColumnName) Sort(order SortOrder) sortOption {
	return sortOption{
		name:  cn,
		order: order,
	}
}

func NewColumnName(name string) ColumnName {
	return ColumnName{Name: name}
}

func (cn ColumnName) String() string {
	return cn.Name
}

func (cn ColumnName) Full() string {
	return lo.Ternary(cn.table == "", cn.Name, fmt.Sprintf("%s.%s", cn.table, cn.Name))
}

func (cn ColumnName) GetColumnName() ColumnName {
	return cn
}

func (cn *ColumnName) setColumnName(table, name string) {
	cn.table = table
	cn.Name = name
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
func (cv ColumnValue[T]) CreateClauses(f *gormschema.Field) []clause.Interface {
	if fc, ok := any(cv.V).(gormschema.CreateClausesInterface); ok {
		return fc.CreateClauses(f)
	}
	return nil
}

// QueryClauses implements the QueryClausesInterface interface from GORM.
func (cv ColumnValue[T]) QueryClauses(f *gormschema.Field) []clause.Interface {
	if fc, ok := any(cv.V).(gormschema.QueryClausesInterface); ok {
		return fc.QueryClauses(f)
	}
	return nil
}

// UpdateClauses implements the UpdateClausesInterface interface from GORM.
func (cv ColumnValue[T]) UpdateClauses(f *gormschema.Field) []clause.Interface {
	if fc, ok := any(cv.V).(gormschema.UpdateClausesInterface); ok {
		return fc.UpdateClauses(f)
	}
	return nil
}

// DeleteClauses implements the DeleteClausesInterface interface from GORM.
func (cv ColumnValue[T]) DeleteClauses(f *gormschema.Field) []clause.Interface {
	if fc, ok := any(cv.V).(gormschema.DeleteClausesInterface); ok {
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

func (c PtrColumn[T]) reflectValue() reflect.Value {
	return reflect.ValueOf(*new(T))
}

func (c PtrColumn[T]) EQ(value any) OpOption {
	return newOpOption[T](value, OpEq, c.ColumnName)
}

func (c PtrColumn[T]) NE(value any) OpOption {
	return newOpOption[T](value, OpNe, c.ColumnName)
}

func (c PtrColumn[T]) GT(value any) OpOption {
	return newOpOption[T](value, OpGt, c.ColumnName)
}

func (c PtrColumn[T]) LT(value any) OpOption {
	return newOpOption[T](value, OpLt, c.ColumnName)
}

func (c PtrColumn[T]) GTE(value any) OpOption {
	return newOpOption[T](value, OpGte, c.ColumnName)
}

func (c PtrColumn[T]) LTE(value any) OpOption {
	return newOpOption[T](value, OpLte, c.ColumnName)
}

func (c PtrColumn[T]) In(values []T) rangeQueryOption[T] {
	return newRangeQueryOption(c.ColumnName, values, false)
}

func (c PtrColumn[T]) NotIn(values []T) rangeQueryOption[T] {
	return newRangeQueryOption(c.ColumnName, values, true)
}

func (c PtrColumn[T]) FuzzyIn(values []T) fuzzyQueryOption[T] {
	return newFuzzyQueryOption(c.ColumnName, values)
}

func (c PtrColumn[T]) Update(value T) updateOption[T] {
	return newUpdateOption(c.ColumnName, value)
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

func (c Column[T]) reflectValue() reflect.Value {
	return reflect.ValueOf(*new(T))
}

func (c Column[T]) EQ(value any) OpOption {
	return newOpOption[T](value, OpEq, c.ColumnName)
}

func (c Column[T]) NE(value any) OpOption {
	return newOpOption[T](value, OpNe, c.ColumnName)
}

func (c Column[T]) GT(value any) OpOption {
	return newOpOption[T](value, OpGt, c.ColumnName)
}

func (c Column[T]) LT(value any) OpOption {
	return newOpOption[T](value, OpLt, c.ColumnName)
}

func (c Column[T]) GTE(value any) OpOption {
	return newOpOption[T](value, OpGte, c.ColumnName)
}

func (c Column[T]) LTE(value any) OpOption {
	return newOpOption[T](value, OpLte, c.ColumnName)
}

func (c Column[T]) In(values []T) rangeQueryOption[T] {
	return newRangeQueryOption(c.ColumnName, values, false)
}

func (c Column[T]) NotIn(values []T) rangeQueryOption[T] {
	return newRangeQueryOption(c.ColumnName, values, true)
}

func (c Column[T]) FuzzyIn(values []T) fuzzyQueryOption[T] {
	return newFuzzyQueryOption(c.ColumnName, values)
}

func (c Column[T]) Update(value T) updateOption[T] {
	return newUpdateOption(c.ColumnName, value)
}

// NewColumn creates a new Column of type T.
func NewColumn[T any](v T) Column[T] {
	return Column[T]{
		ColumnValue: ColumnValue[T]{
			V: v,
		},
	}
}
