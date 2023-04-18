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

func NewOpJoinOption(left ColumnName, op QueryOp, right ColumnName) OpOption {
	return OpOption{
		Either: mo.Left[OpJoinOption, OpQueryOption](opJoinOption{
			left:  left,
			right: right,
			op:    op,
		}),
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
	FilterOption
	QueryOp() QueryOp
}

// opQueryOption implements the OpQueryOption interface.
type opQueryOption[T any] struct {
	option[T]
	op QueryOp
}

func NewOpQueryOption[T any](name ColumnName, op QueryOp, v T) OpOption {
	return OpOption{
		Either: mo.Right[OpJoinOption, OpQueryOption](
			opQueryOption[T]{
				option: newOption(name, v),
				op:     op,
			}),
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

func (opt OpOption) GetFilterOptionType() FilterOptionType {
	return opt.MustRight().(FilterOption).GetFilterOptionType()
}

// RangeQueryOption represents a query that find data from a given range of values.
type RangeQueryOption interface {
	ValuesOption
	FilterOption
	Exclude() bool
}

// rangeQueryOption implements the RangeQueryOption interface.
type rangeQueryOption[T any] struct {
	valuesOption[T]
	exclude bool
}

func NewRangeQueryOption[T any](name ColumnName, values []T, exclude bool) RangeQueryOption {
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
	FilterOption
	ValuesOption
}

// fuzzyQueryOption implements the FuzzyQueryOption.
type fuzzyQueryOption[T any] struct {
	valuesOption[T]
}

func NewFuzzyQueryOption[T any](name ColumnName, values []T) FuzzyQueryOption {
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

func NewUpdateOption[T any](name ColumnName, value T) UpdateOption {
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

func NewSortOption(name ColumnName, order SortOrder) SortOption {
	return sortOption{
		name:  name,
		order: order,
	}
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

func (cv ColumnValue[T]) reflectType() reflect.Type {
	return cv.reflectValue().Type()
}

func (cv ColumnValue[T]) reflectValue() reflect.Value {
	rv := reflect.ValueOf(*new(T))
	if rv.Kind() == reflect.Ptr {
		return reflect.ValueOf(*new(*T))
	}
	return rv
}

func (cv ColumnValue[T]) convertFrom(v any) (res T, err error) {
	var (
		rt  = cv.reflectType()
		rrv = reflect.ValueOf(v)
	)
	if valuer, ok := v.(interface{ reflectValue() reflect.Value }); ok {
		rrv = valuer.reflectValue()
	}
	if !rrv.CanConvert(rt) {
		err = fmt.Errorf("unable to convert value of type %s to the column type %s", rrv.Type(), rt)
		return
	}
	res = rrv.Convert(rt).Interface().(T)
	return
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

type columnBase[T any] struct {
	ColumnValue[T]
	ColumnName
}

func (c columnBase[T]) buildOpOption(value any, op QueryOp) (OpOption, error) {
	v, err := c.convertFrom(value)
	if err != nil {
		return OpOption{}, fmt.Errorf("failed to build query options for the column %s: %w", c.ColumnName, err)
	}
	if getter, ok := value.(ColumnNameGetter); ok {
		return NewOpJoinOption(c.ColumnName, op, getter.GetColumnName()), nil
	}
	return NewOpQueryOption(c.ColumnName, op, v), nil
}

func (c columnBase[T]) EQ(value any) OpOption {
	return lo.Must(c.buildOpOption(value, OpEq))
}

func (c columnBase[T]) NE(value any) OpOption {
	return lo.Must(c.buildOpOption(value, OpNe))
}

func (c columnBase[T]) GT(value any) OpOption {
	return lo.Must(c.buildOpOption(value, OpGt))
}

func (c columnBase[T]) LT(value any) OpOption {
	return lo.Must(c.buildOpOption(value, OpLt))
}

func (c columnBase[T]) GTE(value any) OpOption {
	return lo.Must(c.buildOpOption(value, OpGte))
}

func (c columnBase[T]) LTE(value any) OpOption {
	return lo.Must(c.buildOpOption(value, OpLte))
}

func (c columnBase[T]) In(values []T) RangeQueryOption {
	return NewRangeQueryOption(c.ColumnName, values, false)
}

func (c columnBase[T]) NotIn(values []T) RangeQueryOption {
	return NewRangeQueryOption(c.ColumnName, values, true)
}

func (c columnBase[T]) FuzzyIn(values []T) FuzzyQueryOption {
	return NewFuzzyQueryOption(c.ColumnName, values)
}

func (c columnBase[T]) Update(value any) UpdateOption {
	return NewUpdateOption(c.ColumnName, lo.Must(c.convertFrom(value)))
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
	columnBase[*T]
}

// NewPtrColumn creates a new PtrColumn of type T.
func NewPtrColumn[T any](v T) PtrColumn[T] {
	return PtrColumn[T]{
		columnBase: columnBase[*T]{
			ColumnValue: ColumnValue[*T]{
				V: &v,
			},
		},
	}
}

func (c PtrColumn[T]) In(values []T) RangeQueryOption {
	return NewRangeQueryOption(c.ColumnName, values, false)
}

func (c PtrColumn[T]) NotIn(values []T) RangeQueryOption {
	return NewRangeQueryOption(c.ColumnName, values, true)
}

func (c PtrColumn[T]) FuzzyIn(values []T) FuzzyQueryOption {
	return NewFuzzyQueryOption(c.ColumnName, values)
}

// Column represents a column of a table.
type Column[T any] struct {
	columnBase[T]
}

// NewColumn creates a new Column of type T.
func NewColumn[T any](v T) Column[T] {
	return Column[T]{
		columnBase: columnBase[T]{
			ColumnValue: ColumnValue[T]{
				V: v,
			},
		},
	}
}
