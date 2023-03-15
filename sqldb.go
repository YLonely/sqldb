package sqldb

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/samber/lo"
	"gorm.io/gorm/clause"
	gormschema "gorm.io/gorm/schema"

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
	// GetTargetColumn returns the column the operation processes against.
	GetTargetColumn() ColumnName
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

func (opt Option[T]) GetTargetColumn() ColumnName {
	return opt.Column
}

func (opt Option[T]) GetValue() any {
	return opt.Value
}

// ValuesOptionInterface wraps basic method of options which carry multiple values.
type ValuesOptionInterface interface {
	// GetTargetColumn returns the column the operation processes against.
	GetTargetColumn() ColumnName
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

func (opt ValuesOption[T]) GetTargetColumn() ColumnName {
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
	GetTargetColumn() ColumnName
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

func (opt SortOption[T]) GetTargetColumn() ColumnName {
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
	setColumnName(table, name string)
}

type ColumnGetter interface {
	GetColumnName() ColumnName
}

type ColumnName struct {
	table string
	Name  string
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

type JoinOptions struct {
	SelectedColumns []ColumnName
	Conditions      []OpJoinOptionInterface
}

func NewJoinOptions(selected []ColumnGetter, conditions []OpJoinOptionInterface) JoinOptions {
	return JoinOptions{
		SelectedColumns: lo.Map(selected, func(cg ColumnGetter, _ int) ColumnName { return cg.GetColumnName() }),
		Conditions:      conditions,
	}
}

type OpJoinOptionInterface interface {
	GetLeftTargetColumn() ColumnName
	GetRightTargetColumn() ColumnName
	QueryOp() QueryOp
}

type OpJoinOption struct {
	Left, Right ColumnName
	Op          QueryOp
}

func (opt OpJoinOption) GetLeftTargetColumn() ColumnName {
	return opt.Left
}

func (opt OpJoinOption) GetRightTargetColumn() ColumnName {
	return opt.Right
}

func (opt OpJoinOption) QueryOp() QueryOp {
	return opt.Op
}

func NewOpJoinOption[T any, C ColumnType[T]](left C, op QueryOp, right C) OpJoinOption {
	return OpJoinOption{
		Left:  any(left).(ColumnGetter).GetColumnName(),
		Right: any(right).(ColumnGetter).GetColumnName(),
		Op:    op,
	}
}

func NewEqualJoinOption[T any, C ColumnType[T]](left, right C) OpJoinOption {
	return NewOpJoinOption[T](left, OpEq, right)
}

func NewNotEqualJoinOption[T any, C ColumnType[T]](left, right C) OpJoinOption {
	return NewOpJoinOption[T](left, OpNe, right)
}

func NewGreaterJoinOption[T any, C ColumnType[T]](left, right C) OpJoinOption {
	return NewOpJoinOption[T](left, OpGt, right)
}

func NewLessJoinOption[T any, C ColumnType[T]](left, right C) OpJoinOption {
	return NewOpJoinOption[T](left, OpLt, right)
}

func NewGreaterEqualJoinOption[T any, C ColumnType[T]](left, right C) OpJoinOption {
	return NewOpJoinOption[T](left, OpGte, right)
}

func NewLessEqualJoinOption[T any, C ColumnType[T]](left, right C) OpJoinOption {
	return NewOpJoinOption[T](left, OpLte, right)
}
