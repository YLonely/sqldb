package sqldb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/samber/lo"
	"github.com/samber/mo"
	"gorm.io/gorm"
	gormschema "gorm.io/gorm/schema"
)

// A TransactionFunc starts a transaction.
type TransactionFunc func(ctx context.Context, run func(context.Context) error) error

type contextKey int

const (
	transactionContextKey contextKey = iota
)

func WithTransaction(ctx context.Context, tx *gorm.DB) context.Context {
	return context.WithValue(ctx, transactionContextKey, tx)
}

func TransactionFrom(ctx context.Context) *gorm.DB {
	if obj := ctx.Value(transactionContextKey); obj != nil {
		return obj.(*gorm.DB)
	}
	return nil
}

// NewTransactionFunc returns a TransactionFunc.
func NewTransactionFunc(db *gorm.DB) TransactionFunc {
	return func(ctx context.Context, run func(context.Context) error) error {
		if tx := TransactionFrom(ctx); tx != nil {
			return tx.Transaction(func(tx *gorm.DB) error {
				return run(WithTransaction(ctx, tx))
			})
		}
		return db.Transaction(func(tx *gorm.DB) error {
			return run(WithTransaction(ctx, tx))
		})
	}
}

// Model is an interface defines commonly used methods to manipulate data.
type Model[T any] interface {
	// DB returns the db instance.
	DB(context.Context) *gorm.DB
	// Table returns the table name in the database.
	Table() string
	// Columns returns a instance of type T,
	// all fields of type sqldb.Column[U] in the instance are populated with corresponding column name.
	Columns() T
	// ColumnNames returns all column names the model has.
	ColumnNames() []ColumnNameGetter
	// Create creates an new entity of type T.
	Create(ctx context.Context, entity *T) error
	Query(queries ...FilterOption) Executor[T]
}

// Executor is an interface wraps operations related to db queries.
type Executor[T any] interface {
	Get(ctx context.Context) (T, error)
	List(ctx context.Context, opts ListOptions) ([]T, uint64, error)
	Update(ctx context.Context, opts ...UpdateOption) (uint64, error)
	Delete(ctx context.Context) error
}

// model implements the Model interface.
type model[T any] struct {
	columns           *T
	columnSerializers map[string]serializer
	fieldPathToColumn map[string]ColumnNameGetter
	tableName         string
	joined            bool
	config            modelConfig

	db *gorm.DB
}

var _ Model[struct{}] = model[struct{}]{}

type executor[T any] struct {
	model[T]

	queries []FilterOption
}

var (
	serializers = map[string]serializer{
		"json": jsonSerializer{},
	}
)

type modelConfig struct {
	dbInitialFunc func(*gorm.DB) *gorm.DB
}

type ModelOption func(*modelConfig)

func WithDBInitialFunc(initial func(*gorm.DB) *gorm.DB) ModelOption {
	return func(c *modelConfig) {
		c.dbInitialFunc = initial
	}
}

// NewModel returns a new Model.
func NewModel[T any](db *gorm.DB, opts ...ModelOption) Model[T] {
	var (
		m                 = new(T)
		serializers       = map[string]serializer{}
		fieldPathToColumn = map[string]ColumnNameGetter{}
		tableName         string
		leftTableName     string
		rightTableName    string
		cfg               modelConfig
	)
	for _, opt := range opts {
		opt(&cfg)
	}

	rt := reflect.TypeOf(m).Elem()
	if rt.Kind() != reflect.Struct {
		panic(fmt.Errorf("%s is not a struct", rt.String()))
	}
	joinResult, joined := any(m).(joinResultInterface)
	if joined {
		tableName = joinResult._tableName()
		leftTableName = db.NamingStrategy.TableName(reflect.TypeOf(joinResult._left()).Name())
		rightTableName = db.NamingStrategy.TableName(reflect.TypeOf(joinResult._right()).Name())
	} else {
		tableName = db.NamingStrategy.TableName(rt.Name())
	}
	if err := iterateFields(m, func(fieldAddr reflect.Value, path []reflect.StructField) (bool, error) {
		var (
			fieldInterface = fieldAddr.Interface()
			fieldNames     = lo.Map(path, func(sf reflect.StructField, _ int) string { return sf.Name })
			table          = tableName
		)
		if joined {
			if lo.Contains(fieldNames, "Left") {
				table = leftTableName
			} else {
				table = rightTableName
			}
		}

		if setter, ok := fieldInterface.(columnNameSetter); ok {
			name, s := parseColumn(db, path)
			if joined {
				setter.setColumnName("", fmt.Sprintf("%s.%s", table, name))
			} else {
				setter.setColumnName(table, name)
			}
			cg := fieldInterface.(ColumnNameGetter)
			if s != nil {
				serializers[cg.GetColumnName().String()] = s
			}
			fieldPathToColumn[strings.Join(fieldNames, ".")] = cg
			return false, nil
		}
		return true, nil
	}); err != nil {
		panic(err)
	}

	return model[T]{
		columns:           m,
		columnSerializers: serializers,
		db:                db,
		fieldPathToColumn: fieldPathToColumn,
		tableName:         tableName,
		joined:            joined,
		config:            cfg,
	}
}

func parseColumn(db *gorm.DB, path []reflect.StructField) (string, serializer) {
	var (
		l              = len(path)
		sf, parents    = path[l-1], path[:l-1]
		tagSettings    = gormschema.ParseTagSetting(sf.Tag.Get("gorm"), ";")
		column         = tagSettings["COLUMN"]
		serializerName = tagSettings["SERIALIZER"]
		serializer     serializer
		prefix         string
	)
	if column == "" {
		column = db.NamingStrategy.ColumnName("", sf.Name)
	}

	for _, pf := range parents {
		tagSettings := gormschema.ParseTagSetting(pf.Tag.Get("gorm"), ";")
		if p := tagSettings["EMBEDDEDPREFIX"]; p != "" && (tagSettings["EMBEDDED"] != "" || pf.Anonymous) {
			prefix += p
		}
	}
	column = prefix + column

	if serializerName != "" {
		if s, exist := serializers[serializerName]; exist {
			serializer = s
		} else {
			panic(fmt.Errorf("unsupported serializer %s", serializerName))
		}
	}
	return column, serializer
}

func (m model[T]) DB(ctx context.Context) *gorm.DB {
	var db *gorm.DB
	if tx := TransactionFrom(ctx); tx != nil {
		db = tx.WithContext(ctx)
	} else {
		db = m.db.WithContext(ctx)
	}
	if m.config.dbInitialFunc != nil {
		db = m.config.dbInitialFunc(db)
	}
	return db
}

func (m model[T]) Table() string {
	return m.tableName
}

func (m model[T]) ColumnNames() []ColumnNameGetter {
	return lo.Values(m.fieldPathToColumn)
}

func (m model[T]) Columns() T {
	return *m.columns
}

func (m model[T]) Create(ctx context.Context, entity *T) error {
	return m.DB(ctx).Create(entity).Error
}

func (m model[T]) Query(queries ...FilterOption) Executor[T] {
	return executor[T]{
		model:   m,
		queries: queries,
	}
}

func (e executor[T]) Update(ctx context.Context, opts ...UpdateOption) (uint64, error) {
	if len(opts) == 0 {
		return 0, errors.New("empty options")
	}
	updateMap := map[string]any{}
	for _, opt := range opts {
		column := getColumnName(e.joined, opt)
		v, err := e.serialize(ctx, column, opt.GetValue())
		if err != nil {
			return 0, err
		}
		updateMap[column] = v
	}
	h := newApplyHelper(e.DB(ctx), e.joined, e.serialize).applyFilterOptions(ctx, e.queries)
	if h.Result().IsError() {
		return 0, h.Result().Error()
	}
	updated := h.Result().MustGet().Model(new(T)).Updates(updateMap)
	return uint64(updated.RowsAffected), updated.Error
}

func (e executor[T]) Delete(ctx context.Context) error {
	h := newApplyHelper(e.DB(ctx), e.joined, e.serialize).applyFilterOptions(ctx, e.queries)
	if h.Result().IsError() {
		return h.Result().Error()
	}
	return h.Result().MustGet().Delete(new(T)).Error
}

func (e executor[T]) Get(ctx context.Context) (T, error) {
	h := newApplyHelper(lo.TernaryF(e.joined,
		func() *gorm.DB { return e.DB(ctx) },
		func() *gorm.DB { return e.DB(ctx).Model(new(T)) },
	), e.joined, e.serialize).applyFilterOptions(ctx, e.queries)
	if h.Result().IsError() {
		return lo.Empty[T](), h.Result().Error()
	}
	db := h.Result().MustGet()
	if e.joined {
		var values map[string]any
		if err := db.Take(&values).Error; err != nil {
			return lo.Empty[T](), err
		}
		return e.scan(ctx, values)
	}
	var entity T
	return entity, db.First(&entity).Error
}

func (e executor[T]) List(ctx context.Context, opts ListOptions) (entities []T, total uint64, err error) {
	var t int64
	h := newApplyHelper(lo.TernaryF(e.joined,
		func() *gorm.DB { return e.DB(ctx) },
		func() *gorm.DB { return e.DB(ctx).Model(new(T)) },
	), e.joined, e.serialize).applyFilterOptions(ctx, e.queries)
	if h.Result().IsError() {
		err = h.Result().Error()
		return
	}
	db := h.Result().MustGet()
	if err = db.Count(&t).Error; err != nil {
		return
	}
	total = uint64(t)
	if opts.Limit != 0 {
		db = db.Limit(int(opts.Limit))
	}
	if opts.Offset != 0 {
		db = db.Offset(int(opts.Offset))
	}

	for _, opt := range opts.SortOptions {
		db = db.Order(fmt.Sprintf("%s %s", getColumnName(e.joined, opt), opt.GetSortOrder()))
	}

	if e.joined {
		var valuesList []map[string]any
		if err = db.Find(&valuesList).Error; err != nil {
			return
		}
		entities, err = MapErr(valuesList, func(values map[string]any, _ int) (T, error) {
			return e.scan(ctx, values)
		})
		return
	}
	err = db.Find(&entities).Error
	return
}

func (e executor[T]) serialize(ctx context.Context, column string, v any) (any, error) {
	value := v
	if s, exist := e.columnSerializers[column]; exist {
		v, err := s.value(ctx, v)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize the value of the column %s: %w", column, err)
		}
		value = v
	}
	return value, nil
}

func (e executor[T]) scan(ctx context.Context, values map[string]any) (T, error) {
	target := *e.columns
	if err := iterateFields(&target, func(fieldAddr reflect.Value, path []reflect.StructField) (bool, error) {
		fieldPath := strings.Join(lo.Map(path, func(sf reflect.StructField, _ int) string { return sf.Name }), ".")
		if cg, exist := e.fieldPathToColumn[fieldPath]; exist {
			columnName := cg.GetColumnName().String()
			v := values[columnName]
			if v == nil {
				return false, nil
			}
			var err error
			if s, exist := e.columnSerializers[columnName]; exist {
				err = s.scan(ctx, fieldAddr.Interface(), v)
			} else {
				err = fieldAddr.Interface().(interface{ Scan(any) error }).Scan(v)
			}
			if err != nil {
				return false, fmt.Errorf("failed to scan value %v into field %s: %w", v, fieldPath, err)
			}
			return false, nil
		}
		return true, nil
	}); err != nil {
		return lo.Empty[T](), err
	}
	return target, nil
}

type applyHelper struct {
	db        mo.Result[*gorm.DB]
	serialize func(context.Context, string, any) (any, error)
	joined    bool
}

func newApplyHelper(db *gorm.DB, joined bool, s func(context.Context, string, any) (any, error)) *applyHelper {
	return &applyHelper{db: mo.Ok(db), serialize: s, joined: joined}
}

func (h *applyHelper) Result() mo.Result[*gorm.DB] {
	return h.db
}

func (h *applyHelper) applyFilterOptions(ctx context.Context, opts []FilterOption) *applyHelper {
	filterOpts := parseFilterOptions(opts)
	return h.applyOpQueryOptions(ctx, filterOpts.opQueryOptions).
		applyRangeQueryOptions(ctx, filterOpts.rangeQueryOptions).
		applyFuzzyQueryOptions(ctx, filterOpts.fuzzyQueryOptions)
}

func (h *applyHelper) applyOpQueryOptions(ctx context.Context, opts []OpQueryOption) *applyHelper {
	if len(opts) == 0 {
		return h
	}
	query := strings.Join(lo.Map(opts, func(opt OpQueryOption, _ int) string {
		if opt.QueryOp() == "" {
			panic("Op must be provided in IsQueryOption")
		}
		return fmt.Sprintf("%s %s ?", getColumnName(h.joined, opt), opt.QueryOp())
	}), " AND ")
	h.db = h.db.Map(func(db *gorm.DB) (*gorm.DB, error) {
		values, err := MapErr(opts, func(opt OpQueryOption, _ int) (any, error) {
			return h.serialize(ctx, getColumnName(h.joined, opt), opt.GetValue())
		})
		if err != nil {
			return nil, err
		}
		return db.Where(query, values...), nil
	})
	return h
}

func (h *applyHelper) applyRangeQueryOptions(ctx context.Context, opts []RangeQueryOption) *applyHelper {
	if len(opts) == 0 {
		return h
	}
	query := strings.Join(lo.Map(opts, func(opt RangeQueryOption, _ int) string {
		return fmt.Sprintf("%s %s (?)", getColumnName(h.joined, opt), lo.Ternary(opt.Exclude(), "NOT IN", "IN"))
	}), " AND ")
	h.db = h.db.Map(func(db *gorm.DB) (*gorm.DB, error) {
		values, err := MapErr(opts, func(opt RangeQueryOption, _ int) (any, error) {
			return MapErr(opt.GetValues(), func(v any, _ int) (any, error) {
				return h.serialize(ctx, getColumnName(h.joined, opt), v)
			})
		})
		if err != nil {
			return nil, err
		}
		return db.Where(query, values...), nil
	})
	return h
}

func (h *applyHelper) applyFuzzyQueryOptions(ctx context.Context, opts []FuzzyQueryOption) *applyHelper {
	if len(opts) == 0 {
		return h
	}
	lo.ForEach(opts, func(opt FuzzyQueryOption, _ int) {
		queries := lo.Map(opt.GetValues(), func(_ any, _ int) string {
			return fmt.Sprintf("%s LIKE ?", getColumnName(h.joined, opt))
		})
		values := lo.Map(opt.GetValues(), func(v any, _ int) any { return fmt.Sprintf("%%%v%%", v) })
		h.db = h.db.Map(func(db *gorm.DB) (*gorm.DB, error) {
			return db.Where(strings.Join(queries, " OR "), values...), nil
		})
	})
	return h
}

type filterOptions struct {
	opQueryOptions    []OpQueryOption
	rangeQueryOptions []RangeQueryOption
	fuzzyQueryOptions []FuzzyQueryOption
}

func parseFilterOptions(opts []FilterOption) filterOptions {
	res := filterOptions{}
	for _, opt := range opts {
		switch opt.GetFilterOptionType() {
		case FilterOptionTypeOpQuery:
			res.opQueryOptions = append(res.opQueryOptions, opt.(OpOption).MustRight())
		case FilterOptionTypeRangeQuery:
			res.rangeQueryOptions = append(res.rangeQueryOptions, any(opt).(RangeQueryOption))
		case FilterOptionTypeFuzzyQuery:
			res.fuzzyQueryOptions = append(res.fuzzyQueryOptions, any(opt).(FuzzyQueryOption))
		default:
			panic(fmt.Sprintf("Invalid filter option type %s", opt.GetFilterOptionType()))
		}
	}
	return res
}

func getColumnName(joined bool, opt ColumnNameGetter) string {
	cn := opt.GetColumnName()
	return lo.Ternary(joined, cn.Full(), cn.String())
}

type serializer interface {
	value(ctx context.Context, v any) (any, error)
	scan(ctx context.Context, dest, src any) error
}

type jsonSerializer struct{}

func (jsonSerializer) value(_ context.Context, v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}

func (jsonSerializer) scan(_ context.Context, dest, src any) error {
	var raw []byte
	switch v := src.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		return fmt.Errorf("unsupported value source %s", reflect.TypeOf(src).Name())
	}
	return json.Unmarshal(raw, dest)
}
