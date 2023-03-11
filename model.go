package sqldb

import (
	"context"
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
	// Columns returns a instance of type T,
	// all fields of type sqldb.Column[U] in the instance are populated with corresponding column name.
	Columns() T
	Create(ctx context.Context, entity *T) error
	Get(ctx context.Context, opts []OpQueryOptionInterface) (*T, error)
	List(ctx context.Context, opts ListOptions) ([]*T, uint64, error)
	Update(ctx context.Context, query FilterOptions, opts []UpdateOptionInterface) (uint64, error)
	Delete(ctx context.Context, opts FilterOptions) error
}

// model implements the Model interface.
type model[T any] struct {
	columns           *T
	columnSerializers map[string]columnSerializer

	db *gorm.DB
}

var _ Model[struct{}] = model[struct{}]{}

type columnSerializer func(context.Context, any) (any, error)

// NewModel returns a new Model.
func NewModel[T any](db *gorm.DB) Model[T] {
	var (
		m           = new(T)
		serializers = map[string]columnSerializer{}
	)
	elem := reflect.TypeOf(m).Elem()
	if elem.Kind() != reflect.Struct {
		panic(fmt.Errorf("%s is not a struct", elem.String()))
	}

	if err := iterateFields(m, func(fieldAddr reflect.Value, path []reflect.StructField) bool {
		if setter, ok := fieldAddr.Interface().(columnSetter); ok {
			name, s := parseColumn(db, path)
			setter.setColumnName(name)
			if s != nil {
				serializers[name] = s
			}
			return false
		}
		return true
	}); err != nil {
		panic(err)
	}

	return model[T]{
		columns:           m,
		columnSerializers: serializers,
		db:                db,
	}
}

func parseColumn(db *gorm.DB, path []reflect.StructField) (string, columnSerializer) {
	var (
		l              = len(path)
		sf, parents    = path[l-1], path[:l-1]
		tagSettings    = gormschema.ParseTagSetting(sf.Tag.Get("gorm"), ";")
		column         = tagSettings["COLUMN"]
		serializerName = tagSettings["SERIALIZER"]
		serializer     columnSerializer
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
		if s, exist := gormschema.GetSerializer(serializerName); exist {
			serializer = func(ctx context.Context, v any) (any, error) {
				return s.Value(ctx, &gormschema.Field{}, reflect.Value{}, v)
			}
		}
	}
	return column, serializer
}

func (m model[T]) dbInstance(ctx context.Context) *gorm.DB {
	if tx := TransactionFrom(ctx); tx != nil {
		return tx.WithContext(ctx)
	}
	return m.db.WithContext(ctx)
}

func (m model[T]) Columns() T {
	return *m.columns
}

func (m model[T]) Update(ctx context.Context, query FilterOptions, opts []UpdateOptionInterface) (uint64, error) {
	if len(opts) == 0 {
		return 0, errors.New("empty options")
	}
	updateMap := map[string]any{}
	for _, opt := range opts {
		column := string(opt.TargetColumnName())
		v, err := m.serialize(ctx, column, opt.GetValue())
		if err != nil {
			return 0, err
		}
		updateMap[column] = v
	}
	h := newApplyHelper(m.dbInstance(ctx), m.serialize).applyFilterOptions(ctx, query)
	if h.Result().IsError() {
		return 0, h.Result().Error()
	}
	updated := h.Result().MustGet().Model(new(T)).Updates(updateMap)
	return uint64(updated.RowsAffected), updated.Error
}

func (m model[T]) Delete(ctx context.Context, opts FilterOptions) error {
	h := newApplyHelper(m.dbInstance(ctx), m.serialize).applyFilterOptions(ctx, opts)
	if h.Result().IsError() {
		return h.Result().Error()
	}
	return h.Result().MustGet().Delete(new(T)).Error
}

func (m model[T]) Get(ctx context.Context, opts []OpQueryOptionInterface) (*T, error) {
	if len(opts) == 0 {
		return nil, errors.New("empty options")
	}
	entity := new(T)
	h := newApplyHelper(m.dbInstance(ctx), m.serialize).applyOpQueryOptions(ctx, opts)
	if h.Result().IsError() {
		return nil, h.Result().Error()
	}
	return entity, h.Result().MustGet().First(entity).Error
}

func (m model[T]) Create(ctx context.Context, entity *T) error {
	return m.dbInstance(ctx).Create(entity).Error
}

func (m model[T]) List(ctx context.Context, opts ListOptions) (entities []*T, total uint64, err error) {
	db := m.dbInstance(ctx).Model(new(T))
	var t int64
	h := newApplyHelper(db, m.serialize).applyFilterOptions(ctx, opts.FilterOptions)
	if h.Result().IsError() {
		err = h.Result().Error()
		return
	}
	db = h.Result().MustGet()
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
		db = db.Order(fmt.Sprintf("%s %s", opt.TargetColumnName(), opt.SortOrder()))
	}

	if err = db.Find(&entities).Error; err != nil {
		return
	}
	return
}

func (m model[T]) serialize(ctx context.Context, column string, v any) (any, error) {
	value := v
	if s, exist := m.columnSerializers[column]; exist {
		v, err := s(ctx, value)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize the value of the column %s: %w", column, err)
		}
		value = v
	}
	return value, nil
}

type applyHelper struct {
	db        mo.Result[*gorm.DB]
	serialize func(context.Context, string, any) (any, error)
}

func newApplyHelper(db *gorm.DB, s func(context.Context, string, any) (any, error)) *applyHelper {
	return &applyHelper{db: mo.Ok(db), serialize: s}
}

func (h *applyHelper) Result() mo.Result[*gorm.DB] {
	return h.db
}

func (h *applyHelper) applyFilterOptions(ctx context.Context, opts FilterOptions) *applyHelper {
	return h.applyOpQueryOptions(ctx, opts.OpOptions).
		applyRangeQueryOptions(ctx, "IN", opts.InOptions).
		applyRangeQueryOptions(ctx, "NOT IN", opts.NotInOptions).
		applyFuzzyQueryOptions(ctx, opts.FuzzyOptions)
}

func (h *applyHelper) applyOpQueryOptions(ctx context.Context, opts []OpQueryOptionInterface) *applyHelper {
	if len(opts) == 0 {
		return h
	}
	query := strings.Join(lo.Map(opts, func(opt OpQueryOptionInterface, _ int) string {
		if opt.QueryOp() == "" {
			panic("Op must be provided in IsQueryOption")
		}
		return fmt.Sprintf("%s %s ?", opt.TargetColumnName(), opt.QueryOp())
	}), " AND ")
	h.db = h.db.Map(func(db *gorm.DB) (*gorm.DB, error) {
		values, err := MapErr(opts, func(opt OpQueryOptionInterface, _ int) (any, error) {
			return h.serialize(ctx, string(opt.TargetColumnName()), opt.GetValue())
		})
		if err != nil {
			return nil, err
		}
		return db.Where(query, values...), nil
	})
	return h
}

func (h *applyHelper) applyRangeQueryOptions(ctx context.Context, op string, opts []RangeQueryOptionInterface) *applyHelper {
	if len(opts) == 0 {
		return h
	}
	query := strings.Join(lo.Map(opts, func(opt RangeQueryOptionInterface, _ int) string {
		return fmt.Sprintf("%s %s (?)", opt.TargetColumnName(), op)
	}), " AND ")
	h.db = h.db.Map(func(db *gorm.DB) (*gorm.DB, error) {
		values, err := MapErr(opts, func(opt RangeQueryOptionInterface, _ int) (any, error) {
			return MapErr(opt.GetValues(), func(v any, _ int) (any, error) {
				return h.serialize(ctx, string(opt.TargetColumnName()), v)
			})
		})
		if err != nil {
			return nil, err
		}
		return db.Where(query, values...), nil
	})
	return h
}

func (h *applyHelper) applyFuzzyQueryOptions(ctx context.Context, opts []FuzzyQueryOptionInterface) *applyHelper {
	if len(opts) == 0 {
		return h
	}
	lo.ForEach(opts, func(opt FuzzyQueryOptionInterface, _ int) {
		queries := lo.Map(opt.GetValues(), func(_ any, _ int) string {
			return fmt.Sprintf("%s LIKE ?", opt.TargetColumnName())
		})
		values := lo.Map(opt.GetValues(), func(v any, _ int) any { return fmt.Sprintf("%%%v%%", v) })
		h.db = h.db.Map(func(db *gorm.DB) (*gorm.DB, error) {
			return db.Where(strings.Join(queries, " OR "), values...), nil
		})
	})
	return h
}
