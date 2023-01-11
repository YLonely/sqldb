package sqldb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/samber/lo"
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
	)
	if column == "" {
		column = db.NamingStrategy.ColumnName("", sf.Name)
		var prefix string
		for _, p := range parents {
			tagSettings := gormschema.ParseTagSetting(p.Tag.Get("gorm"), ";")
			if p := tagSettings["EMBEDDEDPREFIX"]; p != "" && tagSettings["EMBEDDED"] != "" {
				prefix += p
			}
		}
		column = prefix + column
	}
	if serializerName != "" {
		if s, exist := gormschema.GetSerializer(serializerName); exist {
			serializer = func(ctx context.Context, v any) (any, error) {
				if v == nil {
					return nil, errors.New("serialize: nil value")
				}
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
		name := string(opt.TargetColumnName())
		value := opt.GetValue()
		if s, exist := m.columnSerializers[name]; exist {
			v, err := s(ctx, value)
			if err != nil {
				return 0, fmt.Errorf("failed to serialize the value of the column %s: %w", name, err)
			}
			value = v
		}
		updateMap[name] = value
	}
	res := applyFilterOptions(m.dbInstance(ctx), query).Model(new(T)).Updates(updateMap)
	if res.Error != nil {
		return 0, res.Error
	}
	return uint64(res.RowsAffected), nil
}

func (m model[T]) Delete(ctx context.Context, opts FilterOptions) error {
	return applyFilterOptions(m.dbInstance(ctx), opts).Delete(new(T)).Error
}

func (m model[T]) Get(ctx context.Context, opts []OpQueryOptionInterface) (entity *T, err error) {
	if len(opts) == 0 {
		return nil, errors.New("empty options")
	}
	entity = new(T)
	err = applyOpQueryOptions(m.dbInstance(ctx), opts).First(entity).Error
	return
}

func (m model[T]) Create(ctx context.Context, entity *T) error {
	return m.dbInstance(ctx).Create(entity).Error
}

func (m model[T]) List(ctx context.Context, opts ListOptions) (entities []*T, total uint64, err error) {
	db := m.dbInstance(ctx).Model(new(T))
	var t int64
	if err = applyFilterOptions(db, opts.FilterOptions).Count(&t).Error; err != nil {
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

func applyFilterOptions(db *gorm.DB, opts FilterOptions) *gorm.DB {
	return applyFuzzyQueryOptions(
		applyRangeQueryOptions(
			applyRangeQueryOptions(
				applyOpQueryOptions(
					db,
					opts.OpOptions,
				),
				"IN",
				opts.InOptions,
			),
			"NOT IN",
			opts.NotInOptions,
		),
		opts.FuzzyOptions,
	)
}

func applyOpQueryOptions(db *gorm.DB, opts []OpQueryOptionInterface) *gorm.DB {
	if len(opts) == 0 {
		return db
	}
	query := strings.Join(lo.Map(opts, func(opt OpQueryOptionInterface, _ int) string {
		if opt.QueryOp() == "" {
			panic("Op must be provided in IsQueryOption")
		}
		return fmt.Sprintf("%s %s ?", opt.TargetColumnName(), opt.QueryOp())
	}), " AND ")
	return db.Where(query, lo.Map(opts, func(opt OpQueryOptionInterface, _ int) any { return opt.GetValue() })...)
}

func applyRangeQueryOptions(db *gorm.DB, op string, opts []RangeQueryOptionInterface) *gorm.DB {
	if len(opts) == 0 {
		return db
	}
	query := strings.Join(lo.Map(opts, func(opt RangeQueryOptionInterface, _ int) string {
		return fmt.Sprintf("%s %s (?)", opt.TargetColumnName(), op)
	}), " AND ")
	return db.Where(query, lo.Map(opts, func(opt RangeQueryOptionInterface, _ int) any { return opt.GetValues() })...)
}

func applyFuzzyQueryOptions(db *gorm.DB, opts []FuzzyQueryOptionInterface) *gorm.DB {
	if len(opts) == 0 {
		return db
	}
	lo.ForEach(opts, func(opt FuzzyQueryOptionInterface, _ int) {
		queries := lo.Map(opt.GetValues(), func(_ any, _ int) string {
			return fmt.Sprintf("%s LIKE ?", opt.TargetColumnName())
		})
		values := lo.Map(opt.GetValues(), func(v any, _ int) any { return fmt.Sprintf("%%%v%%", v) })
		db = db.Where(strings.Join(queries, " OR "), values...)
	})
	return db
}
