package sqldb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/samber/lo"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
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
	ColumnHint[T]
	db *gorm.DB
}

var _ Model[struct{}] = model[struct{}]{}

// NewModel returns a new Model.
func NewModel[T any](db *gorm.DB) Model[T] {
	return model[T]{
		ColumnHint: NewColumnHint[T](buildNameColumnFunc(db)),
		db:         db,
	}
}

func buildNameColumnFunc(db *gorm.DB) NameFieldFunc {
	return func(sf reflect.StructField, parents ...reflect.StructField) string {
		tagSettings := schema.ParseTagSetting(sf.Tag.Get("gorm"), ";")
		column := tagSettings["COLUMN"]
		if column == "" {
			column = db.NamingStrategy.ColumnName("", sf.Name)
			var prefix string
			for _, p := range parents {
				tagSettings := schema.ParseTagSetting(p.Tag.Get("gorm"), ";")
				if p := tagSettings["EMBEDDEDPREFIX"]; p != "" && tagSettings["EMBEDDED"] != "" {
					prefix += p
				}
			}
			column = prefix + column
		}
		return column
	}
}

func (m model[T]) dbInstance(ctx context.Context) *gorm.DB {
	if tx := TransactionFrom(ctx); tx != nil {
		return tx.WithContext(ctx)
	}
	return m.db.WithContext(ctx)
}

func (m model[T]) Update(ctx context.Context, query FilterOptions, opts []UpdateOptionInterface) (uint64, error) {
	if len(opts) == 0 {
		return 0, errors.New("empty options")
	}
	updateMap := map[string]any{}
	for _, opt := range opts {
		updateMap[string(opt.TargetColumnName())] = opt.GetValue()
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
