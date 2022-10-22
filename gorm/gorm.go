package gorm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/samber/lo"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"

	"github.com/YLonely/sqldb"
)

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
func NewTransactionFunc(db *gorm.DB) sqldb.TransactionFunc {
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

// Model implements the sqldb.Model interface.
type Model[T any] struct {
	sqldb.ColumnHint[T]
	db *gorm.DB
}

var _ sqldb.Model[struct{}] = Model[struct{}]{}

// NewModel returns a new Model.
func NewModel[T any](db *gorm.DB) Model[T] {
	return Model[T]{
		ColumnHint: sqldb.NewColumnHint[T](buildNameColumnFunc(db)),
		db:         db,
	}
}

func buildNameColumnFunc(db *gorm.DB) sqldb.NameFieldFunc {
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

func (m Model[T]) dbInstance(ctx context.Context) *gorm.DB {
	if tx := TransactionFrom(ctx); tx != nil {
		return tx.WithContext(ctx)
	}
	return m.db
}

func (m Model[T]) Update(ctx context.Context, query sqldb.FilterOptions, opts []sqldb.UpdateOption) (uint64, error) {
	if len(opts) == 0 {
		return 0, errors.New("empty options")
	}
	updateMap := map[string]any{}
	for _, opt := range opts {
		updateMap[opt.Column.GetColumnName()] = opt.Value
	}
	res := applyFilterOptions(m.dbInstance(ctx), query).Model(new(T)).Updates(updateMap)
	if res.Error != nil {
		return 0, res.Error
	}
	return uint64(res.RowsAffected), nil
}

func (m Model[T]) Delete(ctx context.Context, opts sqldb.FilterOptions) error {
	return applyFilterOptions(m.dbInstance(ctx), opts).Delete(new(T)).Error
}

func (m Model[T]) Get(ctx context.Context, opts []sqldb.OpQueryOption) (entity *T, err error) {
	if len(opts) == 0 {
		return nil, errors.New("empty options")
	}
	entity = new(T)
	err = applyIsQueryOptions(m.dbInstance(ctx), opts).First(entity).Error
	return
}

func (m Model[T]) Create(ctx context.Context, entity *T) error {
	return m.dbInstance(ctx).Create(entity).Error
}

func (m Model[T]) List(ctx context.Context, opts sqldb.ListOptions) (entities []*T, total uint64, err error) {
	db := m.dbInstance(ctx).Model(new(T))
	var t int64
	if err = applyFilterOptions(db, opts.FilterOptions).Count(&t).Error; err != nil {
		return
	}
	total = uint64(t)
	if opts.Limit != 0 {
		db.Limit(int(opts.Limit))
	}
	if opts.Offset != 0 {
		db.Offset(int(opts.Offset))
	}
	if err = db.Find(&entities).Error; err != nil {
		return
	}
	return
}

func applyFilterOptions(db *gorm.DB, opts sqldb.FilterOptions) *gorm.DB {
	return applyFuzzyQueryOptions(
		applyRangeQueryOptions(
			applyRangeQueryOptions(
				applyIsQueryOptions(
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

func applyIsQueryOptions(db *gorm.DB, opts []sqldb.OpQueryOption) *gorm.DB {
	if len(opts) == 0 {
		return db
	}
	query := strings.Join(lo.Map(opts, func(opt sqldb.OpQueryOption, _ int) string {
		if opt.Op == "" {
			panic("Op must be provided in IsQueryOption")
		}
		return fmt.Sprintf("%s %s ?", opt.Column.GetColumnName(), opt.Op)
	}), " AND ")
	return db.Where(query, lo.Map(opts, func(opt sqldb.OpQueryOption, _ int) any { return opt.Value })...)
}

func applyRangeQueryOptions(db *gorm.DB, op string, opts []sqldb.RangeQueryOption) *gorm.DB {
	if len(opts) == 0 {
		return db
	}
	query := strings.Join(lo.Map(opts, func(opt sqldb.RangeQueryOption, _ int) string {
		return fmt.Sprintf("%s %s (?)", opt.Column.GetColumnName(), op)
	}), " AND ")
	return db.Where(query, lo.Map(opts, func(opt sqldb.RangeQueryOption, _ int) any { return opt.Values })...)
}

func applyFuzzyQueryOptions(db *gorm.DB, opts []sqldb.FuzzyQueryOption) *gorm.DB {
	if len(opts) == 0 {
		return db
	}
	lo.ForEach(opts, func(opt sqldb.FuzzyQueryOption, _ int) {
		queries := lo.Map(opt.Values, func(_ any, _ int) string {
			return fmt.Sprintf("%s LIKE ?", opt.Column.GetColumnName())
		})
		values := lo.Map(opt.Values, func(v any, _ int) any { return fmt.Sprintf("%%%v%%", v) })
		db = db.Where(strings.Join(queries, " OR "), values...)
	})
	return db
}
