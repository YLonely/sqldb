package sqldb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/samber/lo"
	"gorm.io/gorm"
)

type joinResultInterface interface {
	_tableName() string
	_left() any
	_right() any
}

type JoinedEntity[L, R any] struct {
	Left  L `gorm:"embedded"`
	Right R `gorm:"embedded"`
}

func (r JoinedEntity[L, R]) _tableName() string {
	subTableName := func(v any) string {
		return reflect.TypeOf(v).Name()
	}
	return fmt.Sprintf("Join%s%s", subTableName(r.Left), subTableName(r.Right))
}

func (r JoinedEntity[L, R]) _left() any {
	return r.Left
}

func (r JoinedEntity[L, R]) _right() any {
	return r.Right
}

func LeftJoin[L, R any](ctx context.Context, left Model[L], right Model[R], opts JoinOptions) Model[JoinedEntity[L, R]] {
	return join(ctx, left, right, opts.SelectedColumns, opts.Conditions, true)
}

func Join[L, R any](ctx context.Context, left Model[L], right Model[R], opts JoinOptions) Model[JoinedEntity[L, R]] {
	return join(ctx, left, right, opts.SelectedColumns, opts.Conditions, false)
}

func join[L, R any](ctx context.Context, left Model[L], right Model[R],
	selectedColumns []ColumnName, conditions []OpJoinOptionInterface, leftJoin bool) Model[JoinedEntity[L, R]] {
	initial := func(db *gorm.DB) *gorm.DB {
		query := strings.Join(lo.Map(conditions, func(opt OpJoinOptionInterface, _ int) string {
			return fmt.Sprintf("%s %s %s", opt.GetLeftTargetColumn().Full(), opt.QueryOp(), opt.GetRightTargetColumn().Full())
		}), " AND ")
		join := fmt.Sprintf("%s %s on %s", lo.Ternary(leftJoin, "LEFT JOIN", "INNER JOIN"), right.Table(), query)
		return db.Model(new(L)).
			Select(strings.Join(lo.Map(selectedColumns, func(col ColumnName, _ int) string {
				return fmt.Sprintf("%s AS `%s`", col.Full(), col.Full())
			}), ",")).
			Joins(join)
	}
	return NewModel[JoinedEntity[L, R]](left.DB(ctx), WithDBInitialFunc(initial))
}
