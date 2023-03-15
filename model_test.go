package sqldb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Relation struct {
	ID       Column[uint64] `gorm:"column:id;primaryKey"`
	Name     Column[string]
	UserName Column[string]
	Age      Column[int]
}

type User struct {
	ID        Column[uint64] `gorm:"column:id;primaryKey"`
	Name      Column[string] `gorm:"column:user_name"`
	Age       Column[int]
	Address   PtrColumn[string]
	Status    Column[Status] `gorm:"serializer:json"`
	Embedded  `gorm:"embeddedPrefix:embedded_"`
	Extra     Extra `gorm:"embedded;embeddedPrefix:extra_"`
	CreatedAt Column[time.Time]
	DeletedAt Column[gorm.DeletedAt]
}

type Extra struct {
	Inner ExtraInner `gorm:"embedded"`
	Email Column[string]
}

type ExtraInner struct {
	Data Column[uint]
}

type Embedded struct {
	Weight Column[uint]
}

type Status struct {
	Occupation string
}

var (
	ctx    = context.Background()
	dbName = "tmp.db"
	u1     = NewUser(1, "William K Turner", 46, "2824 Davis Court", 107, "Health Educator", "lurline1985@yahoo.com")
	u2     = NewUser(2, "Jillian B Bennett", 49, "4209 Ingram Street", 75, "Refrigeration Mechanic", "jeremy.spenc@yahoo.com")
	u3     = NewUser(3, "Sebastian Turner", 30, "Michigan, Billings", 45, "Teacher", "jennie.nichols@facebook.com")
	u4     = NewUser(4, "Vera Crawford", 29, "4431 Jefferson Street", 100, "Collage student", "jake.andrews@163.com")

	r1 = NewRelation(1, "relation1", "Vera Crawford", 20)
	r2 = NewRelation(2, "relation2", "William K Turner", 30)
	r3 = NewRelation(3, "relation3", "Unknown", 40)
)

func NewUser(id uint64, name string, age int, addr string, weight uint, occupation, email string) *User {
	return &User{
		ID:       NewColumn(id),
		Name:     NewColumn(name),
		Age:      NewColumn(age),
		Address:  NewPtrColumn(addr),
		Status:   NewColumn(Status{Occupation: occupation}),
		Embedded: Embedded{Weight: NewColumn(weight)},
		Extra:    Extra{Email: NewColumn(email)},
	}
}

func NewRelation(id uint64, name, userName string, age int) *Relation {
	return &Relation{
		ID:       NewColumn(id),
		Name:     NewColumn(name),
		UserName: NewColumn(userName),
		Age:      NewColumn(age),
	}
}

func TestField(t *testing.T) {
	db, clean := initDB(t)
	defer clean()
	m := NewModel[User](db)
	assert.Equal(t, "user_name", m.Columns().Name.String())
	assert.Equal(t, "age", m.Columns().Age.String())
	assert.Equal(t, "created_at", m.Columns().CreatedAt.String())
	assert.Equal(t, "address", m.Columns().Address.String())
	assert.Equal(t, "status", m.Columns().Status.String())
	assert.Equal(t, "embedded_weight", m.Columns().Weight.String())
	assert.Equal(t, "extra_email", m.Columns().Extra.Email.String())
	assert.Equal(t, "extra_data", m.Columns().Extra.Inner.Data.String())
}

func initDB(t *testing.T) (*gorm.DB, func()) {
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		t.Fatal(err)
	}
	db.NowFunc = time.Now().UTC
	if err := db.AutoMigrate(User{}); err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(Relation{}); err != nil {
		t.Fatal(err)
	}
	m := NewModel[User](db)
	lo.ForEach([]*User{u1, u2, u3, u4}, func(entity *User, _ int) {
		assert.Nil(t, m.Create(ctx, entity))
	})
	r := NewModel[Relation](db)
	lo.ForEach([]*Relation{r1, r2, r3}, func(entity *Relation, _ int) {
		assert.Nil(t, r.Create(ctx, entity))
	})
	return db, func() {
		os.Remove(dbName)
	}
}

func TestDelete(t *testing.T) {
	db, clean := initDB(t)
	defer clean()

	m := NewModel[User](db)
	Transaction := NewTransactionFunc(db)

	for index, c := range []struct {
		opts   FilterOptions
		expect []User
	}{
		{
			opts: FilterOptions{
				OpOptions: []OpQueryOptionInterface{
					NewEqualOption(m.Columns().Name, "William K Turner"),
					NewEqualOption(m.Columns().Status, Status{Occupation: "Health Educator"}),
				},
			},
			expect: []User{*u2, *u3, *u4},
		},
		{
			opts: FilterOptions{
				FuzzyOptions: []FuzzyQueryOptionInterface{
					NewFuzzyQueryOption(m.Columns().Extra.Email, []string{".com", "yahoo"}),
					NewFuzzyQueryOption(m.Columns().Address, []string{"Street"}),
				},
			},
			expect: []User{*u1, *u3},
		},
		{
			opts: FilterOptions{
				InOptions: []RangeQueryOptionInterface{
					NewRangeQueryOption(m.Columns().Weight, []uint{107, 100}),
				},
			},
			expect: []User{*u2, *u3},
		},
		{
			opts: FilterOptions{
				NotInOptions: []RangeQueryOptionInterface{
					NewRangeQueryOption(m.Columns().Weight, []uint{107, 100}),
				},
			},
			expect: []User{*u1, *u4},
		},
		{
			opts: FilterOptions{
				OpOptions: []OpQueryOptionInterface{
					NewEqualOption(m.Columns().Extra.Email, "lurline1985@yahoo.com"),
				},
				FuzzyOptions: []FuzzyQueryOptionInterface{
					NewFuzzyQueryOption(m.Columns().Name, []string{"Turner"}),
				},
				InOptions: []RangeQueryOptionInterface{
					NewRangeQueryOption(m.Columns().Weight, []uint{106, 108, 107}),
				},
				NotInOptions: []RangeQueryOptionInterface{
					NewRangeQueryOption(m.Columns().Age, []int{44, 90, 82}),
				},
			},
			expect: []User{*u2, *u3, *u4},
		},
	} {
		Transaction(ctx, func(ctx context.Context) error {
			err := m.Delete(ctx, c.opts)
			assert.Nil(t, err, "index %d: %v", index, err)
			left, _, err := m.List(ctx, ListOptions{})
			assert.Nil(t, err, err)
			assert.EqualValues(t, c.expect, left, "index %d", index)
			return errors.New("")
		})
	}

	err := m.Delete(ctx, FilterOptions{
		OpOptions: []OpQueryOptionInterface{
			NewEqualOption(m.Columns().ID, uint64(4)),
		},
	})
	assert.Nil(t, err)

	_, err = m.Get(ctx, []OpQueryOptionInterface{
		NewEqualOption(m.Columns().ID, uint64(4)),
	})
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)

	dest := &User{}
	res := db.Unscoped().Model(&User{}).Where("id = ?", 4).First(dest)
	assert.Nil(t, res.Error, res.Error)
}

func TestUpdate(t *testing.T) {
	db, clean := initDB(t)
	defer clean()

	m := NewModel[User](db)
	Transaction := NewTransactionFunc(db)

	for _, c := range []struct {
		query        FilterOptions
		opts         []UpdateOptionInterface
		expect       []User
		updatedTotal uint64
	}{
		{
			query: FilterOptions{
				OpOptions: []OpQueryOptionInterface{
					NewEqualOption(m.Columns().Name, "William K Turner"),
				},
			},
			opts: []UpdateOptionInterface{
				NewUpdateOption(m.Columns().Name, ""),
				NewUpdateOption(m.Columns().Age, 10),
				NewUpdateOption(m.Columns().Status, Status{Occupation: "test"}),
			},
			expect: []User{func() User {
				u := *u1
				u.Name.V = ""
				u.Age.V = 10
				u.Status.V.Occupation = "test"
				return u
			}(), *u2, *u3, *u4},
			updatedTotal: 1,
		},
		{
			query: FilterOptions{
				FuzzyOptions: []FuzzyQueryOptionInterface{
					NewFuzzyQueryOption(m.Columns().Extra.Email, []string{".com", "yahoo"}),
					NewFuzzyQueryOption(m.Columns().Address, []string{"Street"}),
				},
			},
			opts: []UpdateOptionInterface{
				NewUpdateOption(m.Columns().Weight, uint(1000)),
			},
			expect: []User{*u1, func() User {
				u := *u2
				u.Weight.V = 1000
				return u
			}(), *u3, func() User {
				u := *u4
				u.Weight.V = 1000
				return u
			}()},
			updatedTotal: 2,
		},
	} {
		Transaction(ctx, func(ctx context.Context) error {
			total, err := m.Update(ctx, c.query, c.opts)
			assert.Nil(t, err, err)
			assert.Equal(t, c.updatedTotal, total)
			users, _, err := m.List(ctx, ListOptions{})
			assert.Nil(t, err, err)
			assert.EqualValues(t, c.expect, users)
			return errors.New("")
		})
	}
}

func TestList(t *testing.T) {
	db, clean := initDB(t)
	defer clean()
	m := NewModel[User](db)
	Transaction := NewTransactionFunc(db)

	for index, c := range []struct {
		opts        ListOptions
		expectTotal uint64
		expect      []User
	}{
		{
			opts: ListOptions{
				FilterOptions: FilterOptions{
					OpOptions: []OpQueryOptionInterface{
						NewEqualOption(m.Columns().Status, Status{Occupation: "Teacher"}),
					},
				},
			},
			expectTotal: 1,
			expect:      []User{*u3},
		},
		{
			opts: ListOptions{
				FilterOptions: FilterOptions{
					OpOptions: []OpQueryOptionInterface{
						NewLessOption(m.Columns().Weight, uint(101)),
					},
				},
				SortOptions: []SortOptionInterface{
					NewSortOption[int](m.Columns().Age, SortOrderAscending),
				},
			},
			expectTotal: 3,
			expect:      []User{*u4, *u3, *u2},
		},
		{
			opts: ListOptions{
				FilterOptions: FilterOptions{
					OpOptions: []OpQueryOptionInterface{
						NewLessOption(m.Columns().Weight, uint(101)),
					},
				},
				SortOptions: []SortOptionInterface{
					NewSortOption[int](m.Columns().Age, SortOrderDescending),
				},
			},
			expectTotal: 3,
			expect:      []User{*u2, *u3, *u4},
		},
		{
			opts: ListOptions{
				FilterOptions: FilterOptions{
					OpOptions: []OpQueryOptionInterface{
						NewNotEqualOption(m.Columns().Name, "William K Turner"),
					},
				},
				Offset: 0,
				Limit:  1,
			},
			expectTotal: 3,
			expect:      []User{*u2},
		},
		{
			opts: ListOptions{
				FilterOptions: FilterOptions{
					FuzzyOptions: []FuzzyQueryOptionInterface{
						NewFuzzyQueryOption(m.Columns().Extra.Email, []string{".com", "yahoo"}),
						NewFuzzyQueryOption(m.Columns().Address, []string{"Street"}),
					},
				},
			},
			expectTotal: 2,
			expect:      []User{*u2, *u4},
		},
		{
			opts: ListOptions{
				FilterOptions: FilterOptions{
					InOptions: []RangeQueryOptionInterface{
						NewRangeQueryOption(m.Columns().Weight, []uint{107, 100}),
					},
				},
				Offset: 1,
			},
			expectTotal: 2,
			expect:      []User{*u4},
		},
		{
			opts: ListOptions{
				FilterOptions: FilterOptions{
					OpOptions: []OpQueryOptionInterface{
						NewGreaterEqualOption(m.Columns().Age, 46),
						NewLessOption(m.Columns().Age, 49),
					},
					FuzzyOptions: []FuzzyQueryOptionInterface{
						NewFuzzyQueryOption(m.Columns().Name, []string{"Turner"}),
					},
					InOptions: []RangeQueryOptionInterface{
						NewRangeQueryOption(m.Columns().Weight, []uint{106, 108, 107}),
					},
					NotInOptions: []RangeQueryOptionInterface{
						NewRangeQueryOption(m.Columns().Age, []int{100}),
					},
				},
			},
			expectTotal: 1,
			expect:      []User{*u1},
		},
	} {
		Transaction(ctx, func(ctx context.Context) error {
			users, total, err := m.List(ctx, c.opts)
			assert.Nil(t, err, err)
			assert.Equal(t, c.expectTotal, total, "index %d", index)
			assert.EqualValues(t, c.expect, users, "index %d", index)
			return errors.New("")
		})
	}
}

func TestGet(t *testing.T) {
	db, clean := initDB(t)
	defer clean()

	m := NewModel[User](db)

	user, err := m.Get(ctx, []OpQueryOptionInterface{
		NewEqualOption(m.Columns().ID, uint64(4)),
		NewEqualOption(m.Columns().Extra.Email, "jake.andrews@163.com"),
		NewEqualOption(m.Columns().Status, Status{Occupation: "Collage student"}),
	})
	assert.Nil(t, err)
	assert.Equal(t, *u4, user)
	_, err = m.Get(ctx, []OpQueryOptionInterface{
		NewEqualOption(m.Columns().ID, uint64(100)),
	})
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "")
}

func TestTransaction(t *testing.T) {
	db, clean := initDB(t)
	defer clean()

	m := NewModel[User](db)
	Transaction := NewTransactionFunc(db)

	err := Transaction(ctx, func(ctx context.Context) error {
		assert.Nil(t, m.Delete(ctx, FilterOptions{
			OpOptions: []OpQueryOptionInterface{
				NewEqualOption(m.Columns().ID, uint64(1)),
			},
		}))
		assert.Nil(t, m.Delete(ctx, FilterOptions{
			OpOptions: []OpQueryOptionInterface{
				NewEqualOption(m.Columns().ID, uint64(2)),
			},
		}))
		_ = Transaction(ctx, func(ctx context.Context) error {
			assert.Nil(t, m.Delete(ctx, FilterOptions{
				InOptions: []RangeQueryOptionInterface{
					NewRangeQueryOption(m.Columns().ID, []uint64{3, 4}),
				},
			}))
			return errors.New("")
		})
		return errors.New("")
	})
	assert.NotNil(t, err)

	_, total, err := m.List(ctx, ListOptions{})
	assert.Nil(t, err, err)
	assert.Equal(t, 4, int(total))

	err = Transaction(ctx, func(ctx context.Context) error {
		assert.Nil(t, m.Delete(ctx, FilterOptions{
			InOptions: []RangeQueryOptionInterface{
				NewRangeQueryOption(m.Columns().ID, []uint64{1, 2}),
			},
		}))
		_ = Transaction(ctx, func(ctx context.Context) error {
			m.Delete(ctx, FilterOptions{
				OpOptions: []OpQueryOptionInterface{
					NewEqualOption(m.Columns().Weight, uint(100)),
				},
			})
			return errors.New("")
		})
		return nil
	})
	assert.Nil(t, err)
	_, total, err = m.List(ctx, ListOptions{})
	assert.Nil(t, err, err)
	assert.Equal(t, 2, int(total))
}

func TestRelationUserJoin(t *testing.T) {
	db, clean := initDB(t)
	defer clean()

	var (
		users       = NewModel[User](db)
		relations   = NewModel[Relation](db)
		joined      Model[JoinedEntity[Relation, User]]
		transaction = NewTransactionFunc(db)
	)
	for _, c := range []struct {
		opts     JoinOptions
		listOpts ListOptions
		total    uint64
		expect   []JoinedEntity[Relation, User]
		leftJoin bool
	}{
		{
			opts: NewJoinOptions(
				append(users.ColumnNames(), relations.ColumnNames()...),
				[]OpJoinOptionInterface{
					NewGreaterJoinOption[int](relations.Columns().Age, users.Columns().Age),
				},
			),
			listOpts: ListOptions{
				SortOptions: []SortOptionInterface{
					NewSortOption[uint64](relations.Columns().ID, SortOrderDescending),
					NewSortOption[uint64](users.Columns().ID, SortOrderAscending),
				},
			},
			total: 4,
			expect: []JoinedEntity[Relation, User]{
				{
					Left:  *r3,
					Right: *u3,
				},
				{
					Left:  *r3,
					Right: *u4,
				},
				{
					Left:  *r2,
					Right: *u4,
				},
				{
					Left: *r1,
				},
			},
			leftJoin: true,
		},
		{
			opts: NewJoinOptions(
				append(users.ColumnNames(), relations.ColumnNames()...),
				[]OpJoinOptionInterface{
					NewEqualJoinOption[string](users.Columns().Name, relations.Columns().UserName),
				},
			),
			total: 3,
			expect: []JoinedEntity[Relation, User]{
				{
					Left:  *r1,
					Right: *u4,
				},
				{
					Left:  *r2,
					Right: *u1,
				},
				{
					Left: *r3,
				},
			},
			leftJoin: true,
		},
	} {
		assert.Nil(t, transaction(ctx, func(ctx context.Context) error {
			if c.leftJoin {
				joined = LeftJoin(ctx, relations, users, c.opts)
			} else {
				joined = Join(ctx, relations, users, c.opts)
			}
			results, total, err := joined.List(ctx, c.listOpts)
			if err != nil {
				return err
			}
			if c.total != total {
				return fmt.Errorf("total match, expect %d, actual %d", c.total, total)
			}
			if !assert.EqualValues(t, c.expect, removeListColumnNames(results)) {
				return errors.New("elements match")
			}
			return nil
		}))
	}
}

func TestUserRelationJoin(t *testing.T) {
	db, clean := initDB(t)
	defer clean()

	var (
		users     = NewModel[User](db)
		relations = NewModel[Relation](db)
		joined    Model[JoinedEntity[User, Relation]]
	)
	for _, c := range []struct {
		opts     JoinOptions
		listOpts ListOptions
		total    uint64
		expect   []JoinedEntity[User, Relation]
		leftJoin bool
	}{
		{
			opts: NewJoinOptions(
				append(users.ColumnNames(), relations.ColumnNames()...),
				[]OpJoinOptionInterface{
					NewEqualJoinOption[string](users.Columns().Name, relations.Columns().UserName),
				},
			),
			total: 2,
			expect: []JoinedEntity[User, Relation]{
				{
					Left:  *u1,
					Right: *r2,
				},
				{
					Left:  *u4,
					Right: *r1,
				},
			},
		},
		{
			opts: NewJoinOptions(
				append(users.ColumnNames(), relations.ColumnNames()...),
				[]OpJoinOptionInterface{
					NewGreaterJoinOption[int](relations.Columns().Age, users.Columns().Age),
				},
			),
			total: 5,
			expect: []JoinedEntity[User, Relation]{
				{
					Left: *u1,
				},
				{
					Left: *u2,
				},
				{
					Left:  *u3,
					Right: *r3,
				},
				{
					Left:  *u4,
					Right: *r2,
				},
				{
					Left:  *u4,
					Right: *r3,
				},
			},
			leftJoin: true,
		},
		{
			opts: NewJoinOptions(
				[]ColumnGetter{users.Columns().Name, relations.Columns().UserName},
				[]OpJoinOptionInterface{
					NewEqualJoinOption[string](users.Columns().Name, relations.Columns().UserName),
				},
			),
			total: 1,
			listOpts: ListOptions{
				FilterOptions: FilterOptions{
					OpOptions: []OpQueryOptionInterface{
						NewEqualOption(users.Columns().Age, 46),
					},
				},
			},
			expect: []JoinedEntity[User, Relation]{
				{
					Left:  User{Name: NewColumn("William K Turner")},
					Right: Relation{UserName: NewColumn("William K Turner")},
				},
			},
		},
	} {
		if c.leftJoin {
			joined = LeftJoin(ctx, users, relations, c.opts)
		} else {
			joined = Join(ctx, users, relations, c.opts)
		}
		results, total, err := joined.List(ctx, c.listOpts)
		assert.Nil(t, err)
		assert.Equal(t, c.total, total)
		assert.EqualValues(t, c.expect, removeListColumnNames(results))
	}
}

func removeColumnNames[T any](v T) T {
	iterateFields(&v, func(fieldAddr reflect.Value, path []reflect.StructField) (bool, error) {
		if setter, ok := fieldAddr.Interface().(columnSetter); ok {
			setter.setColumnName("", "")
			return false, nil
		}
		return true, nil
	})
	return v
}

func removeListColumnNames[T any](vs []T) []T {
	return lo.Map(vs, func(v T, _ int) T { return removeColumnNames(v) })
}
