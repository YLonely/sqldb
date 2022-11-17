package gorm

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/YLonely/sqldb"
)

type User struct {
	ID      sqldb.Column[uint64] `gorm:"column:id;primaryKey"`
	Name    sqldb.Column[string] `gorm:"column:user_name"`
	Age     sqldb.Column[int]
	Address sqldb.Column[*string]
	Status  sqldb.Column[Status] `gorm:"serializer:json"`
	Embedded
	Extra     Extra `gorm:"embedded;embeddedPrefix:extra_"`
	CreatedAt sqldb.Column[time.Time]
	DeletedAt sqldb.Column[gorm.DeletedAt]
}

type Extra struct {
	Inner ExtraInner `gorm:"embedded"`
	Email sqldb.Column[string]
}

type ExtraInner struct {
	Data sqldb.Column[uint]
}

type Embedded struct {
	Weight sqldb.Column[uint]
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
)

func NewUser(id uint64, name string, age int, addr string, weight uint, occupation, email string) *User {
	return &User{
		ID:       sqldb.NewColumn(id),
		Name:     sqldb.NewColumn(name),
		Age:      sqldb.NewColumn(age),
		Address:  sqldb.NewColumn(&addr),
		Status:   sqldb.NewColumn(Status{Occupation: occupation}),
		Embedded: Embedded{Weight: sqldb.NewColumn(weight)},
		Extra:    Extra{Email: sqldb.NewColumn(email)},
	}
}

func TestField(t *testing.T) {
	db, clean := initDB(t)
	defer clean()
	m := NewModel[User](db)
	assert.Equal(t, "user_name", m.Columns().Name.GetColumnName())
	assert.Equal(t, "age", m.Columns().Age.GetColumnName())
	assert.Equal(t, "created_at", m.Columns().CreatedAt.GetColumnName())
	assert.Equal(t, "address", m.Columns().Address.GetColumnName())
	assert.Equal(t, "status", m.Columns().Status.GetColumnName())
	assert.Equal(t, "weight", m.Columns().Weight.GetColumnName())
	assert.Equal(t, "extra_email", m.Columns().Extra.Email.GetColumnName())
	assert.Equal(t, "extra_data", m.Columns().Extra.Inner.Data.GetColumnName())
}

func initDB(t *testing.T) (*gorm.DB, func()) {
	db, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db.NowFunc = time.Now().UTC
	if err := db.AutoMigrate(User{}); err != nil {
		t.Fatal(err)
	}
	m := NewModel[User](db)
	lo.ForEach([]*User{u1, u2, u3, u4}, func(entity *User, _ int) {
		assert.Nil(t, m.Create(ctx, entity))
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
		opts   sqldb.FilterOptions
		expect []User
	}{
		{
			opts: sqldb.FilterOptions{
				OpOptions: []sqldb.OpQueryOptionInterface{
					sqldb.NewEqualOption(m.Columns().Name, "William K Turner"),
				},
			},
			expect: []User{*u2, *u3, *u4},
		},
		{
			opts: sqldb.FilterOptions{
				FuzzyOptions: []sqldb.FuzzyQueryOptionInterface{
					sqldb.NewFuzzyQueryOption(m.Columns().Extra.Email, []string{".com", "yahoo"}),
					sqldb.NewFuzzyQueryOption(m.Columns().Address, []*string{lo.ToPtr("Street")}),
				},
			},
			expect: []User{*u1, *u3},
		},
		{
			opts: sqldb.FilterOptions{
				InOptions: []sqldb.RangeQueryOptionInterface{
					sqldb.NewRangeQueryOption(m.Columns().Weight, []uint{107, 100}),
				},
			},
			expect: []User{*u2, *u3},
		},
		{
			opts: sqldb.FilterOptions{
				NotInOptions: []sqldb.RangeQueryOptionInterface{
					sqldb.NewRangeQueryOption(m.Columns().Weight, []uint{107, 100}),
				},
			},
			expect: []User{*u1, *u4},
		},
		{
			opts: sqldb.FilterOptions{
				OpOptions: []sqldb.OpQueryOptionInterface{
					sqldb.NewEqualOption(m.Columns().Extra.Email, "lurline1985@yahoo.com"),
				},
				FuzzyOptions: []sqldb.FuzzyQueryOptionInterface{
					sqldb.NewFuzzyQueryOption(m.Columns().Name, []string{"Turner"}),
				},
				InOptions: []sqldb.RangeQueryOptionInterface{
					sqldb.NewRangeQueryOption(m.Columns().Weight, []uint{106, 108, 107}),
				},
				NotInOptions: []sqldb.RangeQueryOptionInterface{
					sqldb.NewRangeQueryOption(m.Columns().Age, []int{44, 90, 82}),
				},
			},
			expect: []User{*u2, *u3, *u4},
		},
	} {
		Transaction(ctx, func(ctx context.Context) error {
			err := m.Delete(ctx, c.opts)
			assert.Nil(t, err, "index %d: %v", index, err)
			left, _, err := m.List(ctx, sqldb.ListOptions{})
			assert.Nil(t, err, err)
			assert.EqualValues(t, c.expect, lo.Map(left, func(item *User, _ int) User { return *item }), "index %d", index)
			return errors.New("")
		})
	}

	err := m.Delete(ctx, sqldb.FilterOptions{
		OpOptions: []sqldb.OpQueryOptionInterface{
			sqldb.NewEqualOption(m.Columns().ID, 4),
		},
	})
	assert.Nil(t, err)

	_, err = m.Get(ctx, []sqldb.OpQueryOptionInterface{
		sqldb.NewEqualOption(m.Columns().ID, 4),
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
		query  sqldb.FilterOptions
		opts   []sqldb.UpdateOptionInterface
		expect []User
	}{
		{
			query: sqldb.FilterOptions{
				OpOptions: []sqldb.OpQueryOptionInterface{
					sqldb.NewEqualOption(m.Columns().Name, "William K Turner"),
				},
			},
			opts: []sqldb.UpdateOptionInterface{
				sqldb.NewUpdateOption(m.Columns().Name, ""),
				sqldb.NewUpdateOption(m.Columns().Age, 10),
			},
			expect: []User{func() User {
				u := *u1
				u.Name.V = ""
				u.Age.V = 10
				return u
			}(), *u2, *u3, *u4},
		},
		{
			query: sqldb.FilterOptions{
				FuzzyOptions: []sqldb.FuzzyQueryOptionInterface{
					sqldb.NewFuzzyQueryOption(m.Columns().Extra.Email, []string{".com", "yahoo"}),
					sqldb.NewFuzzyQueryOption(m.Columns().Address, []*string{lo.ToPtr("Street")}),
				},
			},
			opts: []sqldb.UpdateOptionInterface{
				sqldb.NewUpdateOption(m.Columns().Weight, 1000),
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
		},
	} {
		Transaction(ctx, func(ctx context.Context) error {
			_, err := m.Update(ctx, c.query, c.opts)
			assert.Nil(t, err, err)
			users, _, err := m.List(ctx, sqldb.ListOptions{})
			assert.Nil(t, err, err)
			assert.EqualValues(t, c.expect, lo.Map(users, func(item *User, _ int) User { return *item }))
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
		opts        sqldb.ListOptions
		expectTotal uint64
		expect      []User
	}{
		{
			opts: sqldb.ListOptions{
				FilterOptions: sqldb.FilterOptions{
					OpOptions: []sqldb.OpQueryOptionInterface{
						sqldb.NewLessOption(m.Columns().Weight, 101),
					},
				},
				SortOptions: []sqldb.SortOptionInterface{
					sqldb.NewSortOption(m.Columns().Age, sqldb.SortOrderAscending),
				},
			},
			expectTotal: 3,
			expect:      []User{*u4, *u3, *u2},
		},
		{
			opts: sqldb.ListOptions{
				FilterOptions: sqldb.FilterOptions{
					OpOptions: []sqldb.OpQueryOptionInterface{
						sqldb.NewLessOption(m.Columns().Weight, 101),
					},
				},
				SortOptions: []sqldb.SortOptionInterface{
					sqldb.NewSortOption(m.Columns().Age, sqldb.SortOrderDescending),
				},
			},
			expectTotal: 3,
			expect:      []User{*u2, *u3, *u4},
		},
		{
			opts: sqldb.ListOptions{
				FilterOptions: sqldb.FilterOptions{
					OpOptions: []sqldb.OpQueryOptionInterface{
						sqldb.NewNotEqualOption(m.Columns().Name, "William K Turner"),
					},
				},
				Offset: 0,
				Limit:  1,
			},
			expectTotal: 3,
			expect:      []User{*u2},
		},
		{
			opts: sqldb.ListOptions{
				FilterOptions: sqldb.FilterOptions{
					FuzzyOptions: []sqldb.FuzzyQueryOptionInterface{
						sqldb.NewFuzzyQueryOption(m.Columns().Extra.Email, []string{".com", "yahoo"}),
						sqldb.NewFuzzyQueryOption(m.Columns().Address, []*string{lo.ToPtr("Street")}),
					},
				},
			},
			expectTotal: 2,
			expect:      []User{*u2, *u4},
		},
		{
			opts: sqldb.ListOptions{
				FilterOptions: sqldb.FilterOptions{
					InOptions: []sqldb.RangeQueryOptionInterface{
						sqldb.NewRangeQueryOption(m.Columns().Weight, []uint{107, 100}),
					},
				},
				Offset: 1,
			},
			expectTotal: 2,
			expect:      []User{*u4},
		},
		{
			opts: sqldb.ListOptions{
				FilterOptions: sqldb.FilterOptions{
					OpOptions: []sqldb.OpQueryOptionInterface{
						sqldb.NewGreaterEqualOption(m.Columns().Age, 46),
						sqldb.NewLessOption(m.Columns().Age, 49),
					},
					FuzzyOptions: []sqldb.FuzzyQueryOptionInterface{
						sqldb.NewFuzzyQueryOption(m.Columns().Name, []string{"Turner"}),
					},
					InOptions: []sqldb.RangeQueryOptionInterface{
						sqldb.NewRangeQueryOption(m.Columns().Weight, []uint{106, 108, 107}),
					},
					NotInOptions: []sqldb.RangeQueryOptionInterface{
						sqldb.NewRangeQueryOption(m.Columns().Age, []int{100}),
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
			assert.EqualValues(t, c.expect, lo.Map(users, func(item *User, _ int) User { return *item }), "index %d", index)
			return errors.New("")
		})
	}
}

func TestGet(t *testing.T) {
	db, clean := initDB(t)
	defer clean()

	m := NewModel[User](db)

	user, err := m.Get(ctx, []sqldb.OpQueryOptionInterface{
		sqldb.NewEqualOption(m.Columns().ID, 4),
		sqldb.NewEqualOption(m.Columns().Extra.Email, "jake.andrews@163.com"),
	})
	assert.Nil(t, err)
	assert.Equal(t, u4, user)
	_, err = m.Get(ctx, []sqldb.OpQueryOptionInterface{
		sqldb.NewEqualOption(m.Columns().ID, 100),
	})
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound, "")
}

func TestTransaction(t *testing.T) {
	db, clean := initDB(t)
	defer clean()

	m := NewModel[User](db)
	Transaction := NewTransactionFunc(db)

	err := Transaction(ctx, func(ctx context.Context) error {
		assert.Nil(t, m.Delete(ctx, sqldb.FilterOptions{
			OpOptions: []sqldb.OpQueryOptionInterface{
				sqldb.NewEqualOption(m.Columns().ID, 1),
			},
		}))
		assert.Nil(t, m.Delete(ctx, sqldb.FilterOptions{
			OpOptions: []sqldb.OpQueryOptionInterface{
				sqldb.NewEqualOption(m.Columns().ID, 2),
			},
		}))
		_ = Transaction(ctx, func(ctx context.Context) error {
			assert.Nil(t, m.Delete(ctx, sqldb.FilterOptions{
				InOptions: []sqldb.RangeQueryOptionInterface{
					sqldb.NewRangeQueryOption(m.Columns().ID, []uint64{3, 4}),
				},
			}))
			return errors.New("")
		})
		return errors.New("")
	})
	assert.NotNil(t, err)

	_, total, err := m.List(ctx, sqldb.ListOptions{})
	assert.Nil(t, err, err)
	assert.Equal(t, 4, int(total))

	err = Transaction(ctx, func(ctx context.Context) error {
		assert.Nil(t, m.Delete(ctx, sqldb.FilterOptions{
			InOptions: []sqldb.RangeQueryOptionInterface{
				sqldb.NewRangeQueryOption(m.Columns().ID, []uint64{1, 2}),
			},
		}))
		_ = Transaction(ctx, func(ctx context.Context) error {
			m.Delete(ctx, sqldb.FilterOptions{
				OpOptions: []sqldb.OpQueryOptionInterface{
					sqldb.NewEqualOption(m.Columns().Weight, 100),
				},
			})
			return errors.New("")
		})
		return nil
	})
	assert.Nil(t, err)
	_, total, err = m.List(ctx, sqldb.ListOptions{})
	assert.Nil(t, err, err)
	assert.Equal(t, 2, int(total))
}
