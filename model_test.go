package sqldb

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
)

type User struct {
	ID      Column[uint64] `gorm:"column:id;primaryKey"`
	Name    Column[string] `gorm:"column:user_name"`
	Age     Column[int]
	Address PtrColumn[string]
	Status  Column[Status] `gorm:"serializer:json"`
	Embedded
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

func TestField(t *testing.T) {
	db, clean := initDB(t)
	defer clean()
	m := NewModel[User](db)
	assert.Equal(t, "user_name", string(m.Columns().Name.ColumnName))
	assert.Equal(t, "age", string(m.Columns().Age.ColumnName))
	assert.Equal(t, "created_at", string(m.Columns().CreatedAt.ColumnName))
	assert.Equal(t, "address", string(m.Columns().Address.ColumnName))
	assert.Equal(t, "status", string(m.Columns().Status.ColumnName))
	assert.Equal(t, "weight", string(m.Columns().Weight.ColumnName))
	assert.Equal(t, "extra_email", string(m.Columns().Extra.Email.ColumnName))
	assert.Equal(t, "extra_data", string(m.Columns().Extra.Inner.Data.ColumnName))
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
			assert.EqualValues(t, c.expect, lo.Map(left, func(item *User, _ int) User { return *item }), "index %d", index)
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
		query  FilterOptions
		opts   []UpdateOptionInterface
		expect []User
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
		},
	} {
		Transaction(ctx, func(ctx context.Context) error {
			_, err := m.Update(ctx, c.query, c.opts)
			assert.Nil(t, err, err)
			users, _, err := m.List(ctx, ListOptions{})
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
			assert.EqualValues(t, c.expect, lo.Map(users, func(item *User, _ int) User { return *item }), "index %d", index)
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
	assert.Equal(t, u4, user)
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
