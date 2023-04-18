# Introduction

[![Go Report Card](https://goreportcard.com/badge/github.com/YLonely/sqldb)](https://goreportcard.com/report/github.com/YLonely/sqldb)
[![Go Reference](https://pkg.go.dev/badge/github.com/YLonely/sqldb.svg)](https://pkg.go.dev/github.com/YLonely/sqldb)

sqldb is a useful package which defines some common types and interfaces in manipulating data of models in sql database.

It also provides an implementation of the interfaces based on the [GORM](https://gorm.io/) library.

# Getting Started

## The Model interface
The `Model` and `Executor` defined in `model.go` contains a set of commonly used methods when handling data in a database.
```golang
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
```
## Declaring models
Before using the `Model` you have to declaring your model, `User` for example:
```golang
import (
	"gorm.io/gorm"
	"github.com/YLonely/sqldb"
)

type User struct {
	ID      sqldb.Column[uint64] `gorm:"column:id;primaryKey"`
	Name    sqldb.Column[string] `gorm:"column:user_name"`
	Age     sqldb.PtrColumn[int]
	CreatedAt sqldb.Column[time.Time]
	DeletedAt sqldb.Column[gorm.DeletedAt]
}
```
Here `sqldb.Column` or `sqldb.PtrColumn` is a generic type which represents a table column in the database, it contains the value of the corresponding field and also the column name of it. 

## Operating the model
Now we can initialize a `Model` type for `User`:
```golang
import (
	"context"

	"gorm.io/gorm"
	"github.com/YLonely/sqldb"
)

func main(){
	// Use gorm to open the database.
	dsn := "user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
  	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil{
		panic(err)
	}

	if err := db.AutoMigrate(User{}); err != nil {
		panic(err)
	}
	m := sqldb.NewModel[User](db)
	cols := m.Columns()
	ctx := context.Background()

	u := &User{
		ID:   sqldb.NewColumn(uint64(1)),
		Name: sqldb.NewColumn("lazy"),
		Age:  sqldb.NewPtrColumn(10),
	}
	if err := m.Create(ctx, u); err != nil {
		panic(err)
	}

	u.ID.V = 2
	u.Name.V = "jump"
	if err := m.Create(ctx, u); err != nil {
		panic(err)
	}

	users, _, err := m.Query(
		cols.Name.NE("lazy"),
		cols.Age.In([]int{10, 11, 12}),
		// not recommended
		sqldb.NewOpQueryOption(
			sqldb.NewColumnName("user_name"),
			sqldb.OpEq,
			"jump",
		),
	).List(ctx, sqldb.ListOptions{})
	if err != nil {
		panic(err)
	}
	for _, u := range users {
		fmt.Printf("id: %v\tname: %s\tage: %v\n",
			u.ID.V, u.Name.V, *u.Age.V)
	}
}
```

It is worth noting that you do not write string literals of columns when constructing query options, every `Model[T]` type has a method `Columns()` which returns a instance of type T, all fields of type `sqldb.Column` or `sqldb.PtrColumn` in type T provide a bunch of useful methods for users to construct query options. 
```golang
func EQ(value any) OpOption {}
func NE(value any) OpOption {}
func GT(value any) OpOption {}
func LT(value any) OpOption {}
func GTE(value any) OpOption {}
func LTE(value any) OpOption {}
func In(values []T) RangeQueryOption {}
func NotIn(values []T) RangeQueryOption {}
func FuzzyIn(values []T) FuzzyQueryOption {}
func Update(value any) UpdateOption {}
```
You can also use the option structs directly, but you have to confirm the column name by yourself, which is extremely not recommended.

## Transactions
`sqldb.go` also defines a function type which abstracts transactions:
```golang
type TransactionFunc func(ctx context.Context, run func(context.Context) error) error
```

To create a `TransactionFunc` implemented by GORM and process models in the transaction:
```golang
Transaction := sqldb.NewTransactionFunc(db)

Transaction(context.Background(), func(ctx context.Context) error {
	if err := Users.Query(Users.Columns().Age.In([]int{10, 11, 12})).Delete(ctx); err != nil {
		return err
	}

	// nested transaction.
	Transaction(ctx, func(ctx context.Context) error {
	})
})
```
## Joining tables

sqldb provides a more convenient way to join tables. The complexity of renaming duplicate column names and writing lengthy sql statements is hidden in the internal processing of sqldb. All you need to do is to call the encapsulated join functions. 
```golang
import (
	"context"

	"gorm.io/gorm"
	"github.com/YLonely/sqldb"
)

type User struct {
	Name sqldb.Column[string]
	Age  sqldb.Column[int]
}

type Class struct {
	Name    sqldb.Column[string]
	Address sqldb.Column[string]
	Age     sqldb.Column[int]
}

func main(){
	// Use gorm to open the database.
	users := sqldb.NewModel[User](db)
	classes := sqldb.NewModel[Class](db)
	ctx := context.Background()

	results, total, err := Join(ctx, users, classes, 
		NewJoinOptions(
			append(users.ColumnNames(), classes.ColumnNames()...),
			users.Columns().Name.EQ(classes.Columns().Name),
		),
	).Query().List(ctx, sqldb.ListOptions{})

	for _, result := range results {
		fmt.Printf("user.name: %s, class.name: %s\n", result.Left.Name, result.Right.Name)
	}
}
```
The join functions also return a `Model` type, which allows you to concatenate other complex query operations. The type `JoinedEntity` contains both Model types that are joined which provides a view of the joined tables.