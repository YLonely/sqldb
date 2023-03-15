# Introduction

[![Go Report Card](https://goreportcard.com/badge/github.com/YLonely/sqldb)](https://goreportcard.com/report/github.com/YLonely/sqldb)
[![Go Reference](https://pkg.go.dev/badge/github.com/YLonely/sqldb.svg)](https://pkg.go.dev/github.com/YLonely/sqldb)

sqldb is a useful package which defines some common types and interfaces in manipulating data of models in sql database.

It also provides an implementation of the interfaces based on the [GORM](https://gorm.io/) library.

# Getting Started

## The Model interface
The `Model` defined in `model.go` contains a set of commonly used methods when handling data in a database.
```golang
type Model[T any] interface {
	// DB returns the db instance.
	DB(context.Context) *gorm.DB
	// Table returns the table name in the database.
	Table() string
	// Columns returns a instance of type T,
	// all fields of type sqldb.Column[U] in the instance are populated with corresponding column name.
	Columns() T
	// ColumnNames returns all column names the model has.
	ColumnNames() []ColumnGetter

	Create(ctx context.Context, entity *T) error
	Get(ctx context.Context, opts []OpQueryOptionInterface) (T, error)
	List(ctx context.Context, opts ListOptions) ([]T, uint64, error)
	Update(ctx context.Context, query FilterOptions, opts []UpdateOptionInterface) (uint64, error)
	Delete(ctx context.Context, opts FilterOptions) error
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

	Users := sqldb.NewModel[User](db)
	ctx := context.Background()

	// To create a new user
	age := 10
	u := &User{
		Name: sqldb.NewColumn("test"),
		Age: sqldb.NewPtrColumn(age),
	}
	_ = Users.Create(ctx, u)

	// To get the user
	u, err := Users.Get(ctx, []sqldb.OpQueryOptionInterface{
		// No more string literals, use .Columns() instead.
		sqldb.NewEqualOption(Users.Columns().Name, "test"),
		// Not recommended.
		sqldb.OpQueryOption[string]{
			Op: sqldb.OpEq,
			Option: sqldb.Option[string]{
				Column: sqldb.NewColumnName("user_name"),
				Value: "test",
			},
		},
	})
}
```

It is worth noting that you do not write string literals of columns when constructing query options, every `Model[T]` type has a method `Columns()` which returns a instance of type T, all fields of type `sqldb.Column` in type T are populated with column name during initialization. You can also use the option structs directly, but you have to confirm the column name by yourself, which is extremely not recommended.

## Transactions
`sqldb.go` also defines a function type which abstracts transactions:
```golang
type TransactionFunc func(ctx context.Context, run func(context.Context) error) error
```

To create a `TransactionFunc` implemented by GORM and process models in the transaction:
```golang
Transaction := sqldb.NewTransactionFunc(db)

Transaction(context.Background(), func(ctx context.Context) error {
	if err := Users.Delete(ctx, sqldb.FilterOptions{
		InOptions: []sqldb.RangeQueryOptionInterface{
			sqldb.NewRangeQueryOption(Users.Age, []int{10, 11, 12}),
		}
	}); err != nil {
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
	Name string	
	Age  int
}

type Class struct {
	Name    string
	Address string
	Age     int
}

func main(){
	// Use gorm to open the database.
	users := sqldb.NewModel[User](db)
	classes := sqldb.NewModel[Class](db)
	ctx := context.Background()

	results, total, err := Join(ctx, users, classes, 
		NewJoinOptions(
			append(users.ColumnNames(), classes.ColumnNames()...),
			[]OpJoinOptionInterface{
				NewEqualJoinOption[string](users.Columns().Name, classes.Columns().Name),
			},
		),
	).List(ctx, sqldb.ListOptions{})

	for _, result := range results {
		fmt.Printf("user.name: %s, class.name: %s\n", result.Left.Name, result.Right.Name)
	}
}
```
The join functions also return a `Model` type, which allows you to concatenate other complex query operations. The type `JoinedEntity` contains both Model types that are joined which provides a view of the joined tables.