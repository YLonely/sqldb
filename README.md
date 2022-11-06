# Introduction

sqldb is a useful package which defines some common types and interfaces in manipulating data of models in sql database.

It also provides an implementation of the interfaces based on the [GORM](https://gorm.io/) library.

# Getting Started

A `Model` defined in `sqldb.go` contains a set of commonly used methods when handling data in a database.
```golang
type Model[T any] interface {
	Columns() T
	Create(ctx context.Context, entity *T) error
	Get(ctx context.Context, opts []OpQueryOption) (*T, error)
	List(ctx context.Context, opts ListOptions) ([]*T, uint64, error)
	Update(ctx context.Context, query FilterOptions, opts []UpdateOption) (uint64, error)
	Delete(ctx context.Context, opts FilterOptions) error
}
```
Before using the `Model` you have to declaring your model, `User` for example:
```golang
import "github.com/YLonely/sqldb"

type User struct {
	ID      sqldb.Column[uint64] `gorm:"column:id;primaryKey"`
	Name    sqldb.Column[string] `gorm:"column:user_name"`
	Age     sqldb.Column[*int]
	CreatedAt sqldb.Column[time.Time]
	DeletedAt sqldb.Column[gorm.DeletedAt]
}
```
Here `sqldb.Column` is a generic type which represents a table column in the database, it contains the value of the corresponding field and also the column name of it. 

Now we can initialize a `Model` type for `User`:
```golang
import (
	"context"

	"github.com/YLonely/sqldb"
	sqlgorm "github.com/YLonely/sqldb/gorm"
)

func main(){
	// Use gorm to open the database.
	dsn := "user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
  	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil{
		panic(err)
	}

	var Users sqldb.Model[User] = sqlgorm.NewModel[User](db)
	ctx := context.Background()

	// To create a new user
	age := 10
	u := &User{
		Name: sqldb.NewColumn("test"),
		Age: sqldb.NewColumn(&age),
	}
	_ = Users.Create(ctx, u)

	// To get the user
	u, err := Users.Get(ctx, []sqldb.OpQueryOption{
		// No more string literals, use .Columns() instead.
		{
			Column: Users.Columns().Name
			Op: OpEq,
			Value: "test",
		}, // or use the constructor defined in sqldb.go: sqldb.NewEqualOption(Users.Columns().Name, "test")
	})
}
```

It is worth noting that you do not write string literals of columns when constructing query options, every `Model[T]` type has a method `Columns()` which returns a instance of type T, all fields of type `sqldb.Column` in type T are populated with column name during initialization.

`sqldb.go` also defines a function type which abstracts transactions:
```golang
type TransactionFunc func(ctx context.Context, run func(context.Context) error) error
```

To create a `TransactionFunc` implemented by GORM and process models in the transaction:
```golang
Transaction := gorm.NewTransactionFunc(db)

Transaction(context.Background(), func(ctx context.Context) error {
	if err := Users.Delete(ctx, sqldb.FilterOptions{
		InOptions: []sqldb.RangeQueryOption{
			Column: Users.Age,
			Values: []any{10, 11, 12}
		}
	}); err != nil {
		return err
	}

	// nested transaction.
	Transaction(ctx, func(ctx context.Context) error {
	})
})
```
