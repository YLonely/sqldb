package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/YLonely/sqldb"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type User struct {
	ID        sqldb.Column[uint64] `gorm:"column:id;primaryKey"`
	Name      sqldb.Column[string] `gorm:"column:user_name"`
	Address   sqldb.PtrColumn[string]
	Status    sqldb.Column[Status] `gorm:"serializer:json"`
	CreatedAt sqldb.Column[time.Time]
	DeletedAt sqldb.Column[gorm.DeletedAt]
}

type Status struct {
	Account string
}

func main() {
	db, err := gorm.Open(sqlite.Open("tmp.db"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	defer os.Remove("tmp.db")
	if err != nil {
		panic(err)
	}
	if err := db.AutoMigrate(User{}); err != nil {
		panic(err)
	}
	m := sqldb.NewModel[User](db)
	cols := m.Columns()
	ctx := context.Background()

	u := &User{
		ID:      sqldb.NewColumn(uint64(1)),
		Name:    sqldb.NewColumn("lazy"),
		Address: sqldb.NewPtrColumn("fox"),
		Status: sqldb.NewColumn(Status{
			Account: "1234",
		}),
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
		cols.Address.FuzzyIn([]string{"fox"}),
		cols.Status.EQ(Status{Account: "1234"}),
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
		fmt.Printf("id: %v\tname: %s\taddress: %s\tstatus: %+v\t\n",
			u.ID.V, u.Name.V, *u.Address.V, u.Status.V)
	}
}
