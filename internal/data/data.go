package data

import (
	"context"
	"github.com/go-kratos/kratos-layout/internal/conf"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/google/wire"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	logger2 "gorm.io/gorm/logger"
	"strings"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewGreeterRepo)

// Data .
type Data struct {
	db  *gorm.DB
	rds *redis.ClusterClient
}

func (d Data) DB() *gorm.DB {
	return d.db
}

func (d Data) Redis() *redis.ClusterClient {
	return d.rds
}

// NewData .
func NewData(c *conf.Data, logger log.Logger) (*Data, func(), error) {
	rds := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:              strings.Split(c.Redis.GetAddr(), ","),
		Password:           c.Redis.GetPassword(),
	})
	if err := rds.Ping(context.Background()).Err(); err != nil  {
		return nil, nil, err
	}

	db, err := gorm.Open(mysql.Open(c.Database.Source), &gorm.Config{Logger: logger2.Default.LogMode(logger2.Info)})
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}
	sqlDB.SetMaxOpenConns(int(c.Database.MaxOpenConns))
	sqlDB.SetConnMaxLifetime(c.Database.MaxLifeTime.AsDuration())
	sqlDB.SetMaxIdleConns(int(c.Database.MaxIdleConns))
	cleanup := func() {
		_ = sqlDB.Close()
		_ = rds.Close()
		logger.Print("message", "closing the data resources")
	}
	return &Data{
		db: db,
		rds: rds,
	}, cleanup, nil
}
