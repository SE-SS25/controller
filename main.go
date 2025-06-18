package main

import (
	"context"
	"controller/reader"
	"controller/writer"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"time"
)

func createProductionLogger() *zap.Logger {
	stdout := zapcore.AddSync(os.Stdout)

	file := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "logs/app.log",
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     7, // days
	})

	level := zap.NewAtomicLevelAt(zap.InfoLevel)

	productionCfg := zap.NewProductionEncoderConfig()
	productionCfg.TimeKey = "timestamp"
	productionCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	developmentCfg := zap.NewDevelopmentEncoderConfig()
	developmentCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	consoleEncoder := zapcore.NewConsoleEncoder(developmentCfg)
	fileEncoder := zapcore.NewJSONEncoder(productionCfg)

	core := zapcore.NewTee(
		zapcore.NewSamplerWithOptions(zapcore.NewCore(consoleEncoder, stdout, level), time.Second, 10, 5),
		zapcore.NewCore(fileEncoder, file, level),
	)

	return zap.New(core)
}

func createDevelopmentLogger() *zap.Logger {
	encoderCfg := zap.NewDevelopmentEncoderConfig()
	encoderCfg.TimeKey = " timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	config := zap.Config{
		Level:         zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:   true,
		Encoding:      "json",
		EncoderConfig: encoderCfg,
		OutputPaths: []string{
			"stderr",
		},
	}

	return zap.Must(config.Build())
}

func setupDBConn(logger *zap.Logger) *pgxpool.Pool {

	ctx := context.Background()

	pg_conn := os.Getenv("PG_CONN")
	logger.Debug("Connecting to database", zap.String("conn_string", pg_conn))

	pool, err := pgxpool.New(ctx, pg_conn)
	if err != nil {
		logger.Error("Unable to connect to database", zap.Error(err)) //TODO retries here, if yes how can i best do it
	}

	if err = pool.Ping(ctx); err != nil {
		logger.Error("Unable to ping database", zap.Error(err))
	}

	logger.Info("Connected to PG database", zap.String("conn", pg_conn))

	return pool

}

func main() {

	logger, _ := zap.NewProduction()

	env := os.Getenv("APP_ENV")

	switch env {
	case "prod":
		logger = createProductionLogger()
	case "dev":
		logger = createDevelopmentLogger()
	default:
		logger.Fatal("Requested environment is not valid", zap.String("env", env))
	}

	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			println("Error closing the logger")
		}
	}(logger)

	pool := setupDBConn(logger)

	logger.Info("Logger initialized", zap.String("environment", env))

	dbWriter := writer.Writer{
		Logger: logger.With(zap.String("util", "writer")),
		Pool:   pool,
	}

	dbReader := reader.Reader{
		Logger: logger.With(zap.String("util", "reader")),
		Pool:   pool,
	}

	scheduler := Scheduler{
		logger:   logger.With(zap.String("component", "scheduler")),
		dbWriter: &dbWriter,
		dbReader: &dbReader,
	}

	reconciler := Reconciler{
		logger:   logger.With(zap.String("component", "reconciler")),
		dbReader: &dbReader,
		dbWriter: &dbWriter,
	}

	isShadow := os.Getenv("SHADOW") == "true"

	if isShadow {
		reconciler.CheckControllerUp()
	}
}
