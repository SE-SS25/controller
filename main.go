package main

import (
	"context"
	"controller/components"
	"controller/database"
	"controller/docker"
	"controller/envutils"
	customErr "controller/errors"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"net/http"
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

	pgConn := os.Getenv("PG_CONN")
	logger.Debug("Connecting to database", zap.String("conn_string", pgConn))

	pool, err := pgxpool.New(ctx, pgConn)
	if err != nil {
		logger.Error("Unable to connect to database", zap.Error(err)) //TODO retries here, if yes how can i best do it
	}

	if err = pool.Ping(ctx); err != nil {
		logger.Error("Unable to ping database", zap.Error(err))
	}

	logger.Info("Connected to PG database", zap.String("conn", pgConn))

	return pool

}

func main() {

	//Setting a context without a timeout since this main function should run forever
	ctx := context.Background()

	var logger *zap.Logger

	//Configure the logger depending on app environment
	env := os.Getenv("APP_ENV")

	switch env {
	//Get max retries from env and transform to int
	case "prod":
		logger = createProductionLogger()
	case "dev":
		logger = createDevelopmentLogger()
	default:
		fmt.Printf("Logger creation failed, since an invalid app environment was specified: %s", env)
		return
	}

	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			println("Error closing the logger")
		}
	}(logger)

	//TODO maybe we have to wait here for the database to spin up and/or add retries

	pool := setupDBConn(logger)

	logger.Info("Logger initialized", zap.String("environment", env))

	scheduler, reconciler, gauntlet := setupStructs(pool, logger)

	isShadow := os.Getenv("SHADOW") == "true"

	maxRetries := envutils.ParseEnvInt("MAX_RETRIES", 3, logger)
	iterationTimeout := envutils.ParseEnvDuration(os.Getenv("ITER_TIMEOUT"), time.Second, logger)
	timeout := envutils.ParseEnvDuration("HEARTBEAT_TIMEOUT", 5*time.Second, logger)

	//If this controller is the shadow, it should get stuck in this loop
	if isShadow {
		shadowErr := reconciler.CheckControllerUp(ctx)

		if !errors.Is(shadowErr, &customErr.ControllerCrashed{}) {
			logger.Fatal("shadow reconciliation loop failed", zap.Error(shadowErr))
		}

	}

	reconciler.EvaluateWorkerState(ctx, maxRetries, iterationTimeout, timeout)

	//TODO how do we put the shadow in control

	http.Handle("/migrate", gauntlet.migrationHandler())

	err := http.ListenAndServe(":1234", nil)
	if err != nil {
		logger.Error("serving http traffic failed", zap.Error(err))
		return
	}

	mapping := scheduler.CalculateStartupMapping(ctx)

	scheduler.ExecuteStartUpMapping(ctx, mapping)

	//TODO implement the startup -> wait for
}

// setupStructs sets up all structs needed for functionality in the worker.
// The loggers in reader, writer, and docker should only be used for debug level statements
func setupStructs(pool *pgxpool.Pool, logger *zap.Logger) (components.Scheduler, components.Reconciler, InfinityGauntlet) {

	dbWriter := database.Writer{
		Logger: logger.With(zap.String("util", "writer")),
		Pool:   pool,
	}

	dbReader := database.Reader{
		Logger: logger.With(zap.String("util", "reader")),
		Pool:   pool,
	}

	maxRetries := envutils.ParseEnvInt("MAX_RETRIES", 3, logger)

	writerPerfectionist := database.NewWriterPerfectionist(
		dbWriter,
		maxRetries,
	)

	readerPerfectionist := database.NewReaderPerfectionist(
		dbReader,
		maxRetries)

	dockerInterface, err := docker.New(logger)
	if err != nil {
		logger.Error("could not create docker interface", zap.Error(err))
	}

	scheduler := components.NewScheduler(
		logger.With(zap.String("component", "scheduler")),
		readerPerfectionist,
		writerPerfectionist,
		dockerInterface,
	)

	reconciler := components.NewReconciler(
		logger.With(zap.String("component", "reconciler")),
		readerPerfectionist,
		writerPerfectionist,
	)

	gauntlet := InfinityGauntlet{
		scheduler:  scheduler,
		reconciler: reconciler,
		logger:     logger.With(zap.String("component", "httpHandler")),
	}

	return scheduler, reconciler, gauntlet
}
