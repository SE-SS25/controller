package main

import (
	"context"
	"controller/src/components"
	"controller/src/database"
	"controller/src/docker"
	"controller/src/utils"
	"fmt"
	"github.com/goforj/godump"
	"github.com/jackc/pgx/v5/pgxpool"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"strings"
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

	logger := zap.New(core)

	logger.Info("Production logger created")
	godump.Dump(core)

	return logger
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

	logger := zap.Must(config.Build())

	logger.Info("Development logger created")
	godump.Dump(logger.Core())

	return logger
}

func main() {

	//Setting a context without a timeout since this main function should (optimally) run forever
	ctx := context.Background()

	var logger *zap.Logger

	//Configure the logger depending on app environment
	env := goutils.NoLog().ParseEnvStringPanic("APP_ENV")

	switch env {
	case "prod":
		logger = createProductionLogger()
	case "dev":
		logger = createDevelopmentLogger()
	default:
		fmt.Printf("logger creation failed, since an invalid app environment was specified: %s", env)
		return
	}
	logger.Info("logger initialized", zap.String("environment", env))

	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			println("Error closing the logger")
		}
	}(logger)

	pool, err := utils.SetupDBConn(logger, ctx)
	if err != nil {
		logger.Fatal("establishing connection to database failed, controller is fucking useless, stopping...", zap.Error(err))
		return
		//TODO retries
	}

	_, reconciler, dInterface, controller := setupStructs(pool, logger)

	//test docker daemon connection
	err = dInterface.Ping(ctx)
	if err != nil {
		logger.Error("could not ping docker daemon: %v", zap.Error(err))
		return
	}

	//runs the docker interface so it can accept requests via the channels
	go dInterface.Run()

	//Run the http server
	go func() {
		controller.RunHttpServer()
	}()

	//Make the controller heartbeat to the database
	if !controller.isShadow {

		if registerErr := reconciler.RegisterController(ctx); registerErr != nil {
			logger.Fatal("could not register controller, stopping", zap.Error(registerErr))
		}

		go controller.heartbeat(ctx)

	}

	//If this controller is the shadow, it should get stuck in this function
	controller.checkControllerUp(ctx)

	timeout := goutils.Log().ParseEnvDurationDefault("WORKER_HEARTBEAT_TIMEOUT", 5*time.Second, logger)

	// Function to evaluate worker state
	go func() {

		checkInterval := goutils.Log().ParseEnvDurationDefault("CHECK_WORKER_BACKOFF", 5*time.Second, logger)

		for {

			start := time.Now()
			err := reconciler.EvaluateWorkerState(ctx, timeout)
			if err != nil {
				//Since there is no writing happening, we can kill the controller here so the shadow can step in
				logger.Fatal("fatal error evaluating worker state", zap.Error(err))
			}

			err = reconciler.EvaluateMigrationWorkerState(ctx)
			if err != nil {
				logger.Fatal("fatal error evaluating migration worker state")
			}

			end := time.Now()
			//calculate the time it took for the last check to be concluded and then subtract that from the interval and sleep for the resulting amount of time -> this way the interval should always be the same length
			timeToSleep := checkInterval - (end.Sub(start))

			time.Sleep(timeToSleep)
		}

	}()

	//Function to evaluate failure rate in mongo-worker relationships
	go func() {
		checkFailureRateErr := reconciler.CheckFailureRate(ctx)
		if checkFailureRateErr != nil {
			logger.Fatal("fatal error checking failure rates")
		}
		time.Sleep(5 * time.Minute)
	}()

	select {}
}

// setupStructs sets up all structs needed for functionality in the worker.
// The loggers in reader, writer, and docker should only be used for debug level statements
func setupStructs(pool *pgxpool.Pool, logger *zap.Logger) (components.Scheduler, components.Reconciler, docker.DInterface, Controller) {

	dbWriter := database.Writer{
		Logger: logger.With(zap.String("util", "writer")),
		Pool:   pool,
	}

	dbReader := database.Reader{
		Logger: logger.With(zap.String("util", "reader")),
		Pool:   pool,
	}

	writerPerfectionist := database.NewWriterPerfectionist(
		&dbWriter,
	)

	readerPerfectionist := database.NewReaderPerfectionist(
		&dbReader,
	)

	dockerInterface, err := docker.New(logger)
	if err != nil {
		logger.Error("could not create docker interface", zap.Error(err))
	}

	scheduler := components.NewScheduler(
		logger.With(zap.String("component", "scheduler")),
		&dbReader,
		readerPerfectionist,
		&dbWriter,
		writerPerfectionist,
		dockerInterface,
	)

	reconciler := components.NewReconciler(
		logger.With(zap.String("component", "reconciler")),
		&dbReader,
		readerPerfectionist,
		&dbWriter,
		writerPerfectionist,
		dockerInterface,
	)

	gauntlet := Controller{
		scheduler:  scheduler,
		reconciler: reconciler,
		logger:     logger.With(zap.String("component", "httpHandler")),
		isShadow:   strings.ToLower(goutils.NoLog().ParseEnvStringPanic("SHADOW")) == "true",
	}

	return scheduler, reconciler, dockerInterface, gauntlet
}
