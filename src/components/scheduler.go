package components

import (
	"context"
	"controller/src/database"
	sqlc "controller/src/database/sqlc"
	"controller/src/docker"
	"controller/src/utils"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"math"
	"time"
)

// Scheduler handles all tasks concerning scaling the system, like calculating ranges, running migrations and getting load of the system
type Scheduler struct {
	logger          *zap.Logger
	reader          *database.Reader
	readerPerf      *database.ReaderPerfectionist
	writer          *database.Writer
	writerPerf      *database.WriterPerfectionist
	dockerInterface docker.DInterface
}

type MigrationInfo struct {
	url             string
	maxSpace        int64
	collectionCount int64
	lastQueried     time.Time
	ranges          []sqlc.DbMapping
}

func NewScheduler(logger *zap.Logger, dbReader *database.Reader, readerPerf *database.ReaderPerfectionist, dbWriter *database.Writer, writerPerf *database.WriterPerfectionist, dInterface docker.DInterface) Scheduler {
	return Scheduler{
		logger:          logger,
		reader:          dbReader,
		readerPerf:      readerPerf,
		writer:          dbWriter,
		writerPerf:      writerPerf,
		dockerInterface: dInterface,
	}
}

// UrlToRangeStartMap maps from the url to the start of the range it covers
type UrlToRangeStartMap map[string][]string

// CalculateStartupMapping maps the alphabetical ranges to the databases that are available at startup.
// It will fail if there are no databases available.
// Since there is no data yet, this does not have to be considered when mapping the ranges, which is why this algorithm is different from the ones that run when the databases are filled
func (s *Scheduler) CalculateStartupMapping(ctx context.Context) UrlToRangeStartMap {

	dbInfos, err := s.readerPerf.GetAllDbInstanceInfo(ctx)
	if err != nil {
		errW := fmt.Errorf("calculating startup mapping failed: %w", err)
		s.logger.Error("error when calculating startup", zap.Error(errW))
	}

	s.logger.Info("got db count when calculating startup mapping", zap.Int("dbCount", len(dbInfos)))

	if len(dbInfos) == 0 {
		errW := fmt.Errorf("calculating startup mapping failed: %w", errors.New("no database instances are registered"))
		s.logger.Error("error when calculating startup mapping", zap.Error(errW))
		return nil
	}

	if len(dbInfos) > 26 {
		errW := fmt.Errorf("calculating startup mapping failed: %w", errors.New("too many database instances registered for startup: tf do you need more than 26 db instances for on startup"))
		s.logger.Error("error when calculating startup mapping", zap.Error(errW))
		return nil
	}

	//initialize startup alphabet (a-z) without any 2nd-letter-level differentiation
	var alphabet []string

	for i := 'a'; i <= 'z'; i++ {
		alphabet = append(alphabet, string(i))
	}

	//map from the db url to the "froms" of the ranges that are hosted on that database
	dbRanges := make(map[string][]string, len(dbInfos))

	//calculate length of the database 4head
	initialRangeCount := float64(len(alphabet))

	//We calculate the "exact" (-> floating point) number of the split, and then round up so that we are guaranteed to have enough space in the last database for all entries
	splitLength := initialRangeCount / float64(len(dbInfos))
	rangeCountPerDB := int(math.Ceil(splitLength))

	//In the beginning, every database only gets one range since they are continuous
	for count, v := range dbInfos {

		start := alphabet[count*rangeCountPerDB]
		dbRanges[v.Url] = append(dbRanges[v.Url], start)

		count++
	}

	return dbRanges

}

// ExecuteStartUpMapping executes the mapping for when the service is first started.
// It will assign the database instances the given ranges by writing them into the dbMappingsTable
func (s *Scheduler) ExecuteStartUpMapping(ctx context.Context, rangeMap UrlToRangeStartMap) {

	s.logger.Info("Adding mappings to registered databases", zap.Int("dbCount", len(rangeMap)))

	var err error

	//TODO maybe do this in one query

	for url, dbRanges := range rangeMap {
		for _, dbRangeStart := range dbRanges {

			s.logger.Info("trying to add database mapping from startup", zap.String("url", url), zap.String("from", dbRangeStart))

			err = s.writerPerf.AddDatabaseMapping(dbRangeStart, url, ctx)
			if err != nil {
				s.logger.Warn("Could not write mapping to database", zap.String("url", url), zap.String("from", dbRangeStart))
			}

		}
	}
}

// RunMigration creates a new migration job for the given rangeId. This range will be moved to the db with the provided url. For that a new migration worker will be created, or if there are available instances, one will be chosen
func (s *Scheduler) RunMigration(ctx context.Context, from, to, goalUrl string) error {

	traceId := ctx.Value("traceID")

	var migrationWorkerId string
	newWorker := true

	worker, err := s.reader.GetFreeMigrationWorker(ctx)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		//if there is no available migration worker, create a new one (also add entry for it to db)

		migrationWorkerId = uuid.New().String()
		err = s.writerPerf.AddMigrationWorker(migrationWorkerId, from, to, ctx)
		if err != nil {
			s.logger.Error("could not add migration worker to table", zap.String("workerUUID", migrationWorkerId), zap.Error(err))
			return fmt.Errorf("could not add migration worker (id : %s) to table: %v", migrationWorkerId, err)
		}

		s.logger.Info("created uuid for new worker and added it to migration worker table", zap.Any("traceID", traceId), zap.String("workerId", migrationWorkerId))

	case err == nil:
		s.logger.Info("migration worker exists, assigning migration job to it", zap.String("workerId", worker.String()))
		migrationWorkerId = worker.String()
		newWorker = false
	default:
		s.logger.Error("could not get migration worker from database", zap.Error(err))
		return fmt.Errorf("could not get migration worker from database, but error was NOT sql.NoRows: %w", err)
	}

	addReq := database.MigrationJobAddReq{
		From:      from,
		To:        to,
		Url:       goalUrl,
		MWorkerId: migrationWorkerId,
	}

	if newWorker {

		s.logger.Info("sending request to dockerClient to create a new migration worker", zap.Any("traceID", traceId))

		req := s.dockerInterface.SendMWorkerRequest(ctx, migrationWorkerId)
		responseErr := utils.ChanWihTimeout(req)
		if responseErr != nil {
			errW := fmt.Errorf("spawning migration worker failed: %w", responseErr)
			s.logger.Error("could not migrate db-range", zap.Error(errW))

			//remove it from the db again if it could not be started
			err = s.writerPerf.RemoveMigrationWorker(migrationWorkerId, ctx)
			if err != nil {
				s.logger.Error("could not remove migration worker from database", zap.Error(err))
				return fmt.Errorf("could not remove migration worker from database, but error was NOT sql.NoRows: %w", err)
			}

			s.logger.Info("successfully removed migration worker from database starting the container failed")
		}
	}

	s.logger.Info("successfully created new migration worker", zap.Any("traceID", traceId))

	//after creating the worker in docker and db, we create the migration job for it
	jobErr := s.writerPerf.AddMigrationJob(ctx, addReq)
	if jobErr != nil {
		errW := fmt.Errorf("adding migration job failed: %w", jobErr)
		s.logger.Error("could not migrate db-range", zap.Error(errW))

		return errW
	}

	s.logger.Info("successfully added migration job to database", zap.Any("traceID", traceId))

	return nil
}

func (s *Scheduler) GetSystemState(ctx context.Context) ([]MigrationInfo, error) {

	dbInstances, instanceErr := s.readerPerf.GetAllDbInstanceInfo(ctx)
	if instanceErr != nil {
		return nil, instanceErr
	}

	mappings, mappingsErr := s.readerPerf.GetAllDbMappingInfo(ctx)
	if mappingsErr != nil {
		return nil, mappingsErr
	}

	infos := make([]MigrationInfo, 0)

	var mappingMap map[string][]sqlc.DbMapping

	//map mappings to mappings map
	for _, mapping := range mappings {

		if mappingMap[mapping.Url] == nil {
			mappingMap[mapping.Url] = make([]sqlc.DbMapping, 0)
		}
		//ah yes... i love mapping
		mappingMap[mapping.Url] = append(mappingMap[mapping.Url], mapping)
	}

	//Match dbInstances and according mappings
	for _, instance := range dbInstances {

		var info MigrationInfo

		info.url = instance.Url
		info.maxSpace = instance.MaxSpace
		info.collectionCount = instance.CollectionCount.Int64
		info.lastQueried = instance.LastQueried.Time

		if mappingMap[info.url] == nil {
			info.ranges = mappingMap[info.url]
		}
		//there are no ranges on a db instance (its empty)

		infos = append(infos, info)
	}

	return infos, nil

}

func (s *Scheduler) Scale(ctx context.Context, delta int) {

}
