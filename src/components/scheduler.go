package components

import (
	"context"
	"controller/src/database"
	sqlc "controller/src/database/sqlc"
	"controller/src/docker"
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

func NewScheduler(logger *zap.Logger, dbReader *database.Reader, readerPerf *database.ReaderPerfectionist, dbWriter *database.Writer, writerPerf *database.WriterPerfectionist, dockerInterface docker.DInterface) Scheduler {
	return Scheduler{
		logger:          logger,
		reader:          dbReader,
		readerPerf:      readerPerf,
		writer:          dbWriter,
		writerPerf:      writerPerf,
		dockerInterface: dockerInterface,
	}
}

// UrlToRangeStartMap maps from the url to the start of the range it covers
type UrlToRangeStartMap map[string][]string

// CalculateStartupMapping maps the alphabetical ranges to the databases that are available at startup.
// It will fail if there are no databases available.
// Since there is no data yet, this does not have to be considered when mapping the ranges, which is why this algorithm is different from the ones that run when the databases are filled
func (s *Scheduler) CalculateStartupMapping(ctx context.Context) UrlToRangeStartMap {

	dbCount, err := s.readerPerf.GetDBCount(ctx)
	if err != nil {
		errW := fmt.Errorf("calculating startup mapping failed: %w", err)
		s.logger.Error("error when calculating startup", zap.Error(errW))
	}

	if dbCount == 0 {
		errW := fmt.Errorf("calculating startup mapping failed: %w", errors.New("no database instances are registered"))
		s.logger.Error("error when calculating startup mapping", zap.Error(errW))
		return nil
	}

	if dbCount > 26 {
		errW := fmt.Errorf("calculating startup mapping failed: %w", errors.New("too many database instances registered for startup: tf do you need more than 26 db instances for on startup"))
		s.logger.Error("error when calculating startup mapping", zap.Error(errW))
		return nil
	}

	//initialize startup alphabet (a-z) without any 2nd-letter-level differentiation
	var alphabet []string

	for i := 'a'; i <= 'z'; i++ {
		alphabet = append(alphabet, string(i))
	}

	dbRanges := make(map[string][]string, dbCount)

	initialRangeCount := float64(len(alphabet))

	//We calculate the "exact" (-> floating point) number of the split, and then round up so that we are guaranteed to have enough space in the last database for all entries
	splitLength := initialRangeCount / float64(dbCount)
	rangeCountPerDB := int(math.Ceil(splitLength))

	//In the beginning, every database only gets one range since they are continuous
	counter := 0

	for i, _ := range dbRanges {

		start := alphabet[counter*rangeCountPerDB]
		dbRanges[i] = append(dbRanges[i], start)
	}

	return dbRanges

}

// ExecuteStartUpMapping executes the mapping for when the service is first started.
// It will assign the database instances the given ranges by writing them into the dbMappingsTable
func (s *Scheduler) ExecuteStartUpMapping(ctx context.Context, rangeMap UrlToRangeStartMap) {

	var err error

	//TODO maybe do this in one query

	for url, dbRanges := range rangeMap {
		for _, dbRangeStart := range dbRanges {

			err = s.writerPerf.AddDatabaseMapping(dbRangeStart, url, ctx)
			if err != nil {
				s.logger.Warn("Could not write mapping to database", zap.String("url", url), zap.String("start", dbRangeStart))
			}
		}
	}
}

// RunMigration creates a new migration job for the given rangeId. This range will be moved to the db with the provided url. For that a new migration worker will be created, or if there are available instances, one will be chosen
func (s *Scheduler) RunMigration(ctx context.Context, rangeId, goalUrl string) error {

	//BUT how can we check if the migration is in processing -> maybe processing status
	var migrationWorkerId string
	newWorker := true

	workerId, err := s.readerPerf.GetFreeMigrationWorker(ctx)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		//if there is no available migration worker, create a new one
		migrationWorkerId = uuid.New().String()
	case err == nil:
		migrationWorkerId = workerId.String()
		newWorker = false
	default:
		s.logger.Error("could not get migration worker from database", zap.Error(err))
		return fmt.Errorf("could not get migration worker from database, but error was NOT sql.NoRows: %w", err)
	}

	//copy the rangeId and its "from" to the migrations table with the url of the goal db instance and a workerId chosen by the algorithm
	jobErr := s.writerPerf.AddMigrationJob(ctx, rangeId, goalUrl, migrationWorkerId)
	if jobErr != nil {
		errW := fmt.Errorf("running migration failed: %w", jobErr)
		s.logger.Error("could not migrate db-range", zap.Error(errW))
		return errW
	}

	//create a worker and assign it an id, then assign the job to it (create uuid in beginning and use it for both)
	if !newWorker {
		spawnErr := s.dockerInterface.StartMigrationWorker(ctx)
		if spawnErr != nil {
			errW := fmt.Errorf("spawning migration worker failed: %w", spawnErr)
			s.logger.Error("could not migrate db-range", zap.Error(errW))
		}
	}

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
