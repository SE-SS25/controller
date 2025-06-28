package components

import (
	"context"
	"controller/src/database"
	"controller/src/docker"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"math"
)

type Scheduler struct {
	logger          *zap.Logger
	readerPerf      *database.ReaderPerfectionist
	writerPerf      *database.WriterPerfectionist
	dockerInterface docker.DInterface
}

func NewScheduler(logger *zap.Logger, readerPerf *database.ReaderPerfectionist, writerPerf *database.WriterPerfectionist, dockerInterface docker.DInterface) Scheduler {
	return Scheduler{
		logger:          logger,
		readerPerf:      readerPerf,
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

	if dbCount > 26 { //TODO???
		errW := fmt.Errorf("calculating startup mapping failed: %w", errors.New("too many database instances are registered"))
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

func (s *Scheduler) RunMigration(ctx context.Context, rangeId string) error {

	//TODO creating a new migration worker everytime we have a migration is inefficient, we should check if they still exist and if they do give them a few seconds to start processing the migration
	//BUT how can we check if the migration is in processing -> maybe processing status

	//Create an id for the migration worker we are about to create
	migrationWorkerId := uuid.New().String()

	//copy the given rangeID from the mappings-table to the migrations-table and specify the worker that is responsible
	jobErr := s.writerPerf.AddMigrationJob(ctx, rangeId, migrationWorkerId)
	if jobErr != nil {
		errW := fmt.Errorf("running migration failed: %w", jobErr)
		s.logger.Error("could not migrate db-range", zap.Error(errW))
		return errW
	}

	//create a worker and assign it an id, then assign the job to it (create uuid in beginning and use it for both)

	spawnErr := s.dockerInterface.StartMigrationWorker(ctx)
	if spawnErr != nil {
		errW := fmt.Errorf("spawning migration worker failed: %w", spawnErr)
		s.logger.Error("could not migrate db-range", zap.Error(errW))
	}

	return nil
}
