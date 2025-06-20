package main

import (
	"context"
	"controller/reader"
	"controller/writer"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"math"
)

type Scheduler struct {
	logger   *zap.Logger
	dbReader *reader.Reader
	dbWriter *writer.Writer
}

// DbRange defines the ranges for the databases
// For this to be compatible with the structure in the database BOTH points are INCLUSIVE
type DbRange struct {
	start string
	end   string
}

// CalculateStartupMapping maps the alphabetical ranges to the databases that are available at startup
// It will fail if there are no databases available
func (s *Scheduler) CalculateStartupMapping(ctx context.Context) {

	count, err := s.dbReader.GetDBCount(ctx)
	if err != nil {
		errW := fmt.Errorf("calculating startup mapping failed: %w", err)
		s.logger.Error("error when calculating startup", zap.Error(errW)) //TODO retries yada yada
	}

	if count == 0 {
		errW := fmt.Errorf("calculating startup mapping failed: %w", errors.New("no workers registered in database"))
		s.logger.Error("error when calculating startup mapping", zap.Error(errW))
	}

	//initialize startup alphabet (a-z) without any 2nd-letter-level differentiation
	var alphabet []string

	for i := 'a'; i <= 'z'; i++ {
		alphabet = append(alphabet, string(i))
	}

	calculateMapping(count, alphabet)

}

// calculateMapping calculates the mapping of the alphabetical ranges to the database at any point (e.g., when a new database is added / removed) -> it is universally usable and not just at startup
func calculateMapping(dbCount int, alphabet []string) []DbRange {

	dbRanges := make([]DbRange, 0, dbCount)

	//TODO this should be given so that we can reuse the function for non-startup calculation too
	//var alphabet []string
	//
	//for i := 'a'; i <= 'z'; i++ {
	//	alphabet = append(alphabet, string(i))
	//}

	initialRangeCount := float64(len(alphabet))

	//We calculate the "exact" (-> floating point) number of the split, and then round up so that we are guaranteed to have enough space in the last database for all entries
	splitLength := initialRangeCount / float64(dbCount)
	rangeCountPerDB := int(math.Ceil(splitLength))

	for i, db := range dbRanges {
		db.start = alphabet[i*rangeCountPerDB]

		//if the last range is longer than the number of elements in our alphabet, then we set the length of the alphabet as the maximum upper bound (this will only ever apply to the last database
		if (i+1)*rangeCountPerDB > len(alphabet)+1 {
			db.end = alphabet[len(alphabet)+1]
			continue
		}
		db.end = alphabet[(i+1)*rangeCountPerDB]
	}

	return dbRanges

}
