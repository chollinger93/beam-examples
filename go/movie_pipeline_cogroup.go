package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

/*
An example pipeline that takes movies from IMDb and filters them on custom criteria

This job/file uses `CoGroupByKey` as `join`

This pipeline requires data from IMDb: https://www.imdb.com/interfaces/
*/

// This is the base file
type movieFn struct {
	tconst, titleType, primaryTitle, originalTitle string
	isAdult                                        bool
	startYear, endYear, runtimeMinutes             int64
	genres                                         string
}

// Map \N Strings
func (f *movieFn) cleanNulls(row []string) []string {
	for i, _ := range row {
		if row[i] == "\\N" {
			row[i] = ""
		}
	}
	return row
}

// movieFn is a DoFn and needs ProcessElement
func (f *movieFn) ProcessElement(line string, emit func(movieFn)) {
	row := strings.Split(line, "\t")
	// Skip the header
	if row[0] != "tconst" {
		// Map nulls
		row = f.cleanNulls(row)
		// Convert the types
		startYear, err1 := strconv.ParseInt(row[5], 10, 64)
		endYear, _ := strconv.ParseInt(row[6], 10, 64)
		runtimeMinutes, err3 := strconv.ParseInt(row[7], 10, 64)
		// Convert Boolean
		isAdultInt, err4 := strconv.ParseInt(row[4], 10, 64)
		var isAdult bool
		if isAdultInt == 0 {
			isAdult = false
		} else {
			isAdult = true
		}
		if err1 == nil && err3 == nil && err4 == nil {
			// If the types match, return a rating struct
			m := movieFn{
				tconst:         row[0],
				titleType:      row[1],
				primaryTitle:   row[2],
				originalTitle:  row[3],
				isAdult:        isAdult,
				startYear:      startYear,
				endYear:        endYear,
				runtimeMinutes: runtimeMinutes,
				genres:         row[8],
			}
			//fmt.Printf("%v\n", m)
			emit(m)
		}
	}
}

// Filters Movies
func filterMovies(movie movieFn, emit func(movieFn)) {
	if movie.titleType == "movie" && !movie.isAdult && movie.startYear >= 1970 {
		emit(movie)
	}
}

// This is the ratings file
type ratingFn struct {
	tconst        string  `json:"tconst"`
	averageRating float64 `json:"averageRating"`
	numVotes      int64   `json:"numVotes"`
}

// ratingFn is a DoFn and needs ProcessElement
func (f *ratingFn) ProcessElement(line string, emit func(ratingFn)) {
	row := strings.Split(line, "\t")
	// Skip the header
	if row[0] != "tconst" {
		// Convert the types
		avgrating, errr := strconv.ParseFloat(row[1], 64)
		votes, errv := strconv.ParseInt(row[2], 10, 64)
		if errr == nil && errv == nil {
			// If the types match, return a rating struct
			emit(ratingFn{tconst: row[0], averageRating: avgrating, numVotes: votes})
		}
	}
}

// Filter Rating
func filterRatings(rating ratingFn, emit func(ratingFn)) {
	if rating.averageRating >= 5.0 {
		emit(rating)
	}
}

// Target type
type targetMovie struct {
	Id             string `bigquery:"Id"`
	TitleType      string `bigquery:"TitleType"`
	PrimaryTitle   string `bigquery:"PrimaryTitle"`
	OriginalTitle  string `bigquery:"OriginalTitle"`
	IsAdult        bool   `bigquery:"IsAdult"`
	StartYear      int64  `bigquery:"StartYear"`
	EndYear        int64  `bigquery:"EndYear"`
	RuntimeMinutes int64  `bigquery:"RuntimeMinutes"`
	Genres         string `bigquery:"Genres"`
	// Ratings
	AverageRating float64 `bigquery:"AverageRating"`
	NumVotes      int64   `bigquery:"NumVotes"`
}

// This is still inefficient
func makeRatingsMap(rr int, ratings []ratingFn, emit func(map[string]ratingFn)) {
	m := make(map[string]ratingFn)
	for _, r := range ratings {
		m[r.tconst] = r
	}
	emit(m)
}

// We have to emit `[]byte`, as we cannot serialize a `map`
func combineMoviesRatings(movie movieFn, ratings map[string]ratingFn, emit func(targetMovie)) {
	r, ok := ratings[movie.tconst]
	if ok {
		emit(targetMovie{
			Id:             movie.tconst,
			TitleType:      movie.titleType,
			PrimaryTitle:   movie.primaryTitle,
			OriginalTitle:  movie.originalTitle,
			IsAdult:        movie.isAdult,
			StartYear:      movie.startYear,
			EndYear:        movie.endYear,
			RuntimeMinutes: movie.runtimeMinutes,
			Genres:         movie.genres,
			AverageRating:  r.averageRating,
			NumVotes:       r.numVotes,
		})
	}
}

func extractRatingId(r ratingFn) (string, ratingFn) {
	return r.tconst, r
}

func extractMovieId(m movieFn) (string, movieFn) {
	return m.tconst, m
}

func combineFn(tconst string, movieIter func(*movieFn) bool, ratingIter func(*ratingFn) bool, emit func(targetMovie)) {
	// Pointers to structs
	m := &movieFn{tconst: tconst}
	r := &ratingFn{tconst: tconst}
	// If match, emit
	if movieIter(m) && ratingIter(r) {
		//fmt.Printf("%v %v\n", tconst, m)
		emit(targetMovie{
			Id:             m.tconst,
			TitleType:      m.titleType,
			PrimaryTitle:   m.primaryTitle,
			OriginalTitle:  m.originalTitle,
			IsAdult:        m.isAdult,
			StartYear:      m.startYear,
			EndYear:        m.endYear,
			RuntimeMinutes: m.runtimeMinutes,
			Genres:         m.genres,
			AverageRating:  r.averageRating,
			NumVotes:       r.numVotes,
		})
	}
}

func formatFn(code string, info string) string {
	return fmt.Sprintf("Country code: %v, %v", code, info)
}

// We have to register our custom types
func init() {
	beam.RegisterType(reflect.TypeOf((*ratingFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*movieFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*targetMovie)(nil)).Elem())
}

func main() {
	// Define arguments
	var inputBasePath = flag.String("input-basics", "", "Input base file")
	var inputRatingsPath = flag.String("input-ratings", "", "Input ratings file")
	var output = flag.String("output", "", "Output path")
	var bq = flag.String("bq", "", "Bigquery dataset:table")
	// Parse flags
	flag.Parse()

	// Initialize Beam
	beam.Init()

	// Context
	ctx := context.Background()

	// Input validation. Must be after Init().
	if *inputBasePath == "" || *inputRatingsPath == "" || *output == "" {
		log.Fatal("Usage: movie_pipeline --input-basics $PATH, --input-ratings $PATH --output $PATH")
	}

	// Create a Pipeline
	p := beam.NewPipeline()
	s := p.Root()

	// Parse the movies file
	lines_movies := textio.Read(s, *inputBasePath)
	base_movies := beam.ParDo(s, &movieFn{}, lines_movies)
	filtered_movies := beam.ParDo(s, filterMovies, base_movies)

	// Parse the ratings file
	lines_rating := textio.Read(s, *inputRatingsPath)
	base_rating := beam.ParDo(s, &ratingFn{}, lines_rating)
	filtered_ratings := beam.ParDo(s, filterRatings, base_rating)

	// Combine
	combined := beam.CoGroupByKey(s,
		beam.ParDo(s, extractMovieId, filtered_movies),
		beam.ParDo(s, extractRatingId, filtered_ratings))

	matched := beam.ParDo(s, combineFn, combined)

	// Write
	combinedString := beam.ParDo(s, func(v targetMovie) string {
		j, _ := json.Marshal(v)
		return fmt.Sprintf(string(j))
	}, matched)
	textio.Write(s, *output, combinedString)

	// BQ
	if *bq != "" {
		project := gcpopts.GetProject(ctx)
		bigqueryio.Write(s, project, *bq, matched)
	}

	// Concept #1: The beamx.Run convenience wrapper allows a number of
	// pre-defined runners to be used via the --runner flag.
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
