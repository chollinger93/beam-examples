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

Uses a byte[] array instead of a map - inefficient proof of concept

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
func makeRatingsMap(rr int, ratings []ratingFn, emit func([]byte)) {
	m := make(map[string]ratingFn)
	for _, r := range ratings {
		m[r.tconst] = r
	}
	jsonMap, err := json.Marshal(m)
	if err == nil {
		emit(jsonMap)
	}

}

// We have to emit `[]byte`, as we cannot serialize a `map`
func combineMoviesRatings(movie movieFn, ratings []byte, emit func(targetMovie)) {
	ratingsMap := make(map[string]ratingFn)
	err := json.Unmarshal(ratings, &ratingsMap)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	r, ok := ratingsMap[movie.tconst]
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
	// Fake PCollection to only run the next parDo once
	fakePCol := beam.CreateList(s, [1]int{
		0,
	})
	// To Map
	filteredRatingsMap := beam.ParDo(s, makeRatingsMap, fakePCol, beam.SideInput{Input: filtered_ratings})
	// And match
	combined := beam.ParDo(s, combineMoviesRatings, filtered_movies,
		beam.SideInput{Input: filteredRatingsMap})

	// Convert to JSON to write
	combinedString := beam.ParDo(s, func(v targetMovie) string {
		j, _ := json.Marshal(v)
		return fmt.Sprintf(string(j))
	}, combined)

	// Debug print
	beam.ParDo0(s, func(v targetMovie) {
		j, _ := json.Marshal(v)
		log.Printf("%v\n", string(j))
	}, combined)

	// Write
	textio.Write(s, *output, combinedString)

	// BQ
	if *bq != "" {
		project := gcpopts.GetProject(ctx)
		bigqueryio.Write(s, project, *bq, combined)
	}

	// Concept #1: The beamx.Run convenience wrapper allows a number of
	// pre-defined runners to be used via the --runner flag.
	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
