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
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

// This is the base file
type movieFn struct {
	tconst, titleType, primaryTitle, originalTitle string
	isAdult                                        bool
	startYear, endYear, runtimeMinutes             int64
	genres                                         string
}

// movieFn is a DoFn and needs ProcessElement
func (f *movieFn) ProcessElement(line string, emit func(movieFn)) {
	row := strings.Split(line, "\t")
	fmt.Printf("%v\n", row)
	// Skip the header
	if row[0] != "tconst" {
		// Map nulls
		// ..
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
			fmt.Printf("%v\n", m)
			emit(m)
		}
	}
}

// Filters Movies
func filterMovies(movie movieFn, emit func(movieFn)) {
	if !movie.isAdult && movie.startYear >= 1970 {
		emit(movie)
	}
}

// This is the ratings file
type ratingFn struct {
	tconst        string  //`json:"tconst"`
	averageRating float64 //`json:"averageRating"`
	numVotes      int64   //`json:"numVotes"`
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
	Id             string
	TitleType      string
	PrimaryTitle   string
	OriginalTitle  string
	IsAdult        bool
	StartYear      int64
	EndYear        int64
	RuntimeMinutes int64
	Genres         string
	// Ratings
	AverageRating float64
	NumVotes      int64
}

func combineMoviesRatings(movie movieFn, ratings []ratingFn, emit func(targetMovie)) {
	for _, r := range ratings {
		if r.tconst == movie.tconst {
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
}

// We have to register our custom types
func init() {
	beam.RegisterType(reflect.TypeOf((*ratingFn)(nil)).Elem())
}

func main() {
	// Define arguments
	var inputBasePath = flag.String("input-basics", "", "Input base file")
	var inputRatingsPath = flag.String("input-ratings", "", "Input ratings file")
	var output = flag.String("output", "", "Output path")
	// Parse flags
	flag.Parse()

	// Initialize Beam
	beam.Init()

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

	combined := beam.ParDo(s, combineMoviesRatings, filtered_movies,
		beam.SideInput{Input: filtered_ratings})
	// Convert to JSON to write
	combinedString := beam.ParDo(s, func(v targetMovie) string {
		j, _ := json.Marshal(v)
		return fmt.Sprintf(string(j))
	}, combined)
	// Write
	textio.Write(s, *output, combinedString)

	// Concept #1: The beamx.Run convenience wrapper allows a number of
	// pre-defined runners to be used via the --runner flag.
	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
