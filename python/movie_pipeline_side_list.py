from __future__ import absolute_import

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.pvalue import AsDict, AsList
import csv
import itertools

"""An example pipeline that takes movies from IMDb and filters them on custom criteria

This pipeline requires data from IMDb: https://www.imdb.com/interfaces/

This job/file uses list comparisons as `join` - most inefficient
"""

class ParseCsv(beam.DoFn):
    """Parse a CSV file into a dict
    """
    def __init__(self, col_names: list):
        self.col_names = col_names

    def process(self, string: str):
        reader = csv.DictReader(string.splitlines(),
                                fieldnames=self.col_names, delimiter='\t')
        for row in reader:
            yield row


class CleanData(beam.DoFn):
    """Cleans the data, transforms types
    """
    def __init__(self, bool_cols=[], int_cols=[], float_cols=[]):
        """Cleans the data, transforms types

        Args:
            bool_cols (list, optional): Boolean columns mapped from 0/1->True/False. Defaults to [].
            int_cols (list, optional): Integer columns to convert. Defaults to [].
            float_cols (list, optional): Float columns to convert. Defaults to [].
        """
        self.bool_cols = bool_cols
        self.int_cols = int_cols
        self.float_cols = float_cols

    def _parse_numeric(self, record, ttype):
        if ttype == 'float':
            func = float
            _cols = self.float_cols
        elif ttype == 'int':
            func = int
            _cols = self.int_cols
        else:
            raise AttributeError('Invalid type {}'.format(ttype))
        for col in _cols:
            if record[col]:
                try:
                    record[col] = func(record[col])
                except ValueError:
                    record[col] = None
            else:
                record[col] = None
        return record

    def process(self, record: dict):
        for k in record:
            # Map \N to None
            if record[k] == '\\N':
                record[k] = None
        # Convert types
        for col in self.bool_cols:
            if record[col] == '0':
                record[col] = False
            else:
                record[col] = True
        # This is for demonstrative purposes and can be simplified
        record = self._parse_numeric(record, 'int')
        record = self._parse_numeric(record, 'float')

        # Return
        yield record


class FilterRatingData(beam.DoFn):
    """Filters rating data that has a rating less than 5"""
    def process(self, record: dict):
        if record['averageRating'] and record['averageRating'] >= 5.0:
            yield record


class FilterBasicData(beam.DoFn):
    """Filters base data that is not a movie, an adult movie, or from before 1970"""
    def process(self, record: dict):
        #print(record)
        if record['titleType'] == 'movie' and not record['isAdult'] and record['startYear'] and record['startYear'] >= 1970:
            yield record
        # No else - no yield


class GetAttribute(beam.DoFn):
    def __init__(self, col):
        self.col = col

    def process(self, record: dict):
        yield record[self.col]


class JoinRatings(beam.DoFn):
    def process(self, movie: dict, ratings_side):
        for k in ratings_side:
            if k[0] == movie['tconst']:
                yield {**movie, **k[1]}


def run(argv=None):
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-basics',
                        dest='input_basics',
                        required=True,
                        help='Input movie base file to process.')
    parser.add_argument('--input-ratings',
                        dest='input_ratings',
                        required=True,
                        help='Input rating file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Columns
    columns_title_basic = ['tconst', 'titleType', 'primaryTitle', 'originalTitle',
                           'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'genres']
    columns_ratings = ['tconst', 'averageRating', 'numVotes']

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        basic_data = (p | 'Read data' >> beam.io.ReadFromText(known_args.input_basics, skip_header_lines=1)
                      | 'Parse CSV' >> beam.ParDo(ParseCsv(columns_title_basic))
                      | 'Clean data' >> beam.ParDo(CleanData(bool_cols=['isAdult'], int_cols=['startYear', 'endYear', 'runtimeMinutes']))
                      | 'Filter data' >> beam.ParDo(FilterBasicData())
                      )

        rating_data = (p | 'Read data (Details)' >> beam.io.ReadFromText(known_args.input_ratings, skip_header_lines=1)
                       | 'Parse CSV (Details)' >> beam.ParDo(ParseCsv(columns_ratings))
                       | 'Clean data (Details)' >> beam.ParDo(CleanData(int_cols=['numVotes'], float_cols=['averageRating']))
                       | 'Filter data (Details)' >> beam.ParDo(FilterRatingData())
                       )

        # As dict
        rating_data_d = (rating_data | beam.Map(lambda d: (d['tconst'], d)))
        # Join the PCollections
        joined_dicts = (
            basic_data
            | 'Join' >> beam.ParDo(JoinRatings(), AsList(rating_data_d))
        )

        # Write to disk
        joined_dicts | 'write' >> beam.io.WriteToText(known_args.output)

        result = p.run()
        result.wait_until_finish()


if __name__ == '__main__':
    # Set the logger
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S',
                        level=logging.INFO)
    # Run the core pipeline
    logging.info('Starting')
    run()
