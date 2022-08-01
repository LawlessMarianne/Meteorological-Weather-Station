#!/usr/bin/env python
# Task1.py

#from multiprocessing.sharedctypes import Value
#from tracemalloc import Statistic
from mrjob.job import MRJob

SUN_HEADER = "Statistic|Month|Meteorological Weather Station|UNIT|VALUE"
RAIN_HEADER = "Statistic|Month|Meteorological Weather Station|UNIT|VALUE|"
                   
FIELD_SEP = '|'

class ProjectTask1Job(MRJob):

    def mapper(self, _, line):

        # Skip the header lines in both files
        if line != RAIN_HEADER and line != SUN_HEADER:
            fields = line.split(FIELD_SEP)
            if len(fields) == 5:  # We have the sunshine dataset
                key = fields[1]  # The key is in the attribute month
                Statistic = fields[0]
                VALUE = (float(fields[4]))
                value = (Statistic, VALUE)
                yield key, ('S', value)
            elif len(fields) == 5:  # We have the Rainfall dataset
                key = fields[1]  # The key is in the attribute month
                Meteorological_Weather_Station = fields[2]
                UNIT = (float(fields[3]))
                value = (Meteorological_Weather_Station,UNIT)
                yield key, ('R', value)
            else:
                raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):

        Statistic = ""
        VALUE = ""
        for value in list(values):
            if value[0] == 'R':
                Meteorological_Weather_Station = value[1][0]
                UNIT = value[1][1]
                yield Meteorological_Weather_Station(UNIT)
            elif value[0] == 'S':
                Statistic = value[1][0]
                VALUE = value[1][1]
             

if __name__ == '__main__':
    ProjectTask1Job.run()