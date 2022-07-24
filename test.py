#! /usr/bin/python3
# test.py

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTop5Temp(MRJob):

    def steps(self):
        return[
            MRStep (mapper = self.mapping_meteorological_weather_station,
                    reducer = self.reduce_value),
            MRStep (reducer = self.select_top5)
        ]

    def mapping_meteorological_weather_station(self, _, lines):
        details = lines.split(",")
        yield details[2], details[4]

    def reduce_value(self, key, values):
        yield None, (sum(values), key)

    def select_top5(self, _, pair):
        sorted_pairs = sorted(pair, reverse=True)
        for pair in sorted_pairs[0:5]:
            yield pair

if __name__ == '__main__':
    MRTop5Temp.run()