from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import BytesProtocol

import re

WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):

    
    def steps(self):
        return[MRStep(mapper=self.mapper,reducer=self.reducer)]
        
    def mapper(self, _, line):
        for word in WORD_RE.findall(line) :
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

       


if __name__ == '__main__':
     MRWordFreqCount.run()