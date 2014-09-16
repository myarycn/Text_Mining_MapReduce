# ################################################################## #
# File name: mr_per_term_idf.py                                      #
# Author: Yixin Chen, Shareaholic, Inc.                              #
# Created: 07/20/2014                                                #
# Last modified: 07/23/2014                                          #
# Python Version: 2.7                                                #
# Note: this script implements IDF for each term.                    #
# ################################################################## #

import math
from mrjob.protocol import *
from mrjob.job import MRJob

term_idf = {}
doc_num = 0

# ################################################################## #
# Note: this class is to get the total number of documents,          #
#       which is not used in TF-IDF calculation in terms of          #
#       calculation speed, but in case for future use.               #
# ################################################################## #
class MRDocNum(MRJob):

    def mapper(self, _, line):
        yield None, 1

    def reducer(self, key, number):
        global doc_num
        doc_num = sum(number)

# ################################################################## #
# Note: this class is to get the idf score for each term             #
# Manually configuration: replace the first integer within           #
#       the parenthesis of math.log10 with the total number of docs. #
#       Total # of docs can be get by running cmd "wc -l filenames"  #
# ################################################################## #
class MRTermIDF(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, line):
        # gather the unique terms for each line (which represent each document)
        for term in set(line.split()):
            yield term, 1

    def reducer(self, term, howmany):
        # sum(howmany) is to calculate how many times documents does term t appears
        idf = math.log10(8745 * 1.0 / sum(howmany))
        term_idf[term] = idf
        # yield (term, idf score) pair for TF-IDF calculation
        yield None, {'term': term, 'idf': idf}

if __name__ == '__main__':
    MRTermIDF.run()
