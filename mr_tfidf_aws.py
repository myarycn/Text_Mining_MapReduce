# ################################################################## #
# File name: mr_tfidf_aws.py                                         #
# Author: Yixin Chen, Shareaholic, Inc.                              #
# Created: 07/20/2014                                                #
# Last modified: 07/23/2014                                          #
# Python Version: 2.7                                                #
# Note: this script implements TF-IDF for (term,doc) pair.           #
# ################################################################## #

from mrjob.protocol import JSONValueProtocol
from mrjob.job import MRJob
import ast
import urllib2

term_idf = {}
i = 1

class MRTFIDF(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, key, line):
        global i
        for term in line.split():
            yield {'term': term, 'docId': i}, 1
        i = i + 1

    # ################################################################## #
    # Function: reducer_init                                             #
    # Note: This function did initialization work before reduce,         #
    #       it reads the content of file on amazon s3                    #
    #       and buildup dictionary idfs, e.g. {term:'is', idf:0.301}     #
    # ################################################################## #
    def reducer_init(self):
        self.idfs = {}
        content = urllib2.urlopen('your filename')
        for line in content: 
            line = line.rstrip()
            # convert each line into dict
            term_idf = ast.literal_eval(line)
            self.idfs[term_idf['term']] = term_idf['idf']

    def reducer(self, term_docId, howmany):
        # sum(howmany) returns how many times one term appears in one document
        # and then compute tfidf score by applying equation tf * idf
        tfidf = sum(howmany) * self.idfs[term_docId['term']]
        #yield term_docId, TF-IDF
        yield None, {'term_docId': term_docId, 'tfidf': tfidf}


if __name__ == '__main__':
    MRTFIDF.run()
