# ################################################################## #
# File name: boto_demo.py                                            #
# Author: Yixin Chen, Shareaholic, Inc.                              #
# Created: 07/20/2014                                                #
# Last modified: 07/23/2014                                          #
# Python Version: 2.7                                                #
# Note: this script implements TF-IDF for (term,doc) pair            #
#       using boto as intermediate result retrieving tool.           #
# ################################################################## #

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import subprocess

'''
Method takes two S3 addresses as parameters, in the form 
s3://bucket-name/full/key/path
where the first parameter is the input document, and the second parameter
is the output destination..
'''

# ################################################################## #
# Function: output_to_s3(s3_out)                                     #
# Args: s3_out, format s3://bucket_name/filename                     #
# inputs: s3_out stores the intermediate result                      #
# Note: This function stores the result from running                 #
#       subprocess command and writes it into file s3_out            #
#       s3_out should be public.                                     #
# ################################################################## #
def output_to_s3(s3_out):
    out_parts = s3_out.split('/', 3)
    out_bucket_name = out_parts[2]
    out_key_name = out_parts[3]

    # Make connection. Insert access key and secret key here, or see
    # https://code.google.com/p/boto/wiki/BotoConfig 
    # for other configuration options
    conn = S3Connection('your access key id', 'your secret access key')
    # Get the text from the HTML
    out_text = subprocess.Popen(['python', 'mr_per_term_idf.py','-r','emr','dataset/arts_after_stemming.nlp'], shell=False, stdout=subprocess.PIPE).communicate()[0]
    # Get bucket
    out_bucket = conn.get_bucket(out_bucket_name, validate=True)
    # Always make a Key object
    out_key = Key(out_bucket)
    # Set the path and file name
    out_key.key = out_key_name
    # Write to the file
    out_key.set_contents_from_string(out_text)


if __name__ == "__main__":
    s3_out = "path to your s3 file"
    # this is to write (term, idf) pair to s3_out
    output_to_s3(s3_out)
    # the following cmd is to calculate tfidf score for each (term, doc) pair.
    print subprocess.Popen(['python','mr_tfidf_aws.py','-r','emr','dataset/arts_after_stemming.nlp'], shell=False, stdout=subprocess.PIPE).communicate()[0]
