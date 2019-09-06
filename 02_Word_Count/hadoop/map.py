#!/usr/bin/python
import sys
import os

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
    
    l = line.strip().split()
    
    for word in l:

        try:
        	input_file = os.environ['mapreduce_map_input_file']
        except KeyError:
            input_file = os.environ['map_input_file']
        # input_file[-5:] province file name mapping
        word_and_file = word + "&" + input_file[-5:]
        
        # output goes to STDOUT (stream data that the program writes)
        print "%s\t%d" %( word_and_file, 1 )