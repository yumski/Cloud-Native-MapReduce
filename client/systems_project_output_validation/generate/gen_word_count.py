import sys
import re
from os import listdir
from itertools import groupby
from operator import itemgetter

def read_words(file):
    for line in file:
        # split the line into words
        yield from re.findall(r'[a-z](?:[a-z\'‘’]*[a-z])?', line.lower())

def do_map(file):
    map_list = []
    for word in read_words(file):
        map_list.append(f'{word}\t1')
    return map_list
        
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


def read_map(file, separator='\t'):
    # input comes from STDIN (standard input)
    reduce_list = []
    data = read_mapper_output(file, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["&lt;current_word&gt;", "&lt;count&gt;"] items
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            reduce_list.append("%s%s%d" % (current_word, separator, total_count))
        except ValueError:
            # count was not a number, so silently discard this item
            pass
    return reduce_list

def write_output_file(wc_list):
    file = open("correct_counts.txt", 'w')
    for line in wc_list:
        file.write(line + "\n")
    file.close()

def main():
    text_files = listdir("texts")
    map_list = []
    for file_name in text_files:
        file = open("texts/" + file_name, "r")
        map_list += do_map(file)
        file.close()
    map_list.sort()
    map_list = read_map(map_list)
    write_output_file(map_list)

if __name__ == "__main__":
    main()
