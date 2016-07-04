#!/usr/bin/env python3
# coding: utf-8

import xml.etree.cElementTree as cetree
import argparse
import time

class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input', required=True, dest='input_file',
                        help='The input file to be processed')
    parser.add_argument('-o', '--output', required=True, dest='output_file',
                        help='The output file to save results to')
    parser.add_argument('-c', '--columns', required=True, dest='columns',
                        help='A comma delimited string denoting the column order')

    return parser.parse_args()

def get_data_c(fn, columns):
    res = ''
    cols = columns.split(',')

    for c in cols:
        res = res + c + '|'

    res = res[:-1] + '\n'
    yield res
    
    for event, elem in cetree.iterparse(fn):
        res = ''
        if elem.tag == "row":
            for c in cols:
                if c in elem.attrib:
                    res = res + elem.attrib[c].replace('\n','') + '|'
                else:
                    res = res + '|'
            res = res[:-1] + '\n'
            yield res
            elem.clear()

def save_data(fn, data):
    with open(fn, 'w') as f:
        for item in data:
            f.write(item.encode('utf-8'))

def main():
    args = parse_args()
    with Timer() as t:
        save_data(args.output_file, get_data_c(args.input_file, args.columns))
    print('Done! Work took %.03f sec.' % t.interval)

if __name__ == '__main__':
    main()
