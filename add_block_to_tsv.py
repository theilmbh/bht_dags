#!/usr/bin/env python

# Add Block to bht_birds.tsv
# Adds whichever block you specify to the birds tsv file

import update_tsv as utsv
import argparse
import os


def get_args():

	parser = argparse.ArgumentParser(description='Appends the appropriate data to the bird tsv file to start a new DAG for analysis')
	parser.add_argument('birdid', type=str, nargs='?', help='Bird ID (Including B prefix)')
	parser.add_argument('block', nargs='?', help='block to add')
	parser.add_argument('tsvfile', default='./test.tsv', nargs='?', help='Location of tsv file to append to')

	return parser.parse_args()

def main():
    args = get_args()
    tsvfile = os.path.abspath(args.tsvfile)

    utsv.update_tsv(args.birdid, args.block, tsvfile)

if __name__ == '__main__':
	main()



