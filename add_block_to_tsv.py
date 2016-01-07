# Add Block to bht_birds.tsv
# Adds whichever block you specify to the birds tsv file

import update_tsv as utsv
import argparse


def get_args():
    parser = argparse.ArgumentParser(description='Appends the appropriate data to the bird tsv file to start a new DAG for analysis')
    parser.add_argument('birdid', type=str, nargs='?', help='Bird ID (Including B prefix)')
    parser.add_argument('block', default='./', nargs='?', help='Directory in which to place raster plots')

    return parser.parse_args()