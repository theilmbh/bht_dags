# Add Block to bht_birds.tsv
# Adds whichever block you specify to the birds tsv file

# args:  Bird, Block, tsvfile

import os

def update_tsv(bird, block, tsvfile):
	tsv_full_path = os.path.abspath(tsvfile)

	with open(tsv_full_path, 'a') as f:

		string_to_add = bird + "\t" + block + "\t" + "\n"
		f.write(string_to_add)
