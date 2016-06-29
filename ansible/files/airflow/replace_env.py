#!/usr/bin/env python
# Replace environment variables in the given file with their values.
# Usage: python replace_env.py input_file [ output_file ]
import os
import sys

nargs = len(sys.argv)
if nargs < 2 or nargs > 3:
	print "Usage: python {} input_file [ output_file ]".format(sys.argv[0])
	sys.exit(1)

input_file = sys.argv[1]
if nargs > 2:
	output_file = sys.argv[2]
else:
	output_file = input_file

with open(input_file, "r") as f:
    data = f.read()

expanded = os.path.expandvars(data)

with open(output_file, "w") as f:
	f.write(expanded)