import fnmatch
import os
import shutil

src_dir = "K:\\InfoDepth\\ARCHIVE\\RCVAgilOne\\"
dst_dir = "K:\\Archive\\"

for root, dirnames, filenames in os.walk(src_dir):
    for filename in fnmatch.filter(filenames, '*.csv'):
        shutil.copy(os.path.join(root, filename),dst_dir)


import sys
import os
from os import listdir

directory = "K:\\InfoDepth\\ARCHIVE\\RCVAgilOne\\"
test = os.listdir( directory )

for item in test:
    if item.endswith(".csv"):
        os.remove( os.path.join( directory, item ) )

