#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
#  Wikipedia(ja) plain texts to wakatigaki texts
#  Version 0.5 (September 10th, 2015)
# =============================================================================
#  2015 (c) Shinichiro Naoi (https://github.com/monapochi)
# =============================================================================
import sys
import os
import leveldb
import MeCab
from multiprocessing import Pool

def worker(k,v):
    print '[' + k + ']'
    mecab = MeCab.Tagger ('-Owakati')
    return  (k, mecab.parse(v))

def main():
    usage = 'usage: python JaWiki2Wakati.py <src_leveldb>'
    if len(sys.argv) != 2:
	print usage
	quit()
    name = sys.argv[1]
    if !os.path.isdir(name):
        print usage
        print name + 'is not found'
        quit()
    db = leveldb.DB(name)
    p = Pool(processes=2)
    c = 0
    for k,v in db:
        r = p.apply_async(worker, args = (k,v))
	kn,vn = r.get()
	db.put(kn, vn)
    p.close()
    p.join()
    db.close()
    print 'done'

if __name__ == '__main__':
    main()
