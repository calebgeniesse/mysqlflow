#!/usr/bin/python

###############################################################################
### imports
###############################################################################
import os
import sys
import time
import subprocess
import mysqlflow.mysqlflow as mysqlflow


###############################################################################
### helper methods
###############################################################################
def usage():
    print "usage:", __file__, " <puzzle id list>"


def read_meta_db(file):
    sep = '\t'
    data = [l for l in open(file).readlines() if len(l) and l[0] != '#']
    data = [(sep.join([l[:7], l[7:].strip()]) 
            if l[:7].isdigit() and sep not in l else l) 
            for l in data]
    data = [s.strip().split(sep) for s in data]
    fields = data.pop(0)
    d_idx = {}
    for idx, field in enumerate(fields):
        d_idx[idx] = field
    d_all = []
    for line in data:
        d = {}
        for idx, val in enumerate(line):
            d[d_idx[idx]] = val
        d_all.append(d)
    # update nid if none specified
    if len(d_all) and 'nid' in d_all[0] and 'all' in d_all[0]['nid']:
        nids = read_nids('.puzzle-order.list.orig')
        d_all = [dict([(k,v) 
                 for k,v in d_all[0].items()+[('nid',nid)]]) 
                 for nid in nids]
    # update field_next_puzzle_value
    for idx, d in enumerate(d_all):
        try:
            d_all[idx]['field_next_puzzle_value'] = d_all[idx+1]['nid']
        except:
            d_all[idx]['field_next_puzzle_value'] = d_all[0]['nid']
    return fields, d_all


def read_nids(file):
    nids = [s.strip() for s in open(file).readlines()]
    return nids


def zip_puzzle_order(nids):
    return zip(nids, nids[1:]+nids[:1])


def save_nids(nids, file):
    with open(file, 'w') as f:
        f.write('\n'.join(map(str, nids)))
    return file


###############################################################################
### main methods
###############################################################################
def update_field(nid, table, field, value):
    # setup database connections
    sqlflow = mysqlflow.MySQLFlow()
    sqlflow.setup(
        id='dev',            
        host="host",         
        password="password"  
    )
    # make update
    sqlflow.connect(id='dev')
    sqlflow.USE('database_name')
    sqlflow.UPDATE(table).SET(field, value).WHERE('nid', nid)
    sqlflow.show()
    return sqlflow.execute()


def update_timestamp(nid):
    ts = int(time.time())
    status = update_field(nid, 'node', 'changed', ts) 
    status = status and update_field(nid, 'node_revisions', 'timestamp', ts)
    return status


###############################################################################
### methods
###############################################################################
def update_fields(*args, **kwargs):
     
    # parse puzzle id order from file
    fields, updates = read_meta_db(args[0])
    if not len(updates):
        exit()

    # run mysql query for each pid
    for idx, update in enumerate(updates):
        nid = update.pop('nid')
        table = 'content_type_puzzle'
        for field, value in update.iteritems():
            update_field(nid, table, field, value)
        update_timestamp(nid)
            
    return True 


###############################################################################
### main
###############################################################################
if __name__ == '__main__':

    kwargs = {}
    args = sys.argv[1:]

    sys.exit(update_fields(*args, **kwargs))
