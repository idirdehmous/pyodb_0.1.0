#!/usr/bin/env python

import os, re, sys
import argparse

RE_INCLUDE = re.compile(r'^\s*#include\s+"(.*)"')
RE_CREATE_TABLE = re.compile(r'^\s*CREATE\s+TABLE\s+(\w+)(|\[1:\$(\w+)\])\s+AS', re.IGNORECASE)
RE_SET_VAR = re.compile(r'^\s*SET\s+\$(\w+)\s*=\s*(\w+)\s*;', re.IGNORECASE)
RE_IFDEF = re.compile(r'^\s*#ifdef\s+(\w+)')
RE_IFNDEF = re.compile(r'^\s*#ifndef\s+(\w+)')
RE_ENDIF = re.compile(r'^\s*#endif')

def parse(filename, parent_scope, definitions):

    tables = []
    scope = parent_scope
    skip = False

    for line in open(filename, "r"):

        match = RE_ENDIF.match(line)
        if match:
            skip = False
            continue

        match = RE_IFDEF.match(line)
        if match:
            d = match.group(1)
            if d not in definitions:
                skip = True
            continue

        match = RE_IFNDEF.match(line)
        if match:
            d = match.group(1)
            if d in definitions:
                skip = True
            continue

        if skip: # until closing #endif found
            continue

        match = RE_INCLUDE.match(line)
        if match:
            f = match.group(1)
            t = parse(f, scope, definitions)
            tables.extend(t)

        match = RE_SET_VAR.match(line)
        if match:
            k, v = match.group(1), match.group(2)
            if not v.isdigit():
                v = definitions[v]
            scope[k] = v

        match = RE_CREATE_TABLE.match(line)
        if match:
            table = match.group(1).lower()
            if match.group(3): # multi-table (e.g. enkf[1:$NMXENKF])
                size = int(scope[match.group(3)])
                for n in range(1, size + 1):
                    tables += ["%s_%d" % (table, n)]
            else: # single table
                tables += [table]

    return tables

#-----------------

ap = argparse.ArgumentParser()
ap.add_argument("ddl", nargs=1)
ap.add_argument("-D", help="definitions", action="append", default=[])
ap.add_argument("-I", help="includes", action="append", default=[])
args = vars(ap.parse_args())

definitions = {}

for d in args["D"]:
    k, v = d.split("=")
    definitions[k] = v

tables = parse(args["ddl"][0], {}, definitions)

sys.stdout.write(";".join(tables))
