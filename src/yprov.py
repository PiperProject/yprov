#!/usr/bin/env python

# -------------------------------------- #
import os, sys

# import sibling packages HERE!!!
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
if not adaptersPath in sys.path :
  sys.path.append( adaptersPath )

import Adapter

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../core" )
if not settingsPath in sys.path :
  sys.path.append( settingsPath )

import settings

# -------------------------------------- #


DEBUG = settings.DEBUG


###########
#  YPROV  #
###########
# given an arbitrary query, characterize the why (positive) provenance
# of all result tuples.
def yprov( nosql_type, cursor, query ) :

  # 1. transform query into datalog
  # 2. build provenance query
  # 3. convert provenance query into target db syntax
  # 4. issue both queries on db
  # 5. build provenance trees per result tuple
  # 6. allow materialization of trees per tuple upon request
  # 7. allow dump of all result tuple materializations

  return None


#########
#  EOF  #
#########
