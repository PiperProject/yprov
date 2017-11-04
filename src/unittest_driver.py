#!/usr/bin/env python

import copy, os, pickledb, string, sys, unittest

#####################
#  UNITTEST DRIVER  #
#####################
def unittest_driver() :

  print
  print "************************************"
  print "*   RUNNING TEST SUITE FOR YPROV   *"
  print "************************************"
  print

  os.system( "python -m unittest Test_yprov.Test_yprov.test_example1" )
  os.system( "python -m unittest Test_yprov.Test_yprov.test_example2" )
  os.system( "python -m unittest Test_yprov.Test_yprov.test_example3" )
  os.system( "python -m unittest Test_yprov.Test_yprov.test_example4" )
  os.system( "python -m unittest Test_yprov.Test_yprov.test_example5" )


#########################
#  THREAD OF EXECUTION  #
#########################
unittest_driver()


#########
#  EOF  #
#########
