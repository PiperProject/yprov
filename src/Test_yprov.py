#!/usr/bin/env python

'''
Test_yprov.py
'''


#############
#  IMPORTS  #
#############
# standard python packages
import inspect, logging, os, pickledb, sqlite3, sys, unittest
from StringIO import StringIO

import YProv


SAVEPATH = "/Users/KsComp/projects/piper/src/packages/yprov/src"


################
#  TEST QUEST  #
################
class Test_yprov( unittest.TestCase ) :

  logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.DEBUG )
  #logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.INFO )


  ###############
  #  EXAMPLE 5  #
  ###############
  # tests two queries with negation
  def test_example5( self ) :

    test_id = "test_example5"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_yprov.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ 0, [ 1, 3 ] ] )
    dbInst.set( "c", [ [ 1, 2 ], [ 4, 5 ] ] )

    # --------------------------------------------------------------- #
    yp = YProv.YProv( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated YProv instance '" + str( yp ) )

    # set original queries
    query1 = "a(X,Y) :- b(X,Y), notin d(X,Y) ;"
    yp.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "d(X,Y) :- c(X,Y) ;"
    yp.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    schema = { "a":["int","int"], "b":["int","int"],"c":["int","int"], "d":["int","int"] }

    for rel in schema :
      yp.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    # --------------------------------------------------------------- #
    # test evaluation results
    logging.debug( "  " + test_id + " : calling 'run' on YProv instance." )
    allProgramData = yp.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expectedProvQuery1     = "a_prov0(X,Y) :- b(X,Y), notin d(X,Y);"
    expectedProvQuery2     = "d_prov1(X,Y) :- c(X,Y);"
    expected_program       = ['define(a,{int, int});', \
                              'define(b,{int, int});', \
                              'define(d,{int, int});', \
                              'define(c,{int, int});', \
                              'define(a_prov0,{int, int});', \
                              'define(d_prov1,{int, int});', \
                              'b(0,1);', \
                              'b(0,3);', \
                              'c(1,4);', \
                              'c(2,4);', \
                              'c(1,5);', \
                              'c(2,5);', \
                              query1, \
                              query2, \
                              expectedProvQuery1, \
                              expectedProvQuery2 ]
    expected_table_list    = ['a', 'b', 'd', 'c', 'a_prov0', 'd_prov1']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '0,1', \
                              '0,3', \
                              '---------------------------', \
                              'b', \
                              '0,1', \
                              '0,3', \
                              '---------------------------', \
                              'd', \
                              '2,4', \
                              '2,5', \
                              '1,4', \
                              '1,5', \
                              '---------------------------', \
                              'c', \
                              '2,4', \
                              '2,5', \
                              '1,4', \
                              '1,5', \
                              '---------------------------', \
                              'a_prov0', \
                              '0,1', \
                              '0,3', \
                              '---------------------------', \
                              'd_prov1', \
                              '2,4', \
                              '2,5', \
                              '1,4', \
                              '1,5' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # --------------------------------------------------------------- #
    # test provenance graph results

    # test 0
    logging.debug( "  " + test_id + " : calling 'generate_provenance' on YProv instance." )
    graphData      = yp.generate_provenance( "a", [ 0,1 ], SAVEPATH + "/" + test_id )

    nodeSet = graphData[0]
    edgeSet = graphData[1]

    actual_nodeset = []
    for node in nodeSet :
      actual_nodeset.append( node.get_name() )
    
    actual_edgeset = []
    for edge in edgeSet :
      src = edge.get_source()
      des = edge.get_destination()
      actual_edgeset.append( [src,des] )
    
    expected_nodeset = ['"G_a(0,1)"', \
                        '"R_a_prov0(0,1)"', \
                        '"G_notin d(0,1)"', \
                        '"G_b(0,1)"', \
                        '"F_b(0,1)"']
    expected_edgeset = [['"G_a(0,1)"', '"R_a_prov0(0,1)"'], \
                        ['"R_a_prov0(0,1)"', '"G_notin d(0,1)"'], \
                        ['"R_a_prov0(0,1)"', '"G_b(0,1)"'], \
                        ['"G_b(0,1)"', '"F_b(0,1)"']]

    self.assertEqual( actual_nodeset, expected_nodeset )
    self.assertEqual( actual_edgeset, expected_edgeset )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 4  #
  ###############
  # tests two queries with wildcards
  def test_example4( self ) :

    test_id = "test_example4"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_yprov.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ 0, [ "str1", "str2" ] ] )
    dbInst.set( "c", [ [ 1, 2 ], [ "str2", "str3" ] ] )

    # --------------------------------------------------------------- #
    yp = YProv.YProv( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated YProv instance '" + str( yp ) )

    # set original queries
    query1 = "a(X) :- b(_,X) ;"
    yp.setQuery( query1 )
    logging.debug( "  " + test_id + " : set query '" + query1 + "' to db instance." )

    query2 = "a(X) :- c(_,X) ;"
    yp.setQuery( query2 )
    logging.debug( "  " + test_id + " : set query '" + query2 + "' to db instance." )

    schema = { "a":["string"], "b":["int","string"],"c":["int","string"] }

    for rel in schema :
      yp.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    # --------------------------------------------------------------- #
    # test evaluation results
    logging.debug( "  " + test_id + " : calling 'run' on YProv instance." )
    allProgramData = yp.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expectedProvQuery1     = "a_prov0(X) :- b(_,X);"
    expectedProvQuery2     = "a_prov1(X) :- c(_,X);"
    expected_program       = ['define(a,{string});', \
                              'define(b,{int, string});', \
                              'define(c,{int, string});', \
                              'define(a_prov0,{string});', \
                              'define(a_prov1,{string});', \
                              'b(0,"str1");', \
                              'b(0,"str2");', \
                              'c(1,"str2");', \
                              'c(2,"str2");', \
                              'c(1,"str3");', \
                              'c(2,"str3");', \
                              query1, \
                              query2, \
                              expectedProvQuery1, \
                              expectedProvQuery2 ]
    expected_table_list    = ['a', 'b', 'c', 'a_prov0', 'a_prov1']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'str1', \
                              'str2', \
                              'str3', \
                              '---------------------------', \
                              'b', \
                              '0,str1', \
                              '0,str2', \
                              '---------------------------', \
                              'c', \
                              '2,str2', \
                              '2,str3', \
                              '1,str2', \
                              '1,str3', \
                              '---------------------------', \
                              'a_prov0', \
                              'str1', \
                              'str2', \
                              '---------------------------', \
                              'a_prov1', \
                              'str2', \
                              'str3' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # --------------------------------------------------------------- #
    # test provenance graph results

    # test 0
    logging.debug( "  " + test_id + " : calling 'generate_provenance' on YProv instance." )
    graphData      = yp.generate_provenance( "a", [ "str2" ], SAVEPATH + "/" + test_id + "_0" )

    nodeSet = graphData[0]
    edgeSet = graphData[1]

    actual_nodeset = []
    for node in nodeSet :
      actual_nodeset.append( node.get_name() )
    
    actual_edgeset = []
    for edge in edgeSet :
      src = edge.get_source()
      des = edge.get_destination()
      actual_edgeset.append( [src,des] )
    
    expected_nodeset = ['"G_a(str2)"', \
                        '"R_a_prov0(str2)"', \
                        '"G_b(_,str2)"', \
                        '"G_b(0,str2)"', \
                        '"F_b(0,str2)"', \
                        '"R_a_prov1(str2)"', \
                        '"G_c(_,str2)"', \
                        '"G_c(2,str2)"', \
                        '"F_c(2,str2)"', \
                        '"G_c(1,str2)"', \
                        '"F_c(1,str2)"']
    expected_edgeset = [['"G_a(str2)"', '"R_a_prov0(str2)"'], \
                        ['"R_a_prov0(str2)"', '"G_b(_,str2)"'], \
                        ['"G_b(_,str2)"', '"G_b(0,str2)"'], \
                        ['"G_b(0,str2)"', '"F_b(0,str2)"'], \
                        ['"G_a(str2)"', '"R_a_prov1(str2)"'], \
                        ['"R_a_prov1(str2)"', '"G_c(_,str2)"'], \
                        ['"G_c(_,str2)"', '"G_c(2,str2)"'], \
                        ['"G_c(2,str2)"', '"F_c(2,str2)"'], \
                        ['"G_c(_,str2)"', '"G_c(1,str2)"'], \
                        ['"G_c(1,str2)"', '"F_c(1,str2)"']]

    self.assertEqual( actual_nodeset, expected_nodeset )
    self.assertEqual( actual_edgeset, expected_edgeset )

    # test 1
    logging.debug( "  " + test_id + " : calling 'generate_provenance' on YProv instance." )
    graphData      = yp.generate_provenance( "a", [ "str1" ], SAVEPATH + "/" + test_id + "_1" )

    nodeSet = graphData[0]
    edgeSet = graphData[1]

    actual_nodeset = []
    for node in nodeSet :
      actual_nodeset.append( node.get_name() )
    
    actual_edgeset = []
    for edge in edgeSet :
      src = edge.get_source()
      des = edge.get_destination()
      actual_edgeset.append( [src,des] )
    
    expected_nodeset = ['"G_a(str1)"', \
                        '"R_a_prov0(str1)"', \
                        '"G_b(_,str1)"', \
                        '"G_b(0,str1)"', \
                        '"F_b(0,str1)"']
    expected_edgeset = [['"G_a(str1)"', '"R_a_prov0(str1)"'], \
                        ['"R_a_prov0(str1)"', '"G_b(_,str1)"'], \
                        ['"G_b(_,str1)"', '"G_b(0,str1)"'], \
                        ['"G_b(0,str1)"', '"F_b(0,str1)"']]

    self.assertEqual( actual_nodeset, expected_nodeset )
    self.assertEqual( actual_edgeset, expected_edgeset )
    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 3  #
  ###############
  # tests one query with wildcards
  def test_example3( self ) :

    test_id = "test_example3"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_yprov.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ 0, [ "str1", "str2" ] ] )
    dbInst.set( "c", [ [ 1, 2 ], [ "str2", "str3" ] ] )

    # --------------------------------------------------------------- #
    yp = YProv.YProv( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated YProv instance '" + str( yp ) )

    # set original query
    query = "a(X,Y) :- b(_,X), c(_,Y) ;"
    yp.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["string","string"], "b":["int","string"],"c":["int","string"] }

    for rel in schema :
      yp.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    # --------------------------------------------------------------- #
    # test evaluation results
    logging.debug( "  " + test_id + " : calling 'run' on YProv instance." )
    allProgramData = yp.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expectedProvQuery      = "a_prov0(X,Y) :- b(_,X),c(_,Y);"
    expected_program       = ['define(a,{string, string});', \
                              'define(b,{int, string});', \
                              'define(c,{int, string});', \
                              'define(a_prov0,{string, string});', \
                              'b(0,"str1");', \
                              'b(0,"str2");', \
                              'c(1,"str2");', \
                              'c(2,"str2");', \
                              'c(1,"str3");', \
                              'c(2,"str3");', \
                              query, \
                              expectedProvQuery ]
    expected_table_list    = ['a', 'b', 'c', 'a_prov0']
    expected_results_array = ['---------------------------', \
                              'a', \
                              'str1,str3', \
                              'str2,str3', \
                              'str1,str2', \
                              'str2,str2', \
                              '---------------------------', \
                              'b', \
                              '0,str1', \
                              '0,str2', \
                              '---------------------------', \
                              'c', \
                              '2,str2', \
                              '2,str3', \
                              '1,str2', \
                              '1,str3', \
                              '---------------------------', \
                              'a_prov0', \
                              'str1,str3', \
                              'str2,str3', \
                              'str1,str2', \
                              'str2,str2' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # --------------------------------------------------------------- #
    # test provenance graph results
    logging.debug( "  " + test_id + " : calling 'generate_provenance' on YProv instance." )
    graphData      = yp.generate_provenance( "a", [ "str1", "str2" ], SAVEPATH + "/" + test_id )

    nodeSet = graphData[0]
    edgeSet = graphData[1]

    actual_nodeset = []
    for node in nodeSet :
      actual_nodeset.append( node.get_name() )
    
    actual_edgeset = []
    for edge in edgeSet :
      src = edge.get_source()
      des = edge.get_destination()
      actual_edgeset.append( [src,des] )
    
    expected_nodeset = ['"G_a(str1,str2)"', \
                        '"R_a_prov0(str1,str2)"', \
                        '"G_c(_,str2)"', \
                        '"G_c(2,str2)"', \
                        '"F_c(2,str2)"', \
                        '"G_c(1,str2)"', \
                        '"F_c(1,str2)"', \
                        '"G_b(_,str1)"', \
                        '"G_b(0,str1)"', \
                        '"F_b(0,str1)"']
    expected_edgeset = [['"G_a(str1,str2)"', '"R_a_prov0(str1,str2)"'], \
                        ['"R_a_prov0(str1,str2)"', '"G_c(_,str2)"'], \
                        ['"G_c(_,str2)"', '"G_c(2,str2)"'], \
                        ['"G_c(2,str2)"', '"F_c(2,str2)"'], \
                        ['"G_c(_,str2)"', '"G_c(1,str2)"'], \
                        ['"G_c(1,str2)"', '"F_c(1,str2)"'], \
                        ['"R_a_prov0(str1,str2)"', '"G_b(_,str1)"'], \
                        ['"G_b(_,str1)"', '"G_b(0,str1)"'], \
                        ['"G_b(0,str1)"', '"F_b(0,str1)"']]

    self.assertEqual( actual_nodeset, expected_nodeset )
    self.assertEqual( actual_edgeset, expected_edgeset )

    # ---------------------------- #
    dbInst.deldb()


  ###############
  #  EXAMPLE 2  #
  ###############
  # tests one query with wildcards
  # fails because bad provenance tuple request
  def test_example2( self ) :

    test_id = "test_example2"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_yprov.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", [ 0, [ 1, 2 ] ] )
    dbInst.set( "c", [ 1, [ 2, 3 ] ] )

    # --------------------------------------------------------------- #
    yp = YProv.YProv( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated YProv instance '" + str( yp ) )

    # set original query
    query = "a(X,Y) :- b(_,X), c(_,Y) ;"
    yp.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["int","int"], "b":["int","int"],"c":["int","int"] }

    for rel in schema :
      yp.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    # --------------------------------------------------------------- #
    # test evaluation results
    logging.debug( "  " + test_id + " : calling 'run' on YProv instance." )
    allProgramData = yp.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expectedProvQuery      = "a_prov0(X,Y) :- b(_,X),c(_,Y);"
    expected_program       = ['define(a,{int, int});', \
                              'define(b,{int, int});', \
                              'define(c,{int, int});', \
                              'define(a_prov0,{int, int});', \
                              'b(0,1);', \
                              'b(0,2);', \
                              'c(1,2);', \
                              'c(1,3);', \
                              query, \
                              expectedProvQuery ]
    expected_table_list    = ['a', 'b', 'c', 'a_prov0']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '2,2', \
                              '1,3', \
                              '2,3', \
                              '1,2', \
                              '---------------------------', \
                              'b', \
                              '0,2', \
                              '0,1', \
                              '---------------------------', \
                              'c', \
                              '1,3', \
                              '1,2', \
                              '---------------------------', \
                              'a_prov0', \
                              '2,2', \
                              '1,3', \
                              '2,3', \
                              '1,2' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # --------------------------------------------------------------- #
    # test provenance graph results
    logging.debug( "  " + test_id + " : calling 'generate_provenance' on YProv instance." )
    with self.assertRaises(SystemExit) as cm:
      graphData      = yp.generate_provenance( "a", [ 0, 1 ], SAVEPATH + "/" + test_id )
    self.assertEqual( cm.exception.code, "ERROR : input data tuple '[0, 1]' not in the evaluation results for relation 'a'" )

    # ---------------------------- #
    dbInst.deldb()



  ###############
  #  EXAMPLE 1  #
  ###############
  # tests one query
  def test_example1( self ) :

    test_id = "test_example1"

    # --------------------------------------------------------------- #
    logging.info( "  " + test_id + ": initializing pickledb instance." )
    dbInst = pickledb.load( "./test_yprov.db", False )

    # --------------------------------------------------------------- #
    dbInst.set( "b", { 0:"str10" } )
    dbInst.set( "c", { "str10":1 } )

    # --------------------------------------------------------------- #
    yp = YProv.YProv( "pickledb", dbInst )
    logging.debug( "  " + test_id + " : instantiated YProv instance '" + str( yp ) )

    # set original query
    query = "a(X,Y) :- b(X,Z), c(Z,Y) ;"
    yp.setQuery( query )
    logging.debug( "  " + test_id + " : set query '" + query + "' to db instance." )

    schema = { "a":["int","int"], "b":["int","string"],"c":["string","int"] }

    for rel in schema :
      yp.setSchema( rel, schema[rel] )
      logging.debug( "  " + test_id + " : set relation '" + rel + "' to schema " + str( schema[rel] ) )

    # --------------------------------------------------------------- #
    # test evaluation results
    logging.debug( "  " + test_id + " : calling 'run' on YProv instance." )
    allProgramData = yp.run()

    actual_program       = allProgramData[0]
    actual_table_list    = allProgramData[1]
    actual_results_array = allProgramData[2]

    expectedProvQuery      = "a_prov0(X,Y,Z) :- b(X,Z),c(Z,Y);"
    expected_program       = ['define(a,{int, int});', \
                              'define(b,{int, string});', \
                              'define(c,{string, int});', \
                              'define(a_prov0,{int, int, string});', \
                              'b(0,"str10");', \
                              'c("str10",1);', \
                              query, \
                              expectedProvQuery ]
    expected_table_list    = ['a', 'b', 'c', 'a_prov0']
    expected_results_array = ['---------------------------', \
                              'a', \
                              '0,1', \
                              '---------------------------', \
                              'b', \
                              '0,str10', \
                              '---------------------------', \
                              'c', \
                              'str10,1', \
                              '---------------------------', \
                              'a_prov0', \
                              '0,1,str10' ]

    self.assertEqual( actual_program, expected_program )
    self.assertEqual( actual_table_list, expected_table_list )
    self.assertEqual( actual_results_array, expected_results_array )

    # --------------------------------------------------------------- #
    # test provenance graph results
    logging.debug( "  " + test_id + " : calling 'generate_provenance' on YProv instance." )
    graphData      = yp.generate_provenance( "a", [ 0, 1 ], SAVEPATH + "/" + test_id )

    nodeSet = graphData[0]
    edgeSet = graphData[1]

    actual_nodeset = []
    for node in nodeSet :
      actual_nodeset.append( node.get_name() )

    actual_edgeset = []
    for edge in edgeSet :
      src = edge.get_source()
      des = edge.get_destination()
      actual_edgeset.append( [src,des] )

    expected_nodeset = ['"G_a(0,1)"', \
                        '"R_a_prov0(0,1,str10)"', \
                        '"G_c(str10,1)"', \
                        '"F_c(str10,1)"', \
                        '"G_b(0,str10)"', \
                        '"F_b(0,str10)"']
    expected_edgeset = [['"G_a(0,1)"', '"R_a_prov0(0,1,str10)"'], \
                        ['"R_a_prov0(0,1,str10)"', '"G_c(str10,1)"'], \
                        ['"G_c(str10,1)"', '"F_c(str10,1)"'], \
                        ['"R_a_prov0(0,1,str10)"', '"G_b(0,str10)"'], \
                        ['"G_b(0,str10)"', '"F_b(0,str10)"']]

    self.assertEqual( actual_nodeset, expected_nodeset )
    self.assertEqual( actual_edgeset, expected_edgeset )

    # ---------------------------- #
    dbInst.deldb()


#########
#  EOF  #
#########
