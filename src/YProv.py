#!/usr/bin/env python

##########################################################################
# YProv usage notes:
#
# 1. Input data strings cannot have white space.
#
##########################################################################

# -------------------------------------- #
import logging, os, pydot, string, sys

# import sibling packages HERE!!!

# adapters path
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
if not adaptersPath in sys.path :
  sys.path.append( adaptersPath )
import Adapter

# make sure quest exists!
# quest pathi
try :
  questPath  = os.path.abspath( __file__ + "/../../../../packages/quest/src" )
  if not questPath in sys.path :
    sys.path.append( questPath )
  import Quest
except ImportError :
  sys.exit( "ERROR : YProv depends upon the Quest package from Piper.\nPlease install Quest from the Piper package index, e.g. 'python piper.py install quest'\naborting..." )

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../core" )
if not settingsPath in sys.path :
  sys.path.append( settingsPath )
import settings

# -------------------------------------- #

DEBUG = settings.DEBUG

class YProv( object ) :

  ################
  #  ATTRIBUTES  #
  ################
  nosql_type          = None   # the type of nosql database under consideration
  dbcursor            = None   # pointer to target database instance
  q                   = None   # pointer to quest instance

  final_program       = None  # final c4 program
  final_table_list    = None  # table list from c4 program
  final_results_array = None  # evaluation results in array form
  final_results_dict  = None  # evaluation results in dict form

  prov_rule_counter   = 0     # counts number of provenance rules in program

  ##########
  #  INIT  #
  ##########
  def __init__( self, nosql_type, dbcursor ) :

    self.nosql_type = nosql_type
    self.dbcursor   = dbcursor

    self.q = Quest.Quest( self.nosql_type, self.dbcursor )
    logging.debug( "  ...instantiated Quest instance '" + str( self.q ) + "'" )


  #########################
  #  GENERATE PROVENANCE  #
  #########################
  # rel is a string
  # dataTup is an array
  # generate the postive provenance tree for the given relation and data tuple
  def generate_provenance( self, rel, dataTup, savePath ) :

    # --------------------------------- #
    # verify data tuple is in the evaluation results
    # for the given relation.

    if not self.verifyRelTup( rel, dataTup ) :
      sys.exit( "ERROR : input data tuple '" + str( dataTup ) + "' not in the evaluation results for relation '" + str( rel )+ "'" )

    # --------------------------------- #
    # generate provenance graph data

    graphData = self.get_prov_tree( rel, dataTup, [] )
    nodeSet   = graphData[0]
    edgeSet   = graphData[1]

    #print "nodeSet : " + str( nodeSet )
    #for node in nodeSet :
    #  print "node = " + str( node.get_name() )

    #print "edgeSet : " + str( edgeSet )
    #for edge in edgeSet :
    #  print "src = " + str( edge.get_source() ) + ", dest = " + str( edge.get_destination() )

    # --------------------------------- #
    # create graph
    graph = pydot.Dot( graph_type = 'digraph', strict=True ) # strict => ignore duplicate edges

    # add nodes :
    for n in nodeSet :
      graph.add_node( n )

    # add edges
    for e in edgeSet :
      graph.add_edge( e )

    # --------------------------------- #
    # output png

    print "Saving prov tree render to " + str( savePath )
    graph.write_png( savePath + ".png" )

    return graphData


  ###################
  #  GET PROV TREE  #
  ###################
  def get_prov_tree( self, rel, dataTup, parentNodes ) :

    logging.debug( "  GET PROV TREE : rel         = " + str( rel ) )
    logging.debug( "  GET PROV TREE : dataTup     = " + str( dataTup ) )
    logging.debug( "  GET PROV TREE : parentNodes = " + str( parentNodes ) )

    nodeSet = []
    edgeSet = []

    # ----------------------------------------------------- #
    # RECURSIVE CASE : data tuple contains wildcards
    if "_" in dataTup :

      thisNode = self.createNode( rel, dataTup, "goal" )
      nodeSet.append( thisNode )
      for node in parentNodes :
        edgeSet.append( self.createEdge( node, thisNode ) )

      resolvedTups = self.resolveWildcards( rel, dataTup )

      for tup in resolvedTups :
        graphData = self.get_prov_tree( rel, tup, [ thisNode ] )
        nodeSet.extend( graphData[0] )
        edgeSet.extend( graphData[1] )

    # ----------------------------------------------------- #
    # BASE CASE : relation has only an edb definition
    elif self.isEDBOnly( rel ) :

      # +++++++++++++++++++++++++++++++++++++++++++++++++++++ #
      thisGoalNode = self.createNode( rel, dataTup, "goal" )
      nodeSet.append( thisGoalNode )
      for node in parentNodes :
        edgeSet.append( self.createEdge( node, thisGoalNode ) )

      # only positive provenance
      if not "notin" in rel :
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++ #
        thisNode = self.createNode( rel, dataTup, "fact" )
        nodeSet.append( thisNode )
        edgeSet.append( self.createEdge( thisGoalNode, thisNode ) )


    # ----------------------------------------------------- #
    # RECURSIVE CASE : relation has an idb definition
    else :

      # +++++++++++++++++++++++++++++++++++++++++++++++++++++ #
      # grab all provenance idb rules for this relation

      idbList = []
      for q in self.q.queryList :
         q_goal_name = self.getGoalName( q )
         if "_prov" in q_goal_name and q_goal_name[:len(rel)] == rel :
           idbList.append( q )

      #print "idList = " + str( idbList )

      # +++++++++++++++++++++++++++++++++++++++++++++++++++++ #
      # find the provenance versions of the 
      # idb rule(s) responsible for firing this tuple

      firingRules = []
      for q in idbList :
        if self.isCandidateFiringRule( q, dataTup ) :
          firingRules.append( q )

      #print "firingRules = " + str( firingRules )

      # +++++++++++++++++++++++++++++++++++++++++++++++++++++ #
      # create goal node for relation and data tuple

      if len( firingRules ) > 0 :
        thisNode =  self.createNode( rel, dataTup, "goal" )
        nodeSet.append( thisNode )

        # +++++++++++++++++++++++++++++++++++++++++++++++++++++ #
        # create an edge between this node and all parent nodes
        for node in parentNodes :
          edgeSet.append( self.createEdge( node, thisNode ) )

        # +++++++++++++++++++++++++++++++++++++++++++++++++++++ #
        # create edge nodes between the goal node and all relevant idb provenance
        # _RULES_.
        # the rule nodes become the parent nodes in the appropriate subsequent
        # recusrive calls. 
        # launch recursive calls on each idb subgoals grounded in the relevant tuples.

        for fr in firingRules :

          # get all aligned prov tuples
          provTuples = self.getProvTuples( fr, dataTup )

          for tup in provTuples :

            # create firing rule node
            frNode = self.createNode( self.getGoalName( fr ), "[" + tup + "]", "rule"  )
            nodeSet.append( frNode )

            # create firing rule edge
            edgeSet.append( self.createEdge( thisNode, frNode ) )

            # iterate over grounded subgoals
            subgoalData = self.mapTupData( fr, tup )
            for subName in subgoalData :
              firingTup = subgoalData[ subName ]
              #print firingTup

              # get the subgraph
              graphData = self.get_prov_tree( subName, firingTup, [ frNode ] )
              nodeSet.extend( graphData[0] )
              edgeSet.extend( graphData[1] )

    print "returning [ nodeSet, edgeSet ] = " + str( [ nodeSet, edgeSet ] )
    return [ nodeSet, edgeSet ]


  #######################
  #  RESOLVE WILDCARDS  #
  #######################
  # given a data tuple with wildcards, return the list of tuples in the relation
  # corresponding with the wildcard tuple.
  def resolveWildcards( self, rel, dataTup ) :

    logging.debug( "  RESOLVE WILDCARDS : rel     = " + rel )
    logging.debug( "  RESOLVE WILDCARDS : dataTup = " + str( dataTup ) )

    tupList = []
    for record in self.final_results_dict[ rel ] :
      #print "record = " + str( record )
      record = record.split( "," )
      flag = True
      for i in range( 0, len(record) ) :
        component = record[ i ]
        data      = dataTup[ i ]
        if data == "_" :
          pass
        elif not component == data :
          flag = False
          break
      if flag :
        tupList.append( record )

    logging.debug( "  RESOLVE WILDCARDS : tupList = " + str( tupList ) )
    return tupList


  ##################
  #  MAP TUP DATA  #
  ##################
  # map data tuple values to tuples of data in subgoals
  def mapTupData( self, firingRule, provDataTup ) :

    provDataTup = provDataTup.split( "," )
    subgoalList = self.getSubgoals( firingRule )

    # generate dict mapping universal attributes in the firing rule to data from dataTup
    goalAtts = self.getGoalAtts( firingRule )
    univDataMap = {}
    for i in range( 0, len( goalAtts ) ) :
      att                = goalAtts[ i ]
      univDataMap[ att ] = provDataTup[ i ]

    subgoalMap = {}
    for subgoal in subgoalList :
      subName = subgoal[0]
      subAtts = subgoal[1]
      dataList = []
      for att in subAtts :
        if att == "_" :
          dataList.append( "_" )
        else :
          dataList.append( univDataMap[ att ] )
      subgoalMap[ subName ] = dataList 

    #print subgoalMap
    return subgoalMap


  #####################
  #  GET PROV TUPLES  #
  #####################
  # return the set of tuples on the provenance relation which could
  # have contributed to the appearance of datatTup in the original 
  # relation
  def getProvTuples( self, firingRule, dataTup ) :

    validTups = []

    # get goal name
    provGoalName = self.getGoalName( firingRule )

    #print firingRule
    #print dataTup
    #print provGoalName

    # collect aligned data tuples
    for tup in self.final_results_dict[ provGoalName ] :
      if self.provAlignment( tup, dataTup ) :
        #print "prov tup '" + str( tup ) + "' aligns with dataTup '" + str( dataTup ) + "'"
        validTups.append( tup )

    #print validTups
    logging.debug( "  GET PROV TUPLES : validTups = " + str( validTups ) )

    if len( validTups ) > 0 :
      return validTups

    else :
      sys.exit( "ERROR : no tuples the firing provenance rule for the current relation align with data tuple '" + str( dataTup ) + "'" )


  ##############################
  #  IS CANDIDATE FIRING RULE  #
  ##############################
  # check if the given idb rule could have fired dataTup into the target relation.
  def isCandidateFiringRule( self, idbRule, dataTup ) :

    # -------------------------------------------------------------------------- #
    # get the corresponding provenance rule

    provMatch = self.getProvMatch( idbRule )
    #print "provMatch = " + provMatch

    # -------------------------------------------------------------------------- #
    # check if the dataTup matches any tuples in the provenance table.
    # this only works if the ordering of universal variables remains fixed
    # between the original and provenance rules such that the list of existential
    # varables appears after the list of universal variables in the provenance 
    # rules.

    provGoal = self.getGoalName( provMatch )
    #print "provgoal = " + provGoal

    for tup in self.final_results_dict[ provGoal ] :
      if self.provAlignment( tup, dataTup ) :
        return True

    return False


  ####################
  #  PROV ALIGNMENT  #
  ####################
  # determine if the first N entries in the provenance tuple are identical (align)
  # with the first N entries in the data tuple, where N is the length of the 
  # data tuple.
  def provAlignment( self, provTup, dataTup ) :

    # covert dataTup to string and split into a list of strings
    dataTup = str( dataTup )
    dataTup = dataTup.translate( None, string.whitespace )
    dataTup = dataTup.replace( "[", "" )
    dataTup = dataTup.replace( "]", "" )
    dataTup = dataTup.replace( '"', '' )
    dataTup = dataTup.replace( "'", "" )
    dataTup = dataTup.split( "," )

    # split provenance string into list of strings
    provTup = provTup.split( "," )

    logging.debug( "  PROV ALIGNMENT : provTup = " + str( provTup ) )
    logging.debug( "  PROV ALIGNMENT : dataTup = " + str( dataTup ) )

    for i in range( 0, len( dataTup ) ) :
        dData = dataTup[ i ]
        pData = provTup[ i ]
        #print "dData '" + dData + "', pData '" + pData + "'"
        if dData == pData :
          pass
        else :
          return False

    return True


  ####################
  #  GET PROV MATCH  #
  ####################
  def getProvMatch( self, idbRule ) :

    for query in self.q.queryList :
      if "_prov" in self.getGoalName( query ) :
        qBody   = self.getBody( query )
        idbBody = self.getBody( idbRule )

        if qBody == idbBody :
          return query

    sys.exit( "ERROR : idb rule '" + idbRule + "' has no corresponding provenance rule...aborting" )


  ##############
  #  GET BODY  #
  ##############
  def getBody( self, query ) :

    body = query.replace( "notin", "___NOTIN___" )
    body = body.replace( ";", "" )
    body = body.translate( None, string.whitespace )
    body = body.split( ":-" )
    body = body[1]
    body = body.replace( "___NOTIN___", " notin " )

    return body


  #################
  #  CREATE NODE  #
  #################
  def createNode( self, rel, dataTup, nodeType ) :

    label = rel + str( dataTup )
    label = label.replace( "notin", "___NOTIN___" )
    label = label.replace( "'", "" )
    label = label.replace( '"', '' )
    label = label.replace( '[', '(' )
    label = label.replace( ']', ')' )
    label = label.translate( None, string.whitespace )
    label = label.replace( "___NOTIN___", "notin " )
    logging.debug( "  CREATE NODE : label = " + label )

    # CASE : fact
    if nodeType == "fact" :
      return pydot.Node( "F_"+label, shape='cylinder', margin=0.1 )

    # CASE : goal
    elif nodeType == "goal" :
      return pydot.Node( "G_"+label, shape='oval', margin=0.1 )

    # CASE : rule
    elif nodeType == "rule" :
      return pydot.Node( "R_"+label, shape='box', margin=0.1 )

    # CASE : wtf???
    else :
      sys.exit( "ERROR : unrecognized node type...aborting" )



  #################
  #  CREATE EDGE  #
  #################
  # create an edge from e1 to e2
  def createEdge( self, e1, e2 ) :
    logging.debug( "  CREATE EDGE : src = " + str( e1.get_name() ) + ", dest = " + str( e2.get_name() )  )
    return pydot.Edge( e1, e2 )


  #################
  #  IS EDB ONLY  #
  #################
  # the query list only contains rules. 
  # therefore, relations with only edb definitions
  # will not appear as rule goals.
  def isEDBOnly( self, rel ) :

    for q in self.q.queryList :
      goalName = self.getGoalName( q )
      if goalName == rel :
        return False
    return True


  ####################
  #  VERIFY REL TUP  #
  ####################
  # make sure the given data tuple is in the results of the specified relation
  # return True if tuple found in results, False otherwise
  def verifyRelTup( self, rel, dataTup ) :

    # convert dataTup into list of strings
    dataTup = str( dataTup )
    dataTup = dataTup.translate( None, string.whitespace ) # strings with whitespace not allowed
    dataTup = dataTup.replace( "[", "" )
    dataTup = dataTup.replace( "]", "" )
    dataTup = dataTup.replace( '"', '' )
    dataTup = dataTup.replace( "'", "" )
    dataTup = dataTup.split( "," )

    logging.debug( "  VERIFY REL TUP : rel     = " + rel )
    logging.debug( "  VERIFY REL TUP : dataTup = " + str( dataTup ) )

    flag = False

    print self.final_results_dict

    for tup in self.final_results_dict[ rel ] :
      tup = tup.split( "," )
      print "tup = " + str( tup )
      if tup == dataTup :
        flag = True

    return flag


  #########
  #  RUN  #
  #########
  # given an arbitrary query, characterize the why (positive) provenance
  # of all result tuples.
  # assume query input in c4 datalog syntax
  def run( self ) :

    # --------------------------------- #
    # build provenance queries and schemas

    prov_queries_schemas = []
    for query in self.q.queryList :

      # build provenance rule
      prov_query = self.buildProvQuery( query )
      logging.debug( "  RUN : prov_query = " + str( prov_query ) )
  
      prov_schema = self.buildProvSchema( prov_query )
      logging.debug( "  RUN : query schema '" + str( prov_schema )+ "'" )

      prov_queries_schemas.append( [ prov_query, prov_schema ] )
 
    # --------------------------------- #
    # add provenance query and schema

    for pq in prov_queries_schemas : 

      prov_query  = pq[0]
      prov_schema = pq[1]

      self.q.setQuery( prov_query )
      logging.debug( "  RUN : set query '" + prov_query + "'" )
 
      rel    = prov_schema[0] 
      schema = prov_schema[1] 
      self.q.setSchema( rel, schema )
      logging.debug( "  RUN : set relation '" + rel + "' to schema " + str( schema ) )

    # --------------------------------- #
    # run query evaluation

    allProgramData = self.q.run()

    self.final_program       = allProgramData[0]
    self.final_table_list    = allProgramData[1]
    self.final_results_array = allProgramData[2]
    self.final_results_dict  = self.getResultsDict()

    return allProgramData


  ######################
  #  GET RESULTS DICT  #
  ######################
  def getResultsDict( self ) :

    results_dict = {}
    currRelation = None
    currDataList = []
    for i in range( 0, len( self.final_results_array ) ) :
      line = self.final_results_array[ i ]
      print "line = " + line + ",  currRelation = " + str( currRelation ) + ", currDataList = " + str( currDataList )

      # hit a break line
      if "---------------------------" in line :
        pass

      # hit a relation
      elif "---------------------------" in self.final_results_array[ i-1 ] :
        results_dict[ currRelation ] = currDataList
        currRelation                 = line
        currDataList                 = []

      # hit the last line of results
      elif i == len( self.final_results_array ) - 1 :
        currDataList.append( line )
        results_dict[ currRelation ] = currDataList
        currRelation                 = None
        currDataList                 = []

      # hit a data line
      else :
        currDataList.append( line )

    # delete the rebel None key
    del results_dict[ None ]

    #print results_dict
    return results_dict


  ######################
  #  BUILD PROV QUERY  #
  ######################
  def buildProvQuery( self, query ) :

    # ------------------------------------------ # 
    # get goal name

    goal = self.getGoalName( query )

    # ------------------------------------------ # 
    # get body

    body = self.getBody( query )

    # ------------------------------------------ # 
    # get list of goal attributes

    goalAtts = self.getGoalAtts( query )

    # ------------------------------------------ # 
    # get set of all attributes across subgoals

    tmp = query.translate( None, string.whitespace )
    bodyAttList = tmp.replace( ";", "" )
    bodyAttList = bodyAttList.split( ":-" )
    bodyAttList = bodyAttList[1]
    bodyAttList = bodyAttList.split( ")," )

    finalAttList = goalAtts
    #print "finalAttList : " + str( finalAttList )
    for atts in bodyAttList :
      atts = atts.split( "(" )
      atts = atts[1]
      atts = atts.replace( ")", "" )

      atts = atts.split( "," )

      for att in atts :
        if not att in finalAttList and not att == "_" :
          #print "att = " + att
          finalAttList.append( att )

    # ------------------------------------------ # 
    # build final rule

    finalAttList = ",".join( finalAttList )
    prov_query   = goal + "_prov" + str( self.prov_rule_counter ) + "(" + finalAttList + ") :- " + body + ";"
    self.prov_rule_counter += 1

    return prov_query
 

  #######################
  #  BUILD PROV SCHEMA  #
  #######################
  def buildProvSchema( self, prov_query ) :

    # --------------------------------- #
    # get goal name

    goalName = self.getGoalName( prov_query )

    # --------------------------------- #
    # get goal attributes

    goalAttList = self.getGoalAtts( prov_query )

    # --------------------------------- #
    # get list of subgoals and associated attributes 

    subgoals = self.getSubgoals( prov_query )
    #print subgoals

    # --------------------------------- #
    # get data types per attribute
    # only works because users are required to
    # provide the relation schemas as input.
    # accordingly, by the time the program generates
    # provenance rules, the schemas for all IDB and EDB
    # relations are declared in the Quest instance.

    dataTypeList = []
    coloredAtts  = []
    for gatt in goalAttList :

      # check the list of subgoals for the goal attribute
      for sub in subgoals :
        subName = sub[0]
        subAtts = sub[1]
        for i in range( 0, len( subAtts ) ) :
          satt = subAtts[ i ]
          if gatt == satt and not gatt in coloredAtts :
            dataType = self.q.schema[ subName ][ i ]
            dataTypeList.append( dataType )
            coloredAtts.append( gatt )

    #print dataTypeList

    return [ goalName, dataTypeList ]


  ###################
  #  GET GOAL ATTS  #
  ###################
  def getGoalAtts( self, query ) :
    tmp      = query.translate( None, string.whitespace )
    goalAtts = tmp.split( ":-" )
    goalAtts = goalAtts[0]
    goalAtts = goalAtts.split( "(" )
    goalAtts = goalAtts[1]
    goalAtts = goalAtts.replace( ")", "" )
    goalAtts = goalAtts.split( "," )
    return goalAtts


  ##################
  #  GET SUBGOALS  #
  ##################
  # return list of subgoal names mapped to the list of subgoal attributes
  # e.g. [['b', ['X', 'Z']], ['c', ['Z', 'Y']]]
  def getSubgoals( self, query ) :

    body        = self.getBody( query )
    subgoals    = []
    subgoalList = body.split( ")," )

    for sub in subgoalList :
      data       = sub.replace( ")", "" )
      data       = data.split( "(" )
      subName    = data[0]
      subAttList = data[1]
      subAttList = subAttList.split( "," )
      subgoals.append( [ subName, subAttList ] )

    return subgoals


  ###################
  #  GET GOAL NAME  #
  ###################
  def getGoalName( self, query ) :

    goalName = query.translate( None, string.whitespace )
    goalName = goalName.split( ":-" )
    goalName = goalName[0]
    goalName = goalName.split( "(" )
    goalName = goalName[0]

    return goalName


  ###############
  #  SET QUERY  #
  ###############
  # add a query to the query list for the quest instance
  def setQuery( self, queryStr ) :

    # --------------------------------- #
    # add original query
    self.q.setQuery( queryStr )
    logging.debug( "  SET QUERY : set query '" + queryStr + "'" )


  ####################
  #  GET QUERY LIST  #
  ####################
  # return the query list in the quest instance
  def getQueryList( self ) :
    return self.q.queryList


  ################
  #  SET SCHEMA  #
  ################
  # define schema for input relation in the quest instance
  def setSchema( self, relationName, typeList ) :
    self.q.schema[ relationName ] = typeList


  ################
  #  GET SCHEMA  #
  ################
  # return the schema in the quest instance
  def getSchema( self ) :
    return self.q.schema


#########
#  EOF  #
#########
