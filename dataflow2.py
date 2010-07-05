#!/usr/bin/env python

# Copyright 2010 Randall David Smith
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import pdb
import copy
import sys
import os

###		Dataflow Programming Library

### Overview

# This module implements a simple dataflow programming library,
# allowing nodes of a dataflow graph to be easily created, linked
# together, and run as a program.  The library provides some commonly
# needed dataflow operators/nodes (split, serial merge, record
# windowing, data sink to a function), base classes from which other
# nodes may be derived, and a set of primitive operators for combining
# nodes.
#
# A "DataflowNode" is a operation with some number of inputs and some
# number of outputs that does an arbitrary transformation on the inputs
# to produce the outputs.  Inputs and outputs are named by an index
# (0 <= i < node.num{Input,Output}Ports()) in the context of a particular
# node.  
#
# Combination of any number of DataflowNodes is also a DataflowNode.
# Combination of nodes always occurs in the context of a specific
# ordering of the combined nodes.  If the nodes are combined without
# linking (see "Parallel Combination" below) the input and output
# ports of the individual nodes are mapped to the input and output
# ports of the combined node by adding the number of ports of all the
# of the earlier nodes in the ordering to that port.  I.e. the name of
# an input or output port is the index into the nodes array of input
# or output ports, and the arrays of the combined node are formed by
# concatening in order the arrays of the component nodes.  Any links
# made between nodes result in removal of the ports used for those
# links from that combined array.
# 
# In combining nodes, arbitrary linking of ports may be specified.
# Two simplified forms of node combination are also supported: Serial
# Combinination and Parallel Combination.  In serial combination, the
# number of outputs of each node must equal the number of inputs to
# the next node in the sequence, and those outputs and inputs are
# connected.  In parallel combination, no linking is done; all inputs
# and outputs of all nodes remain exposed.
#
# The "&" operator corresponds to serial combination, the "|" operator
# to parallel combination.  Note that this does not follow the bourne
# shell pipe conventions; instead it attempts to parallel the
# mathematical meaning of "&" and "|" in the context of a dataflow
# graph.
# 
# The simplest class derived from SingleDataflowNode will override the
# input_() routine to accept incoming records, and will implement that
# routine to call the output_() routine with the transformation of
# those incoming records; no other code is required.  Other options
# available to derived classes are:
#	* Overridding "eos_" to receive notification from upstream
# 	  nodes that no further data will be received on a link, and
# 	  calling _signalEos() to signal the same to downstream
# 	  nodes.  The default "eos_" shuts down all processing of the
#	  operator when called.
# 	* Overriding "initialize_" to get a notification after the
# 	  graph has been created but before it starts to run, to do
# 	  any expensive initialization that may be required.
#	* Overridding "seekOutput_" to signal that they can specially
# 	  handle requests from downstream nodes to skip over some
# 	  number of records (if not overridden the infrastructure
# 	  automatically discards those records).
#	* Overriding "execute_" to receive a thread context.  This is
# 	  usually only needed for generator nodes (i.e. nodes that
# 	  have no input but produce outputs).  
#	* Overriding "batchInput_" to receive groups of record at a time.
#
# Note: Using these optional operations will increase the complexity that
# must be tracked by the class.  Specifically a) it is illegal to output any
# records on an output port after _signalEos() has been called on that port
# (or _done() has been called at all), and b) the methods listed above
# may be called indirectly as a result of a node's _output() or
# _batchOutput() call.  So if a class calls _output()/_batchOutput()
# repeatedly within a method it must ensure that later calls have not been
# made illegal by a _signalEos()/_done() call in another method.

### Included derived classes

# A series of derived classes that implement specific, commonly useful 
# dataflow constructs are included.  These may be used as example
# implementations patterns for user DataflowNode s.  They are:
# 
# SplitDFN -- Copy an input stream to multiple output streams
# FilterDFN -- Transform records 1-to-1 based on a function provided
#     as argument.
# SinkDFN -- Feed records into a function at the endpoint of a graph.
# WindowDFN -- Let pass only a specific window of records, specified
#     by record number.
# BatchDFN -- Group some number of incoming records into a single
#     outgoing record. 
# SerialMergeDFN -- Merge multiple streams by passing the entire
#     first input stream, then the second, then the third ...
# FileSourceDFN -- Output a file on a stream, with individual bytes
#     as the records.  Motivation for batchInput_/_batchOutput
#     functionality. 
# StringNewlineBatchDFN -- Group incoming byte records into line records. 
# FileWriteDFN -- Write a stream to a file.
# GenerateIntervalDFN -- Generate integer records to output 
#     according to the usual python rules for intervals.  Primarily a
#     debugging class.

### Implementation sketch

# The three key classes in this file are DataflowNode,
# SingleDataflowNode, and CompositeDataflowNode.  DataflowNode is an
# interface/abstract base class from which SingleDataflowNode and
# CompositeDataflowNode derive.  SingleDataflowNode contains the
# individual nodes within a dataflow graph.  CompositeDataflowNode
# could also have been called "DataflowGraph"; it contains some number
# of simple operators (including zero) connected into a graph.  It may
# not be executed until there are no inputs or outputs remaining on
# it.
#
# Note that CompositeDataflowNodes don't contain other
# CompositeDataflowNodes; when two CompositeDataflowNodes are merged,
# a new CompositeDataflowNode is created containing operators
# corresponding to all of the operators in the two arguments.  This
# does not modify the arguments (except in specific cases where that
# behavior is required, e.g. CompositeDataflowNode.addNode())
#
# The key code for creating combinations of DataflowNodes is in the
# following routines:
# 	* CompositeDataflowNode.__addNodeNoLinks().  This modifies the current
#	  composite node by adding another node (Single or Composite)
#	  to it without making any links between the two.
#	* CompositeDataflowNode.makeInternalLinks().  This makes links
#	  within an already existing CompositeDataflowNode between
#	  some of its output ports and some of its input ports.
#	* CompositeDataflowNode.addNode(),
#	  CompositeDataflowNode.__initFromList().  These functions
#	  translate the external view of the links requested (before
#	  __addNodeNoLinks() was called) into the view of the new
#	  CompositeDataflowNode.
#
# The key code for execution of an existing dataflow graph is in the
# following routines:
#	* CompositeDataflowNode.run().  Runs a series of checks on the
#	  graph, initializes the graph, and then drives the nodes that
#	  need an execution thread by calling their execute_()
#	  routines.
#	* SingleDataflowNode._output().  Called by each node when it
#	  needs to pass a record onto the next node in the graph.
#	* <user defined class>.input_().  This function is called
#	  whenever a new input record is to be presented to a node.
#	  It must be overridden by all DataflowNodes which have inputs
#	  (i.e. most of them).
#	  
# The library also provides support for signalling end of stream (so
# that nodes can do any cleanup or terminal work), for requesting 
# a gap in the output (to avoid the cost of a pipe processing a lot of
# records which will then be discarded), and for providing a thread
# context for nodes which generate data but do not consume it.
#
# The library is biased towards as simple a programming model as
# possible for creation of the dataflow graph, and as a result accepts
# the costs of a lot of copying during graph creation.  Specifically,
# it has the invariant that the user never has a reference to a node
# that has links to other nodes.  It may have a reference to a
# composite node that has internal links, but never to a node (single
# or composite) that has links outside of itself.  Performance is only
# a priority for graph execution.

### Naming conventions

# Class method naming conventions.
#	* Public methods: No decoration
#	* Private methods: By language spec, prefixed with __ within the
#	  class, and by _<classname>_ outside.
#	* "Protected" methods (methods intended to be called only
#	  by derived classes): Prefixed with a single underscore.
#	  __init__ is an exception to this.
#	* Base->Derived interface (interfaces the base class calls
#	  in the derived class): Suffixed with a single underscore.
# If a method is public but expected to be overridden by derived classes,
# it is named as public.

# * Types (classes) are CamelCase with initial caps.
# * Class methods are camelCase with initial lowercase.
# * Enumerated constants are camelCase with initial lowercase "e"
# * Variables (including method arguments) will be all lower case with
#   underscores separating words
#
# The following entries describe suggested variable (or method) names
# for objects of different types.  If a routine requires multiple
# variables of the same type, a distinguishing prefix should be
# added. 
# * The base classes of the module (DataflowNode, SingleDataflowNode,
#   CompositeDataflowNode) will be spelled out, but all derived classes
#   will have a DFN suffix for brevity.
# * DataflowNode: "node".  CompositeDataflowNode: "cnode" (if the
#   distinction from DataflowNode is important in that routine).
#   SingleDataflowNode: "snode".  "n", "cn", and "sn" may be used for
#   tight for loops where space is important.
# * A node index (the index of a node within an array):
#   "[cs]?node_idx", "[cs]?ni" as above.
# * A port: "port".  "iport"/"oport" can be used when the input/output
#   distinction is important and the focus of the routine is on the
#   node.  "dport/sport" can be used when the destination/source
#   distinction is important and the focus of the routine is on the
#   link between nodes.
# * Port descriptor (a two element tuple: (node_idx, port)):
#   "[iods]?port_descr". 
# * Fully qualified port (a two element tuple: (node, port)):
#   "[iods]?nodeport". 
# * Record (data flowing between nodes): "rec"
# * Link (two element tuple: (sport_descr, dport_descr)): "link" or
#   "l" in tight for loops.
# * Internal link (two element tuple: (sport, dport) used for
#   describing links inside of a composite operator in terms of the
#   port space of that operator): "intlink"
# * List of any obove objects: Suffix "s"
# * Size of a list of any above objects: num_<obj>s

### Potential future work

# One thing that's often useful when debugging dataflow graph problems
# is a log of the events as they occur.  There are two challenges with
# doing a general logging interface.  The first is that dataflow
# graphs are often used to handle high volumes of data, and that would
# produce a very large log.  The second is that timestamps would be
# useful, but (again, related to the high data volumes) often the work
# done on a per-log entry basis would be dwarfed by the cost of a
# system call.  The design this suggests is one in which adjacent
# identical log entries are collapsed into an X happened Y times type
# of entry, and a timestamp only done for each such combined entry.
# This is problematic because based on the current design, individual
# records would flow down the stream until they hit a buffer before
# the next item is pushed in.  Alterantively, timestamps and log
# entries could be done on each call to an "execute" function
# (possibly with mini-log entries rolled up automatically), and
# adjacent equal such entries combined.
#
# I think this suggests a design.  Log entries would be divided into
# fully-logged and breadcrumbed.  Breadcrumb entries are: a single
# record was transmitted along edge Y.  Fully-logged entries are:
# 	* Any type of batch output
#	* EOS signalled along edge Y
#	* Skip input signalled along edge Y (Do we breadcrumb skipped
# 	  input?  No.)
# Fully logged entries are timestamped when they start.  
# 
# Specific events roll up breadcrumbs.  Rolling up breadcrumbs means 
#	* Create a vector of edge numbers for the current set of breadcrumbs.
# 	* Compare that vector with the last fully-logged entry.  
#	* If it matches, increment the count on that entry by one.
# 	* If it doesn't match, create a new fully logged entry.
# Note that fully logged entries being logged when they start, and
# breadcrumbs rolling up into a fully logged entry, mean that when a
# breadcrumb is started, a timestamp must be generated.
#
# Breadcrumbs are rolled up when:
#	* A fully logged entry is generated
#	* The main control loop is returned into.  

# Identifiers exported by this module
__all__ = (
    # Base classes
    "DataflowNode", "SingleDataflowNode", "CompositeDataflowNode",
    # Derived classes
    "SplitDFN", "FilterDFN", "SelectDFN", "SinkDFN", "WindowDFN", "BatchDFN",
    "SerialMergeDFN", "FileSourceDFN", "StringNewlineBatchDFN",
    "FileWriteDFN", "GenerateIntervalDFN", "SortStreamDFN", 
    # Constants
    "eSerial", "eParallel",
    # Routines
    )

# Exceptions used by module
class BadInputArguments(Exception): pass
class BadMethodOverride(Exception): pass
class BadGraphConfig(Exception): pass

# Enums used in module class interfaces
eSerial = 1
eParallel = 2

# Module private routines used by class implementation
def checkLinksArg(links, nodes, method_name):
    """Confirm that the LINKS argument is valid in the context of the
    node list.  This means that it's a link list with valid
    values in the context of the node list, or it's eParallel, or it's
    eSerial and adjacent nodes have matching number of input and output
    ports."""
    if not (links == eSerial or links == eParallel
            or isinstance(links, list)):
        raise BadInputArguments, "Args LINKS (%s) to method %s isn't eSerial, eParallel or a list." % (list, method_name)
    if isinstance(links, list):
        for l in links:
            if not (0 <= l[0][0] < range(len(nodes))
                    and 0 <= l[1][0] < range(len(nodes))):
                raise BadInputArguments, "Link %s in arg LINKS to method %s contains a reference to an out of bounds node." % (l, method_name)
            if not 0 <= l[0][1] < nodes[l[0][0]].numOutputPorts():
                raise BadInputArguments, "Link %s in arg LINKS to method %s contains an out of range output port (%d)." % (l, method_name, l[0][1])
            if not 0 <= l[1][1] < nodes[l[1][0]].numInputPorts():
                raise BadInputArguments, "Link %s in arg LINKS to method %s contains an out of range input port (%d)." % (l, method_name, l[1][1])

    if links == eSerial:
        for i in range(len(nodes)-1):
            if nodes[i].numOutputPorts() != nodes[i+1].numInputPorts():
                raise BadInputArguments, "Method %s called with eSerial and non-matching numbers of output (%d) and input (%d) ports on nodes %s, %s." % (method_name, nodes[i].numOutputPorts(), nodes[i+1].numInputPorts(), str(nodes[i]), str(nodes[i+1]))


class DataflowNode(object):
    """Interface class to define type.  
In C++ this would be an abstract base class, in Java an interface."""
    # Public interface
    def numInputPorts(self):
        """Return the number of input ports that this node has
        available.  Defines the range of allowed input port indices that can
        be used in the context of this operator."""
        raise NotImplementedError, "Method numInputPorts not overridden in inherited class."
    def numOutputPorts(self):
        """Return the number of output ports that this node has
        available.  Defines the range of allowed output port indices that can
        be used in the context of this operator."""
        raise NotImplementedError, "Method numOutputPorts not overridden in inherited class."

    def __and__(self, node):
        """Connect two dataflow nodes in series, with the outputs of the
        first linked to the inputs of the second.
        As appropriate to the "&"& operator, this is a copy operator; it will
        not modify its arguments."""
        if not isinstance(node, DataflowNode):
            raise BadInputArguments, "Argument to DataflowNode & operator (%s) is not a DataflowNode." % node
        return CompositeDataflowNode((self, node), eSerial)

    def __or__(self, node):
        """Connect two dataflow nodes in parallel, with all the inputs and
        outputs of both exposed in the combined operator.  
        As appropriate to the "|" operator, this is a copy operator; it will
        not modify its arguments.
        Note that this operator is *not* parallel to the shell pipe operation;
        use "&" for that.  This is because if DataflowNodes are filters,
        this corresponds to a logical or operation, and piping corresponds to a
        logical and operation."""
        if not isinstance(node, DataflowNode):
            raise BadInputArguments, "Argument to DataflowNode & operator (%s) is not a DataflowNode." % node
        return CompositeDataflowNode((self, node), eParallel)

class SingleDataflowNode(DataflowNode):
    """Base class for simple nodes/operators; should be inherited from
    to define specific nodes.  See "Stubs of functions that derived
    classes may choose to implement" below, as well as a selection of
    simple DataflowNodes implemented at the end of this file."""
    ### Public methods
    def numInputPorts(self):
        """Return the number of input ports that this node has
        available.  Defines the range of allowed input port indices that can
        be used in the context of this operator."""
        return self.__num_input_ports
    def numOutputPorts(self):
        """Return the number of output ports that this node has
        available.  Defines the range of allowed output port indices that can
        be used in the context of this operator."""
        return self.__num_output_ports

    def copy(self):
        """Create a copy of this node."""
        copy_node = copy.copy(self)
        copy_node.__initConnections() # Nuke any links; they're incorrect now.
        return copy_node

    def __str__(self):
        """Produce a printable representation of the object, not
        necessarily one that eval() can accept."""
        res = "<" + self.__class__.__name__ + " (%d, %d) " % (self.numInputPorts(),
                                                              self.numOutputPorts())
        for att in self.__dict__:
            # Ignore base class attributes
            if att in ("_SingleDataflowNode__num_input_ports",
                       "_SingleDataflowNode__num_output_ports",
                       "_SingleDataflowNode__input_nodes",
                       "_SingleDataflowNode__output_nodes",
                       "_SingleDataflowNode__output_node_iports",
                       "_SingleDataflowNode__ignoring_output_records"):
                continue
            res += ", " + att + ": " + str(self.__dict__[att])
        res += ">"
        return res

    ### "Protected" interface (for use of derived classes)
    def __init__(self, num_input_ports=1, num_output_ports=1):
        """Initialize the base class, specifying the number of input
        and output ports."""
        if num_input_ports < 0 or num_output_ports < 0:
            raise BadInputArguments, "Arguments to SingleDataflowNode constructor (%d,%d) includes negative number." % (num_input_ports, num_output_ports)
        self.__active = True
        self.__num_input_ports = num_input_ports
        self.__num_output_ports = num_output_ports

        # Setup the basic connection tracking
        self.__initConnections()
        
    def _signalEos(self, output_port=0):
        """Signal that no more records will be transmitted on this port."""
        # print "SingleDataflowNode._signalEos() called for object ", self
        if not 0 <= output_port < self.numOutputPorts():
            raise BadInputArguments, "SingleDataflowNode._signalEos: output_port (%d) out of range [0, %d]" % (output_port, numOutputPorts())
        dnode = self.__output_nodes[output_port]
        if self.__eos_pending[output_port] or dnode is None:
            return              # We've previously been called; don't repeat

        if self.__in_transition_records[output_port] != 0:
            self.__eos_pending[output_port] = True
            return              # Will propagate after batch output

        dest_self_iport = dnode.__input_nodes.index(self)
        self.__output_nodes[output_port] = None
        dnode.eos_(dest_self_iport)

    def _ignoreInput(self, num_recs=-1, input_port=0):
        """Request that the given number of records be skipped on this
        port.  NUM_RECS == -1 indicates that all records may be skipped.
        This call stacks additively with previous calls to _ignoreInput."""
        # print "SingleDataflowNode._ignoreInput() called for object ", self
        if not isinstance(num_recs, int) or num_recs < -1:
            raise BadInputArguments, "SingleDataflowNode._ignoreInput: Invalid num_recs value %s" % num_recs
        if not 0 <= input_port < self.numInputPorts():
            raise BadInputArguments, "SingleDataflowNode._ignoreInput: Invalid input_port value %d" % input_port
        src_node = self.__input_nodes[input_port]

        if self not in src_node.__output_nodes:
            # _signalEos has been called; we'll never see input from
            # this node again.
            return
        src_self_oport = src_node.__output_nodes.index(self)

        assert len(src_node.__ignoring_output_records) > src_self_oport

        # Deal with records in transition, which need to be dropped
        # by infrastructure
        if src_node.__in_transition_records[src_self_oport] != 0:
            infra_num_recs_to_drop = min(src_node.__in_transition_records[src_self_oport],
                                         num_recs)
            if infra_num_recs_to_drop != -1:
                infra_num_recs_to_drop += src_node.__ignoring_output_records[src_self_oport]
            src_node.__ignoring_output_records[src_self_oport] = infra_num_recs_to_drop
            num_recs -= infra_num_recs_to_drop
        
        # Deal with other records, which may or may not need to be dropped by
        # infrastructure
        rval = src_node.seekOutput_(num_recs, src_self_oport)
        if rval is None:
            raise BadMethodOverride("%s.seekOutput_ function did not return a value"
                                     % type(src_node))
        if not rval:
            if num_recs != -1:
                num_recs += src_node.__ignoring_output_records[src_self_oport]
            src_node.__ignoring_output_records[src_self_oport] = num_recs
        
    def _done(self):
        """Signal that this node has completed all its processing."""
        # print "SingleDataflowNode._done() called for object ", self
        self.__active = False
        for i in range(self.__num_input_ports):
            self._ignoreInput(input_port=i)
        for i in range(self.__num_output_ports):
            self._signalEos(i)

    def _output(self, output_port, rec):
        """Output a record on the specified port for the next node."""
        # Not doing interface checking for performance; this function
        # is called repeatedly
        if self.__ignoring_output_records[output_port] != 0:
            if self.__ignoring_output_records[output_port] != -1:
                self.__ignoring_output_records[output_port] -= 1
        else:
            self.__output_nodes[output_port].input_(self.__output_node_iports[output_port], rec)

    def _batchOutput(self, output_port, recs):
        """Output a whole bunch of records at once."""
        # Handle records currently being ignored by infrastructure
        if self.__ignoring_output_records[output_port] != 0:
            if self.__ignoring_output_records[output_port] == -1:
                return          # Ignore all of them
            if self.__ignoring_output_records[output_port] >= len(recs):
                self.__ignoring_output_records[output_port] =- len(recs)
                return
            else:
                recs = recs[self.__ignoring_output_records[output_port]:]
                self.__ignoring_output_records[output_port] = 0

        # Try to get the downstream node to take the rest in a bunch
        res = self.__output_nodes[output_port].batchInput_(
            self.__output_node_iports[output_port], recs
            )
        if res is None:
            raise BadMethodOverride("%s method batchInput_ did not return a value" % type(self.__output_nodes[output_port]))

        # If it doesn't, pass them down by hand, doing appropriate accounting
        # to handle indirectly calling _ignoreInput() or _signalEos() 
        if not res:
            self.__in_transition_records[output_port] = len(recs)
            for r in recs:
                # This should be safe, as eos should be held off while
                # we finish outputting, and _output() is responsible for
                # dropping records.
                self._output(output_port, r)
                self.__in_transition_records[output_port] -= 1
            assert self.__in_transition_records[output_port] == 0
            if self.__eos_pending[output_port]:
                self.__eos_pending[output_port] = False
                self._signalEos(output_port)


    def _nodeActive(self):
        """True if _done() has never been called, False if it has.
        Convenience function for derived classes."""
        return self.__active

    ### Stubs of functions that derived classes may choose to implement
    def input_(self, input_port, rec):
        """Override to accept input from upstream nodes."""
        raise NotImplementedError, "SingleDataflowNode.input_ method not implemented in derived class."

    def batchInput_(self, input_port, recs):
        """Override to accept batches of records all at once from upstream
        nodes.  This is a performance optimization when you're in a pipe that
        you expect to have large numbers of small records, and the upstream
        operator may have many of them to send at once (i.e. where the record
        boundaries have some meaning that isn't related to a natural processing
        unit.
        RECS will be a sequence of individual records.  The result of calling
        batchInput_ should be the same as the loop
        	for r in recs: n.input_(self, input_port, r)
        which is precisely what the infrastructure will do if batchInput_
        is not defined.  Return True from this function if it successfully
        hands the batch."""
        return False

    def eos_(self, input_port):
        """Override if notification of end of stream (no further input
        will be provided) is wanted; this function will be called when the
        node linked to the indicated port signals EOS.

        The default version of this function closes down the operator
        when it is called, so it should be overridden when that is
        inappropriate behavior.  Two specific contexts in which this is
        wise are if the operator has more than one input, or if it saves any
        state between input records that need to be flushed on EOS."""
        self._done()

    def seekOutput_(self, num_recs, output_port):
        """Override if a request from a downstream node to seek
        forward NUM_RECS in the stream (see _ignoreInput()) can be handled
        in some efficient fashion by the node (-1 means skip all records).
        If this function returns False, the infrastructure will manually
        skip the records; if it returns True, the responsibility for
        skipping them has been accepted by the derived class.  Note that
        if this function is called multiple times the arguments
        should be added.

        For pure transformation nodes (one record out for each
        record in, no maintained state) it is valuable to override this method
        to pass the notification up the stream; if a record is going to
        be dropped, it should be dropped as far upstream as possible."""
        return False

    def execute_(self, num_recs):
        """Override if the node requires threading support during
        execution.  This is usually only for pure output nodes
        (e.g. read a line from a file and output it as a record); most
        other nodes are driven by output of records coming
        from upstream nodes. 

        NUM_RECS indicates the number of records you should generate (or
        the number of units of some sort of equivalent processing you
        should do) before returning.  If NUM_RECS is -1, an arbitrary
        amount of processing may be done.  execute_ should check before
        doing any output to confirm that the operator has not been
        shutdown by some other method; specifically if
        _done() or _signalEos() has been called by any method operator, it
        is no longer legal to output on that output port.

        execute_ should return False if it does does not need to be
        called again to drive processing and True if there is more
        processing for the node to do.  It is the responsibility of
        the execute_ method to detect this state; specifically,
        calling _done() does not prevent execute_() from later
        being called on the node."""
        return False

    def initialize_(self):
        """Override if the derived node should perform some expensive
        initialization before processing.  Many copies of classes will be
        constructed and destructed during graph creation, so (e.g.) opening
        of files should occur in this routine."""
        pass
        
    ### "Private" interface, for use of class methods and friends
    ### (CompositeDataflowNode, specifically)
    def __initConnections(self):
        """Initialize the connections data structure to have no links."""
        # Used for both init and copy

        # Will be filled in with the nodes in question
        self.__input_nodes = [None,] * self.__num_input_ports
        self.__output_nodes = [None,] * self.__num_output_ports
        # Input port # on peer corresponding to our output port.
        self.__output_node_iports = [None,] * self.__num_output_ports

        # Number of records that need to be skipped on a particular
 	# output port on output.  May be -1 (skip all).  Note that this
 	# value may be non-zero even if the class overrides seekOutput_;
 	# if a class method has called _batchOutput() and there are
 	# records "in transition" when an _ignoreInput() call is done
	# (i.e. the infrastructure is calling _output() by hand for them)
 	# those records must be skipped by the infrastructure.
        self.__ignoring_output_records = [0,] * self.__num_output_ports

        # Number of records in transition in the infrastructure (have
        # been passed in by _batchOutput() but not yet passed on to the
        # downstream node.)
        self.__in_transition_records = [0,] * self.__num_output_ports

        # Note that an eos has been signalled when a batch output is
        # in progress; this will need to be processed when it's done.
        self.__eos_pending = [False,] * self.__num_output_ports

    @staticmethod
    def __link(snodeport, dnodeport):
        """Make a link between the actual nodes passed (side effects
        both arguments).  Both SNODEPORT and DNODEPORT are tuples of the form
        (node, port)."""
        (snode, sport) = snodeport
        (dnode, dport) = dnodeport
        assert isinstance(snode, SingleDataflowNode)
        assert isinstance(dnode, SingleDataflowNode)
            
        snode.__output_nodes[sport] = dnode
        snode.__output_node_iports[sport] = dport
        dnode.__input_nodes[dport] = snode
        

class CompositeDataflowNode(DataflowNode):
    """Class for containing a connected set of SingleDataflowNode.  This is
    a final class; it is not intended to be inherited from."""
    ### Public interface

    # Interfaces for structure creation

    # Constructor is considered public; may be called via:
    # CompositeDataflowNode() -- Null container
    # CompositeDataflowNode(node) -- Wrapper around single node
    # CompositeDataflowNode(nodes, links) -- Links two or more nodes
    def __init__(self, *args):
        # Simple nodes contained by this instance
        self.__contained_nodes = []

	# These arrays map from the composite node port# to
        # a port descriptor for an internal node
        self.__input_port_descrs = []  
        self.__output_port_descrs = [] 

        if len(args) == 0:
            return # Composite node with no components
        elif len(args) == 1:
            self.__initFromSingleton(args[0])
        else:
            self.__initFromList(*args)

    def addNode(self, node, links=eSerial):
        """Add a new node to an existing CompositeDataflowNode.  The
        new node may be Single or Composite.  LINKS is a list of links
        ((sport_descr, dport_descr) tuples) in which all node indices are
        0 (referring to self) or 1 (referring to node)."""

        # Validate arguments
        if not isinstance(node, DataflowNode):
            raise BadInputArguments, "Arg NODE (%s) to method CompositeDataflowNode.addNode isn't a DataflowNode" % node

        checkLinksArg(links, (self,node), "CompositeDataflowNode.addNode")

        # Translate symbolic links argument to list
        if links == eParallel:
            links = []
        elif links == eSerial:
            links = [((0,i),(1,i)) for i in range(node.numInputPorts())]

        # Save the important data about ourselves before consuming the new
        # node, then eat it.  This will produce a valid composite node without
        # any of the links having been executed.
        oport_offset = (0, len(self.__output_port_descrs))
        iport_offset = (0, len(self.__input_port_descrs))
        self.__addNodeNoLinks(node)

        # Execute the links
        intlinks = [(l[0][1] + oport_offset[l[0][0]],
                     l[1][1] + iport_offset[l[1][0]])
                    for l in links]
        self.makeInternalLinks(intlinks)

    def makeInternalLinks(self, intlinks):
        """Link output ports to input ports within a single
        (composite) operator."""
        for intlink in intlinks:
            if not 0 <= intlink[0] < self.numOutputPorts():
                raise BadInputArguments, "CompositeDataflowNode.makeInternalLinks: Specified output port (%d) out of range for this node" % intlink[0]
            if not 0 <= intlink[1] < self.numInputPorts():
                raise BadInputArguments, "CompositeDataflowNode.makeInternalLinks: Specified input port (%d) out of range for this node" % intlink[1]

        # If there aren't any links, return; simplifies processing below
        if len(intlinks) == 0:
            return

        # Separate out the output and input ports
        output_ports, input_ports = zip(*intlinks)

        # Get the descriptors without removing them since that would change
        # the mapping for future descriptors
        oport_descrs = [self.__output_port_descrs[oport]
                        for oport in output_ports]
        iport_descrs = [self.__input_port_descrs[iport]
                        for iport in input_ports]

        # Remove those ports from the list; they're about to be used up
        self.__output_port_descrs = [self.__output_port_descrs[i]
                                     for i in range(self.numOutputPorts())
                                     if i not in output_ports]
        self.__input_port_descrs = [self.__input_port_descrs[i]
                                     for i in range(self.numInputPorts())
                                     if i not in input_ports]

        # Make all the links
        for oport_descr, iport_descr in zip(oport_descrs, iport_descrs):
            SingleDataflowNode._SingleDataflowNode__link(
                (self.__contained_nodes[oport_descr[0]], oport_descr[1]),
                (self.__contained_nodes[iport_descr[0]], iport_descr[1])
                )
        
    def copy(self):
        copy_node = CompositeDataflowNode()
        # Safe to make shallow copy as entries are tuples, which are immutable
        copy_node.__output_port_descrs = self.__output_port_descrs[:]
        copy_node.__input_port_descrs = self.__input_port_descrs[:]

        # New copy of list
        copy_node.__contained_nodes = [o.copy() for o in self.__contained_nodes[:]]

        # Re-create internal links
        for l in self.internalLinks():
            source, dest = l
            src_node_idx, src_port = source
            dest_node_idx, dest_port = dest
            SingleDataflowNode._SingleDataflowNode__link(
                (copy_node.__contained_nodes[src_node_idx], src_port),
                (copy_node.__contained_nodes[dest_node_idx], dest_port)
                )

        return copy_node

    # Interfaces for running the graph
    def run(self):
        """Run the dataflow graph contained in this object."""
        ### Check:
        ###	* Graph self-contained
        ###	* No cycles
        ###	* Not disjoint
        ### Call all initialize routines
        ### Drive graph by calling execute routines of nodes that need it.

        if self.numInputPorts() != 0:
            raise BadInputArguments, "Graph %s has non-zero inputs" % self
        if self.numOutputPorts() != 0:
            raise BadInputArguments, "Graph %s has non-zero output" % self

        self.__checkAcyclic()

        # Arguably disjoint graphs should be ok; I could imagine cases
        # in which you'd want a composite node that did two things
        # in parallel.  Going for more constrained rather than less
        # constrained for the moment.  
        self.__checkConnected()

        # Initialize the graph
        for n in self.__contained_nodes:
            n.initialize_()

        # Call all execute_() routines until they've all returned
        # False.  Stop calling an node's routine when it returns
        # False.  If there's only one node, just hand control to it.
        nodes = self.__contained_nodes[:]
        while nodes:
            nodes1 = nodes[:]
            if len(nodes1) > 1:
                num_recs = 1
            else:
                num_recs = -1
            for d in nodes1:
                res = d.execute_(num_recs)
                if res is None:
                    raise BadMethodOverride("%s execute_ method did not return a value" % type(d))
                if not res: nodes.remove(d)

    # Interfaces for probing structure
    def numInputPorts(self):
        """Return the number of input ports that this node has
        available.  Defines the range of allowed input port indices that can
        be used in the context of this operator."""
        return len(self.__input_port_descrs)

    def numOutputPorts(self):
        """Return the number of output ports that this node has
        available.  Defines the range of allowed output port indices that can
        be used in the context of this operator."""
        return len(self.__output_port_descrs)

    def internalNodes(self):
        """Returns a list of the internal nodes used for this
        composite (copied to remove links)."""
        return [node.copy() for node in self.__contained_nodes]

    def internalLinks(self):
        """Returns the links between the simple nodes that form
        this composite node.  Links are of the form
        ((source_op_idx, source_port), (dest_op_idx, dest_port)).
        The op_idx are indices into the list returned by internalNodes()."""
        links = []
        for (src_node_idx, src_node) in enumerate(self.__contained_nodes):
            for src_port in range(src_node.numOutputPorts()):
                dest_node = src_node._SingleDataflowNode__output_nodes[src_port]
                if dest_node is not None:
                    dest_node_idx = self.__contained_nodes.index(dest_node)
                    dest_port = dest_node._SingleDataflowNode__input_nodes.index(src_node)
                    links.append(((src_node_idx, src_port), (dest_node_idx, dest_port)))
        return links

    def inputPortDescrs(self):
        """Returns the mapping between input ports of the composite
        node and the input ports of the single nodes within it.
        The array returned is indexed by composite input p
ort
        descriptor and contains a list of tuples of the form
        (dest_node_idx, dest_node_port)."""
        return self.__input_port_descrs[:]

    def outputPortDescrs(self): 
        """Returns the mapping between output ports of the composite
        node and the output ports of the single nodes within it.
        The array returned is indexed by composite output port
        descriptor and contains a list of tuples of the form
        (src_node_idx, src_node_port)."""
        return self.__output_port_descrs[:]

    # Operator overloading
    def __iand__(self, node):
        """Link the argument node into this one, attaching all outputs of
        this node to all inputs of the argument node.
        A copy is made of the argument node, but this node is modified."""

        # Argument validation done by callee
        self.addNode(node, eSerial)

    def __ior__(self, node):
        """Link the argument node into this one, exposing all inputs
        and outputs of both nodes in the resulting node.
        A copy is made of the argument node, but this node is modified."""

        # Argument validation done by callee
        self.addNode(node, eParallel)

    def __str__(self):
        """Produce a printable representation of the object, not
        necessarily one that eval() can accept.  Much more complicated
        than users will want--for debugging."""
        res = "<CompositeDataflowNode (%d, %d): " % (self.numInputPorts(),
                                                   self.numOutputPorts())
        res += "["
        for i,node in enumerate(self.__contained_nodes):
            if i != 0: res += ", "
            res += str(node)
        res += "] " + str(self.internalLinks()) + ">"
        return res

    # Protected (null; this is a final class not intended for inheritance).

    # Private
    def __initFromSingleton(self, node):
        """Make self a copy of node."""

        if not isinstance(node, DataflowNode):
            raise BadInputArguments("CompositeDataflowNode(node) called with non-DataflowNode argument")

        node = node.copy()
        if isinstance(node, CompositeDataflowNode):
            self.__contained_nodes = node.__contained_nodes
            self.__input_port_descrs = node.__input_port_descrs
            self.__output_port_descrs = node.__output_port_descrs
        else:
            self.__contained_nodes = [node.copy()]
            self.__input_port_descrs = [(0, i) for i in range(node.numInputPorts())]
            self.__output_port_descrs = [(0, i) for i in range(node.numOutputPorts())]

    def __initFromList(self, nodes, links=eSerial):
        """Create a composite DFN from the passed in nodes and
        inter-node links specified.  NODES should be a list of
        DataFlowNodes (either single or composite).  LINKS may be
        eSerial, eParallel, or a list of the form ((sourcenodeindex,
        sourceport), (destnodeindex, dest_port)).  If eSerial, each
        pair of adjacent nodes in the node list must have matching
        inputs and outputs, which will be connected.  If eParallel, no
        connections are done--all input links for all nodes will be
        presented by the composite node (in the order passed) and the
        same will be true for the output links.  """

        for n in nodes:
            if not isinstance(n, DataflowNode):
                raise BadInputArguments("CompositeDataflowNode(nodes, links) called with non-node element in first argument.")
        checkLinksArg(links, nodes, "CompositeDataflowNode constructor")

        # Create a real link list from symbolic args
        if links==eParallel:
            links = []          # No extra links to form
        if links==eSerial:
            links = []
            for i in range(len(nodes)-1):
                links += [((i, j), (i+1,j))
                          for j in range(nodes[i].numOutputPorts())]

        # Turn everything composite
        nodes = [CompositeDataflowNode(n) for n in nodes]

        # Record the offsets needed
        port_descr_iport_offsets = reduce(lambda x, y: x + [x[-1]+y,],
                                          [n.numInputPorts()
                                           for n in nodes],
                                          [0])
        port_descr_oport_offsets = reduce(lambda x, y: x + [x[-1]+y,],
                                          [n.numOutputPorts()
                                           for n in nodes],
                                          [0])

        # Create a single composite operator containing all of the
        # passed arguments, but without any links executed.
        # This presumes we're currently empty
        for n in nodes:
            self.__addNodeNoLinks(n)

        # Translate all the links into the port space of the new
        # composite operator & execute them.
        intlinks = [(l[0][1] + port_descr_oport_offsets[l[0][0]],
                     l[1][1] + port_descr_iport_offsets[l[1][0]])
                    for l in links]
        self.makeInternalLinks(intlinks)

    def __addNodeNoLinks(self, node):
        """Add NODE to self, making no links between them.  Input and
        output ports of the result will be a concatenation of the
        input and output ports of self and node (in that order)."""
        assert isinstance(node, DataflowNode)

        # Copy and make composite for simplicity, then add it in
        node = CompositeDataflowNode(node) # Copies and normalizes

        node_idx_offset = len(self.__contained_nodes)
        self.__contained_nodes += node.__contained_nodes
        self.__input_port_descrs += [(port_descr[0] + node_idx_offset,
                                      port_descr[1])
                                     for port_descr in node.__input_port_descrs]
        self.__output_port_descrs += [(port_descr[0] + node_idx_offset,
                                      port_descr[1])
                                     for port_descr in node.__output_port_descrs]

    def __checkAcyclic(self):
        """Confirm self graph has no cycles."""
        # Get a list of links (skipping ports; unneeded for this algorithm)
        links = [(s[0], d[0]) for (s, d) in self.internalLinks()]
        last_num_links = 0
        
        # Repeatedly prune link list of references to nodes that have no
        # inputs
        while len(links) != 0 and last_num_links != len(links):
            last_num_links = len(links)
            dest_nodes = set(zip(*links)[1])
            # We don't want the link if the source has no inputs
            links = [l for l in links if l[0] in dest_nodes]

        if len(links) != 0:
            # We have a loser!
            # The right way to do this error message would be to output
            # all the cycles detected.  But that would take a fair amount
            # of work for a rare event, and one in which the user can probably
            # figure out exactly what's going on just from the list of nodes.
            # So I'll put off the more complicate error message until I need
            # it.
            nodes = zip(*links)
            nodes = nodes[0] + nodes[1] # Flatten
            nodes = dict.fromkeys(nodes).keys() # Uniquify
            msg = "Cycle detected among DataflowNodes: ("
            msg += ", ".join([str(self.__contained_nodes[n]) for n in nodes])
            msg += ")"
            raise BadGraphConfig, msg

    def __checkConnected(self):
        """Confirm self graph is not disjoint."""
        # Idea is to start from a random link, and explore in
        # an undirected fashion from that link, marking nodes as
        # visited.  If there are unvisited nodes when we're done, we're
        # disjoint
        nodes = set(range(len(self.__contained_nodes)))
        if len(nodes) == 1: return

        # Get a list of links (skipping ports; unneeded for this algorithm)
        links = [(s[0], d[0]) for (s, d) in self.internalLinks()]

        visited_nodes = set(links.pop()) # Starts with two nodes
        old_num_visited_nodes = -1
        while old_num_visited_nodes != len(visited_nodes):
            old_num_visited_nodes = len(visited_nodes)
            for l in links:
                if l[0] in visited_nodes or l[1] in visited_nodes:
                    links.remove(l)
                    visited_nodes.add(l[0])
                    visited_nodes.add(l[1])
                    
        if links:
            # Graph is Disjoint
            unvisited_nodes = nodes - visited_nodes

            msg = "Disjoint sets of nodes found.  \n"
            msg += ("First set: (" +
                    ", ".join([str(self.__contained_nodes[o])
                               for o in visited_nodes])
                    + ")\n")
            msg += ("First set: (" +
                    ", ".join([str(self.__contained_nodes[o])
                               for o in unvisited_nodes])
                    + ")\n")
            raise BadGraphConfig, msg

### Derived classes

class SplitDFN(SingleDataflowNode):
    """DFN to split a single input stream into multiple output
    streams."""
    def __init__(self, num_outputs):
        if num_outputs < 0:
            raise BadInputArguments, ("SplitDFN constructor: num_outputs invalid (%d)"
                                      % num_outputs)
        SingleDataflowNode.__init__(self, num_output_ports=num_outputs)
        self.__num_outputs = num_outputs
        self.__skip_records = [0,] * num_outputs
        self.__broken_pipes = 0

    def input_(self, input_port, rec):
        for i in range(self.__num_outputs):
            if self.__skip_records[i]:
                if self.__skip_records[i] > 0:
                    self.__skip_records[i] -= 1
            else:
                self._output(i, rec)

    def batchInput_(self, input_port, recs):
        for i in range(self.__num_outputs):
            # -1 case
            if self.__skip_records[i] < 0:
                continue

            # More records to skip than we have case
            if self.__skip_records[i] >= len(recs):
                self.__skip_records[i] -= len(recs)
                continue

            # Fewer records to skip (possibly zero) than we have case
            orecs = recs[self.__skip_records[i]:]
            self.__skip_records[i] = 0
            self._batchOutput(i, orecs)
        return True

    def eos_(self, input_port):
        assert input_port == 0
        self._done()            # Will result in downstream eoses.

    def seekOutput_(self, num_recs, output_port):
        # print "SplitDFN.seekOutput_(%d, %d) called" % (num_recs, output_port)
        if num_recs != -1:
            num_recs += self.__skip_records[output_port]
        self.__skip_records[output_port] = num_recs
        if num_recs == -1:
            self.__broken_pipes += 1
            self._signalEos(output_port)
        if self.__broken_pipes == len(self.__skip_records):
            # Nothing more to do here
            self._done()
            return True         # Don't want to deal with minval processing
        minval = min([s for s in self.__skip_records if s >= 0])
        if minval > 0:
            self._ignoreInput(minval)
            for i in range(len(self.__skip_records)):
                if self.__skip_records[i] > 0:
                    self.__skip_records[i] -= minval
        return True

    # No need to override execute_ or initialize_
                                    
class SelectDFN(SingleDataflowNode):
    """Pass records through, or not, depending on the result of the
    input function applied ot the record.  True should be returned
    to pass the record."""
    def __init__(self, select_func):
        SingleDataflowNode.__init__(self)
        self.__select_func = select_func

    def input_(self, input_port, rec):
        if self.__select_func(rec):
            self._output(0, rec)

class FilterDFN(SingleDataflowNode):
    """DFN to transform each record through a passed function."""
    def __init__(self, filter_func):
        SingleDataflowNode.__init__(self)
        self.__filter_func = filter_func

    def input_(self, input_port, rec):
        self._output(0, self.__filter_func(rec))

    def eos_(self, input_port):
        assert input_port == 0
        self._done()

    def seekOutput_(self, num_recs, output_port):
        assert output_port == 0
        if num_recs == -1:
            self._done()
        else:
            self._ignoreInput(num_recs)
        return True

class SinkDFN(SingleDataflowNode):
    """DFN to pass each record to a function."""
    def __init__(self, sink_func):
        SingleDataflowNode.__init__(self, num_output_ports=0)
        self.__sink_func = sink_func

    def input_(self, input_port, rec):
        self.__sink_func(rec)

class WindowDFN(SingleDataflowNode):
    """DFN to extract a specific window of records.  The constructor takes
    two arguments, which are the first to pass on and the first record
    to ignore (i.e. it's a half open interval).  These arguments can be
    specified via named arguments WINDOW_START and WINDOW_END.  If WINDOW_END 
    is -1, that indicates no end to the window.

    Note that records are zero indexed, so if this is used on (for
    instance) the lines of a file, it will produce results off by one
    from head/tail."""
    def __init__(self, window_start=0, window_end=-1):
        if window_start < 0 or window_end < -1:
            raise BadInputArguments, "WindowDFN constructor: Argument invalid (window_start = %d, window_end = %d)" % (window_start, window_end)
        SingleDataflowNode.__init__(self)
        self.__interval = (window_start, window_end)
        self.__next_record = 0

    def initialize_(self):
        self.__checkSeek()

    def input_(self, input_port, rec):
        self._output(0, rec)
        self.__next_record += 1
        self.__checkSeek()

    def eos_(self, input_port):
        assert input_port == 0
        self._done()

    def seekOutput_(self, num_recs, output_port):
        if num_recs == -1 or self.__next_record + num_recs >= self.__interval[1]:
            self._done()
        else:
            self._ignoreInput(num_recs)
            self.__next_record += num_recs
        return True

    def __checkSeek(self):
        if self.__interval[0] > self.__next_record:
            self._ignoreInput(self.__interval[0] - self.__next_record)
            self.__next_record = self.__interval[0]
        if self.__interval[1] <= self.__next_record:
            self._done()


class BatchDFN(SingleDataflowNode):
    """Batch up some number of incoming records (which may be all of
    them if a -1 is provided as argument) and combine them into a
    single record sent downstream.  Note that this record will be a
    list of all incoming records; no python type-level manipulation is
    attempted."""
    def __init__(self, batch_size=-1):
        SingleDataflowNode.__init__(self)
        self.__batch_size = batch_size
        self.__buffer = []

    def input_(self, input_port, rec):
        if len(self.__buffer) == self.__batch_size:
            self._output(0, self.__buffer)
            self.__buffer = []
        self.__buffer.append(rec)

    def eos_(self, input_port):
        self._output(0, self.__buffer)
        self._done()

    def seekOutput_(self, num_recs, output_port):
        # If I'm buffering infinitely and someone's asked me to
        # skip any records, I'm done
        if num_recs == -1:
            self._done()
        else:
            input_recs_to_skip = num_recs * self.__batch_size
            self._ignoreInput(input_recs_to_skip - len(self.__buffer))
            self.__buffer = []
        return True

class SerialMergeDFN(SingleDataflowNode):
    """Merge incoming streams serially; i.e. everything on stream 0
    will be sent before anything on stream one is sent, and etc."""
    def __init__(self, num_inputs):
        SingleDataflowNode.__init__(self, num_input_ports=num_inputs)
        self.__num_inputs = num_inputs
        self.__buffers = [[] for i in range(num_inputs)]
        self.__eos_seen = [False,] * num_inputs
        self.__next_stream_to_output = 0
        
    def input_(self, input_port, rec):
        self.__buffers[input_port].append(rec)

    def eos_(self, input_port):
        assert not self.__eos_seen[input_port]
        self.__eos_seen[input_port] = True
        while (self.__next_stream_to_output < self.__num_inputs
               and self.__eos_seen[self.__next_stream_to_output]):
            for rec in self.__buffers[self.__next_stream_to_output]:
                self._output(0, rec)
            self.__buffers[self.__next_stream_to_output] = []
            self.__next_stream_to_output += 1
        if self.__next_stream_to_output >= self.__num_inputs:
            self._done()

    def seekOutput_(self, num_recs, output_port):
        # Can only usefully handle this in the shutdown case, as we don't
        # know where the boundaries between records are.
        if num_recs == -1:
            self._done()
            return True
        return False

class FileSourceDFN(SingleDataflowNode):
    """Dump all the lines from a source file out the output.  NOTE that
    this is done with individual records as bytes (but using batch output,
    so it doesn't have to be a performance travesty downstream if
    batchInput_ is overridden).  This allows intelligence responses
    to ignoreInput() (i.e. seeking within the file.)."""
    def __init__(self, file, buffer_size=4096):
        SingleDataflowNode.__init__(self, num_input_ports=0)
        self.__file = file
        self.__buffer_size = buffer_size

    def execute_(self, num_recs):
        while num_recs != 0 and self._nodeActive():
            file_block = self.__file.read(self.__buffer_size)
            if not file_block:
                # We're at EOF, therefore done.
                self._done()
                return False
            self._batchOutput(0, file_block)
            if num_recs != -1:
                num_recs -= 1
        if self._nodeActive():
            return True
        else:
            return False

    def seekOutput_(self, num_recs, output_port):
        # Can only usefully handle this in the shutdown case, as we don't
        # know where the boundaries between records are in the file
        # print "FileSourceDFN.seekOutput_(%d, %d) called at file position %d" % (num_recs, output_port, self.__file.tell())
        if num_recs == -1:
            self._done()
        else:
            self.__file.seek(num_recs, os.SEEK_CUR)
        return True

class StringNewlineBatchDFN(SingleDataflowNode):
    """Take an incoming stream of characters and turn it into records
    at newline boundaries.  The most obvious use is with FileSourceDFN."""
    def __init__(self):
        SingleDataflowNode.__init__(self)
        self.__partial_line = ""

    def input_(self, input_port, rec):
        self.__partial_line += rec
        if rec == "\n":
            self._output(0, self.__partial_line)
            self.__partial_line = ""

    def batchInput_(self, input_port, recs):
        self.__partial_line += recs # Char and sequence of char are both strings
        orecs = self.__partial_line.splitlines(True)
        if orecs[-1][-1] != "\n":
            # Last entry isn't a full line
            self.__partial_line = orecs[-1]
            orecs = orecs[:-1]
        else:
            # Outputting them all
            self.__partial_line = ""
        self._batchOutput(0, orecs)
        return True

    def eos_(self, input_port):
        if self.__partial_line:
            self._output(0, self.__partial_line)
        self._done()

    def seekOutput_(self, num_recs, output_port):
        if num_recs == -1:
            self.__partial_line = "" # Don't output anything on eos
            self._done()
            return True
        return False

class FileWriteDFN(SingleDataflowNode):
    """Write all incoming records into a file; note that the records
    must be strings."""
    def __init__(self, filename):
        SingleDataflowNode.__init__(self, num_output_ports=0)
        self.__filename = filename

    def initialize_(self):
        self.__file = open(self.__filename, "w")

    def input_(self, input_port, rec):
        self.__file.write(rec)

    def eos_(self, input_port=0):
        self.__file.close()
        self._done()

class GenerateIntervalDFN(SingleDataflowNode):
    """General a series of integers according to the usual python rules
    on the output.  Interval tuple is (start, bound[, stride]) as usual
    for python.  Primarily used for debugging."""
    def __init__(self, interval_tuple):
        SingleDataflowNode.__init__(self, num_input_ports=0)
        def gfunc():
            for i in range(*interval_tuple):
                yield i
        self.__iter = gfunc()
    def execute_(self, num_recs):
        try:
            while num_recs != 0:
                self._output(0, self.__iter.next())
                if num_recs != -1: num_recs -= 1
        except StopIteration:
            self._done()
            return False
        return True

class SortStreamDFN(SingleDataflowNode):
    """Batch the incoming input stream until eos seen, sort it, and
    dump it on the output.  Does not handle streams too large to fit
    in memory."""
    def __init__(self, uniquify=False):
        SingleDataflowNode.__init__(self)
        self.__uniquify_stream = uniquify
        self.record_array = []

    def input_(self, input_port, rec):
        self.record_array.append(rec)

    def eos_(self, input_port):
        self.record_array.sort()
        ## Uniquify if requested
        if self.__uniquify_stream:
            last = self.record_array[0]
            dst = src = 1
            while src < len(self.record_array):
                if self.record_array[src] != last:
                    self.record_array[dst] = last = self.record_array[src]
                    dst += 1
                src += 1
            self.record_array = self.record_array[:dst]
        ## Ship the sucker out.  Note that this could conceivably
        ## cause grief in complicated graphs if a merge point gets
        ## swamped from one direction and can't get input from another.
        ## It would be nice to figure out a general solution to this,
        ## but the general problem exists as long as people use
        ## _batchOutput and that's way too useful a feature, so I'm
        ## not going to try to solve the problem piecemeal.
        self._batchOutput(0, self.record_array)
        self._done()

## Testing
def printRec(rec):
    sys.stdout.write(rec)
    return rec                  # To use with filter.

def voidFunc(rec):
    pass

class InvalidStreamException(Exception): pass
class ValidateStream(SingleDataflowNode):
    """validate that an input stream corresponds to a pre-specified set of
    records; throw an invalid stream exception if not."""

    def __init__(self, expected_stream):
        SingleDataflowNode.__init__(self, 1, 0)
        self.__expected_stream = expected_stream
        self.__stream_pos = 0

    def input_(self, input_port, rec):
        if self.__stream_pos >= len(self.__expected_stream):
            raise InvalidStreamException("Record received past end of expected set:\n\t%s" % rec)
        if rec != self.__expected_stream[self.__stream_pos]:
            raise InvalidStreamException("Mismatch on stream at position %d; expected:\n\t|%s|\nand got:\n\t|%s|\n(Types: %s, %s)" % (self.__stream_pos, self.__expected_stream[self.__stream_pos], rec, type(self.__expected_stream[self.__stream_pos]), type(rec)))
        self.__stream_pos += 1

    def eos_(self, input_port):
        if self.__stream_pos != len(self.__expected_stream):
            raise InvalidStreamException("Stream ended early at position %d"% self.__stream_pos)
        self._done()

def test1(arg1, argr):
    g = GenerateIntervalDFN((2, 20, 4)) & SinkDFN(printRec)
    g.run()

mbox_file = "/Users/randy/utils/python/tmpoutfile"
crange = (101010110, 101010210)
lrange = (40, 60)
expected_list = (
    "Line number 5106061",
    "Line number 5106062",
    "Line number 5106063",
    "Line number 5106064",
    "Line number 5106065",
    "Line number 40",
    "Line number 41",
    "Line number 42",
    "Line number 43",
    "Line number 44",
    "Line number 45",
    "Line number 46",
    "Line number 47",
    "Line number 48",
    "Line number 49",
    "Line number 50",
    "Line number 51",
    "Line number 52",
    "Line number 53",
    "Line number 54",
    "Line number 55",
    "Line number 56",
    "Line number 57",
    "Line number 58",
    "Line number 59",
    )

def complexWindowTest(arg1, argr):
    mbox_fileobj = open(mbox_file)
    g = (FileSourceDFN(mbox_fileobj) & SplitDFN(2)
         & ((WindowDFN(crange[0], crange[1]) & StringNewlineBatchDFN())
            | (StringNewlineBatchDFN() & WindowDFN(lrange[0], lrange[1])))
         & SerialMergeDFN(2) & ValidateStream(expected_list))
    g.run()

def snlbatch_test(arg1, argr):
    mbox_fileobj = open(mbox_file)
    g = (FileSourceDFN(mbox_fileobj) & StringNewlineBatchDFN()
         & WindowDFN(40, 60) & SinkDFN(printRec))
    g.run()

def sort_test(arg1, argr):
    mbox_fileobj = open(mbox_file)
    g = (FileSourceDFN(mbox_fileobj) & SplitDFN(2)
         & ((WindowDFN(155, 305) & StringNewlineBatchDFN()) 
            | (StringNewlineBatchDFN() & WindowDFN(16, 28)))
         & SerialMergeDFN(2) & SortStreamDFN(uniquify=True)
         & ValidateStream(("Line number 11\n",
                           "Line number 12\n", 
                           "Line number 13\n", 
                           "Line number 14\n", 
                           "Line number 15\n", 
                           "Line number 16\n", 
                           "Line number 17\n", 
                           "Line number 18\n", 
                           "Line number 19\n", 
                           "Line number 20\n", 
                           "Line number 21\n", 
                           "Line number 22\n", 
                           "Line number 23\n", 
                           "Line number 24\n", 
                           "Line number 25\n", 
                           "Line number 26\n", 
                           "Line number 27\n", 
                           )))
    g.run()
    
test_function_mapping = {
    "simple_pipe" : test1,
    "complex_graph" : complexWindowTest,
    "snlbatch_test" : snlbatch_test,
    "sort_test" : sort_test
    }

if __name__ == "__main__":
    assert len(sys.argv) > 1, "No argument for test provided"
    test_name = sys.argv[1]
    assert test_name in test_function_mapping, "Test %s not found" % test_name
    test_function_mapping[test_name](test_name, sys.argv[2:])

## Local Variables: **
## compile-command: "./dataflow2.py sort_test" **
## End: **
