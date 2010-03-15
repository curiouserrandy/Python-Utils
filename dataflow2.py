
import copy
from sets import *

# Note: Changing the way that port numbers map when nodes are combined
# from being based around the nature of the combination of the node
# (splitting the space around the link between the nodes) to being order
# of node dependent (If there are two nodes A & B, and A has two
# inputs and B has five, then the combined node will have 7 inputs, the
# first two of which are As and the last five of which are B's.  If
# some of those inputs are used in linking A & B, they'll be elided
# from that order).  

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

# Naming conventions

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
# * List of any obove objects: Suffix "s"
# * Size of a list of any above objects: num_<obj>s

# Exceptions used by module
class BadInputArguments(Exception): pass
class NotImplemented(Exception): pass
class BadGraphConfig(Exception): pass

# Enums used in module class interfaces
eSerial = 1
eParallel = 2

class DataflowNode(object):
    """Interface class to define type.  
In C++ this would be an abstract base class, in Java an interface."""
    # Public interface
    def numInputPorts(self):
        raise NotImplemented("Method numInputPorts not overridden in inherited class.")
    def numOutputPorts(self):
        raise NotImplemented("Method numOutputPorts not overridden in inherited class.")

    def __and__(self, node):
        """Connect two dataflow nodes in series, with the outputs of the
        first linked to the inputs of the second.
        As appropriate to the "&"& operator, this is a copy operator; it will
        not modify its arguments."""
        if not isinstance(node, DataflowNode):
            raise BadInputArguments("Argument to DataflowNode & operator (%s) is not a DataflowNode." % node)
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
            raise BadInputArguments("Argument to DataflowNode & operator (%s) is not a DataflowNode." % node)
        return CompositeDataflowNode((self, node), eParallel)

class SingleDataflowNode(DataflowNode):
    ### Public methods
    def numInputPorts(self): return self.__num_input_ports
    def numOutputPorts(self): return self.__num_output_ports

    def copy(self):
        copy_node = copy.copy(self)
        copy_node.__initConnections() # Nuke any links; they're incorrect now.
        return copy_node

    ### "Protected" interface (for use of derived classes
    def __init__(self, num_input_ports=1, num_output_ports=1):
        self.__num_input_ports = num_input_ports
        self.__num_output_ports = num_output_ports

        # Setup the basic connection tracking
        self.__initConnections(num_input_ports, num_output_ports)
        
    def _signalEos(self, output_port=0):
        assert self.__output_nodes[output_port]
        dest_self_iport = self.__output_nodes[output_port].__input_nodes.index(self)
        self.__output_nodes[output_port].eos_(dest_self_iport)
        self.__output_nodes[output_port] = None

    def _ignoreInput(self, num_recs=-1, input_port=0):
        assert self.__input_nodes[input_port]
        src_self_oport = self.__input_nodes[input_port].__output_nodes.index(self)
        if not self.__input_nodes[input_port].seekOutput_(num_recs, src_self_oport):
            assert len(self.__input_nodes[input_port].__ignoring_output_records) > src_self_oport
            self.__input_nodes[input_port].__ignoring_output_records[src_self_oport] = num_recs
        
    def _done(self):
        for i in range(self.__num_input_ports):
            self.ignoreInput(input_port=i)
        for i in range(self.__num_output_ports):
            self.signalEos(i)

    def _output(self, output_port, rec):
        assert self.__output_nodes[output_port] # Skip for performance?
        if self.__ignoring_output_records[output_port] != 0:
            self.__ignoring_output_records[output_port]--
        else:
            self.__output_nodes[output_port].input_(self.__output_node_iports[output_port], rec)

    ### Stubs of functions that derived classes may choose to implement
    def input_(self, input_port, rec):
        """Override to accept input from upstream nodes."""
        raise NotImplemented("SingleDataflowNode.input_ method not implemented in derived class.")

    def eos_(self, input_port):
        """Override if notification of end of stream (no further input
        will be provided) is wanted; this function will be called when the
        node linked to the indicated port signals EOS."""
        pass

    def seekOutput_(self, num_recs, output_port):
        """Override if a request from a downstream node to seek
        forward NUM_RECS in the stream can be handled in some
        efficient fashion by the node.  If this function returns
        False, the infrastructure will manually skip the records; if it
        returns True, the responsibility for skipping them has been
        accepted by the derived class.

        Note that for pure transformation nodes (one record out for each
        record in, no maintained state) this should be overridden to pass the
        notification upstream; if a record is going to be dropped, it should be
        dropped as far upstream as possible."""
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
        amount of processing may be done.  False should be returned if this
        routine does not need to be called again, True if there is more
        processing for the node to do."""
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
        # Used for both init and copy

        # Will be filled in with the nodes in question
        self.__input_nodes = [None,] * self.__num_input_ports
        self.__output_nodes = [None,] * self.__num_output_ports
        # Input port # on peer corresponding to our output port.
        self.__output_node_iports = [None,] * self.__num_output_ports

        # Non-zero if automatically processing an ignoreInput from
        # that output nodes
        self.__ignoring_output_records = [0,] * self.__num_output_ports

    @staticmethod
    def __link(snodeport, dnodeport):
        """Make a link between the actual nodes passed (side effects
        args).  Both SNODEPORT and DNODEPORT are tuples of the form
        (node, port)."""
        (snode, sport) = snodeport
        (dnode, dport) = dnodeport
        self.__checkArgIsNode(snode, "First argument to DataflowNode.__link")
        self.__checkArgIsNode(dnode, "Second argument to DataflowNode.__link")
            
        snode.__output_nodes[sport] = dnode
        snode.__output_node_iports[sport] = dport
        dnode.__input_nodes[dport] = snode
        

class CompositeDataflowNode(DataflowNode):
    ### Public interface

    ## Creating structure

    # Constructor is considered public; may be called via:
    # CompositeDataflowNode() -- Null container
    # CompositeDataflowNode(node) -- Wrapper around single node
    # CompositeDataflowNode(nodes, links) -- Links two or more nodes
    def __init__(self, *args):
        self.__contained_nodes = []

	# These arrays map from the composite node port# to
        # a port descriptor for an internal node
        self.__input_port_descrs = []  
        self.__output_port_descrs = [] 

        if len(args) == 0:
            return # Composite node with no components
        elif len(args) == 1:
            self.__checkArgIsNode(args[0], "First argument to composite node constructor");
            self.__initFromSingleton(self, args[0])
        else:
            self.__initFromList(*args)

    def addNode(self, node, links=eSerial):
        """Add a new node to an existing CompositeDataflowNode.  The
        new node may be Single or Composite.  LINKS is a list of links
        ((sport_descr, dport_descr) tuples) in which all node indices are
        0 (referring to self) or 1 (referring to node)."""

        # Validate arguments
        if not isinstance(node, DataflowNode):
            raise BadInputArguments("Arg NODE (%s) to method CompositeDataflowNode.addNode isn't a DataflowNode" %s node)

        self.__checkLinksArg(links, (self,node), "CompositeDataflowNode.addNode")

        # Translate symbolic links argument to list
        if links == eParallel:
            links = []
        elif links == eSerial:
            links = [((0,i),(1,i)) for i in range(node.numInputPorts())]

        # Save the important data about ourselves before consuming the new
        # node, then eat it.  This will produce a valid composite node without
        # any of the links having been executed.
        oport_offset = len(self.__output_port_descrs)
        iport_offset = len(self.__input_port_descrs)
        self.__addNodeNoLinks(node)

        # Execute the links 
        oiports = [(l[0][1] if l[0][0] == 0 else l[0][1] + oport_offset,
                    l[1][1] if l[1][0] == 0 else l[1][1] + iport_offset)
                   for l in links]
        # Transpose the above array into (oports, iports) and pass
        # that list as the args list to makeInternalLinks
        self.makeInternalLinks(*zip(*oiport))

    def makeInternalLinks(self, output_ports, input_ports):
        # For creating links within already existing graphs; i.e. merges

        # Get the descriptors without removing them since that would change
        # the mapping for future descriptors
        oport_descrs = [self.__output_port_descrs[port]
                        for port in output_ports]
        iport_descrs = [self.__output_port_descrs[port]
                        for port in output_ports]

        # Remove those ports from the list; they're about to be used up
        self.__output_port_descrs = [self.__output_port_descrs[i]
                                     for i in self.numOutputPorts()
                                     if i not in output_ports]
        self.__input_port_descrs = [self.__input_port_descrs[i]
                                     for i in self.numInputPorts()
                                     if i not in input_ports]

        # Make all the links
        for oport_descr, iport_descr in zip(oport_descrs, iport_descrs):
            SingleDataflowNode._SingleDataflowNode_link(
                (self.__contained_nodes[oport_descr[0]], oport_descr[1]),
                (self.__contained_nodes[iport_descr[0]], iport_descr[1])
                )
        
    def run(self):
        """Run the dataflow graph contained in this object."""
        ### Check:
        ###	* Graph self-contained
        ###	* No cycles
        ###	* Not disjoint
        ### Call all initialize routines
        ### Drive graph by calling execute routines of nodes that need it.

        assert self.numInputPorts() == 0
        assert self.numOutputPorts() == 0

        self.__checkAcyclic()

        # Arguably disjoint graphs should be ok; I could imagine cases
        # in which you'd want a composite node that did two things
        # in parallel.  But the current construction mechanism doesn't
        # allow for disjoint graphs, and so asserting for it seems a wise
        # idea
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
            num_recs = 1 if len(nodes1) > 1 else -1
            for d in nodes1:
                if not d.execute_(num_recs):
                    nodes.remove(d)

    def copy(self):
        copy_node = CompositeDataflowNode()
        # Safe to make shallow copy as entries are tuples, which are immutable
        copy_node.__output_port_descrs = self.__output_port_descrs[:]
        copy_node.__input_port_descrs = self.__input_port_descrs[:]

        # New copy of list
        copy_node.__contained_nodes = [o.copy() for o in self.__contained_nodes[:]]

        # Re-create internal links
         for l in self.internalLinks():
            (src_node_idx, src_port, dest_node_idx, dest_port) = l
            DataflowNode._DataflowNode_link(
                (copy_node.__contained_nodes[src_node_idx], src_port),
                (copy_node.__contained_nodes[dest_node_idx], dest_port)
                )

        return copy_node

    # Instance information probes
    def numInputPorts(self):
        return len(self.__input_port_descrs)

    def numOutputPorts(self):
        return len(self.__output_port_descrs)

    def internalNodes(self):
        return [node.copy() for node in self.__contained_nodes]

    def internalLinks(self):
        """Returns the links between the simple node that form
        this composite node.  Links are of the form
        ((source_op_idx, source_port), (dest_op_idx, dest_port)).
        The op_idx are indices into the list returned by internalNodes()."""
        links = []
        for (src_node_idx, src_node) in enumerate(self.__contained_nodes):
            for src_port in range(src_node.num_output_ports()):
                dest_node = src_node._DataflowNode_outputs
                if dest_node is not None:
                    dest_node_idx = self.__contained_nodes.index(dest_node)
                    dest_port = dest_node._DataflowNode_inputs.index(src_node)
                    links.append((src_node_idx, src_port), (dest_node_idx, dest_port))
        return links

    def inputPortDescrs(self): return self.__input_port_descrs[:]
    def outputPortDescrs(self): return self.__output_port_descrs[:]

    # Operator overloading
    def __iand__(self, node):
        """Link the argument node into this one, attaching all outputs of
        this node to all inputs of the argument node.
        A copy is made of the argument node, but this node is modified."""
        self.addNode(node, eSerial)
    def __ior__(self, node):
        """Link the argument node into this one, exposing all inputs
        and outputs of both nodes in the resulting node.
        A copy is made of the argument node, but this node is modified."""
        self.addNode(node, eParallel)

    # Protected (null; this is a final class not intended for inheritance).

    # Private
    def __initFromSingleton(self, node):
        node = node.copy()
        if isinstance(node, CompositeDataflowNode):
            self.__contained_nodes = node.__contained_nodes
            self.__input_port_descrs = node.__input_port_descrs
            self.__output_port_descrs = node.__output_port_descrs
        else:
            self.__contained_nodes = [node.copy()]
            self.__input_port_descrs = [(0, i) for i in range(len(node.inputPorts()))]
            self.__output_port_descrs = [(0, i) for i in range(len(node.numOutputPorts()))]

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

        # Validate nodes
        for n in nodes:
            if not isinstance(node, DataflowNode):
                raise BadInputArguments(
                    "Argument NODES to CompositeDataflowNode constructor contains invalid node %s" % node)

        # Validate link list
        self.__checkLinksArg(links, nodes, "CompositeDataflowNode(nodes, links) constructor")

        # Create a real link list from symbolic args
        if links==eParallel:
            links = []          # No extra links to form
        if links==eSerial:
            links = []
            for i in range(len(nodes)-1):
                links += [((i, j), (i+1,j))
                          for j in range(nodes.[i].numOutputPorts())]

        # Turn everything composite
        nodes = [CompositeDataflowNode(n) for n in nodes]

        # Record the offsets needed
        port_descr_iport_offsets = reduce(lambda x, y: x + [x[-1]+y,],
                                          [len(n.__input_port_descrs)
                                           for n in nodes],
                                          [0])
        port_descr_oport_offsets = reduce(lambda x, y: x + [x[-1]+y,],
                                          [len(n.__output_port_descrs)
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
        self.makeInternalLinks(*zip(*intlinks))

    def __addNodeNoLinks(self, node):
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
        # Get a list of links (skipping ports; unneeded for this algorithm)
        links = [(s[0], d[0]) for (s, d) in self.internalLinks()]
        last_num_links = 0
        
        # Repeatedly prune link list of references to nodes that have no
        # inputs
        while last_num_links != len(links):
            last_num_links = len(links)
            dest_nodes = set(zip(*links)[1])
            links = [l for l in links if l[0] in dest_nodes or l[1] in dest_nodes]

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
            msg += ", ".join([self.__contained_nodes[n].repr() for n in nodes])
            msg += ")"
            raise BadGraphConfig(msg)

    def __checkConnected(self):
        # Idea is to start from a random link, and explore in
        # an undirected fashion from that link, marking nodes as
        # visited.  If there are unvisited nodes when we're done, we're
        # disjoint
        nodes = set(range(len(self.__contained_nodes)))
        if len(nodes) == 1: return

        links = self.internalLinks()
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
                    ", ".join([self.__contained_nodes[o].repr()
                               for o in visited_nodes])
                    + ")\n")
            msg += ("First set: (" +
                    ", ".join([self.__contained_nodes[o].repr()
                               for o in unvisited_nodes])
                    + ")\n")
            raise BadGraphConfig(msg)

    @staticmethod
    def __checkArgIsNode(node, arg_descript):
        if (not isinstance(node, DataflowNode) and
            (len(node) != 2 or not isinstance(node[0], DataflowNode)
             or not isinstance(node[1], int))):
            raise BadInputArguments("%s isn't DataflowNode or (DataflowNode,int) tuple" % arg_descript)

    @staticmethod
    def __checkLinksArg(links, nodes, method_name):
        """Confirm that the LINKS argument is valid in the context of the
        node list.  This means that it's a link list with valid
        values in the context of the node list, or it's eParallel, or it's
        eSerial and adjacent nodes have matching number of input and output
        ports."""
        if not (links == eSerial or links == eParallel
                or isinstance(links, list)):
            raise BadInputArguments("Args LINKS (%s) to method %s isn't eSerial, eParallel or a list." % (list, method_name))
        if isinstance(links, list):
            for l in links:
                if not (0 <= l[0][0] < range(len(nodes))
                        and 0 <= l[1][0] < range(len(nodes))):
                    raise BadInputArguments("Link %s in arg LINKS to method %s contains a reference to an out of bounds node." % (l, method_name))
                if not 0 <= l[0][1] < nodes[l[0][0]].numOutputPorts():
                    raise BadInputArguments("Link %s in arg LINKS to method %s contains an out of range output port (%d)." % (l, method_name, l[0][1]))
                if not 0 <= l[1][1] < nodes[l[1][0]].numInputPorts():
                    raise BadInputArguments("Link %s in arg LINKS to method %s contains an out of range input port (%d)." % (l, method_name, l[1][1]))

        if links == eSerial:
            for i in range(len(nodes)-1):
                if nodes[i].numOutputPorts() != nodes[i+1].numInputPorts():
                    raise BadInputArguments("Method %s called with eSerial and non-matching numbers of output (%d) and input (%d) ports on nodes %d, %d." % (method_name, nodes[i].numOutputPorts(), nodes[i+1].numInputPorts(), i, i+1))

        
