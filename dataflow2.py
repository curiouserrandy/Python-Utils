
import copy
from sets import *

# Note: Changing the way that port numbers map when operators are combined
# from being based around the nature of the combination of the operator
# (splitting the space around the link between the operators) to being order
# of operator dependent (If there are two operators A & B, and A has two
# inputs and B has five, then the combined operator will have 7 inputs, the
# first two of which are As and the last five of which are B's.  If
# some of those inputs are used in linking A & B, they'll be elided
# from that order).  

# Exceptions used by module
class BadInputArguments(Exception): pass
class NotImplemented(Exception): pass
class BadGraphConfig(Exception): pass

class DataflowNode(object):
    """Interface class to define type.  
In C++ this would be an abstract base class, in Java an interface."""
    # Public interface
    def inputPorts(self):
        raise NotImplemented("Method inputPorts not overridden in inherited class.")
    def outputPorts(self):
        raise NotImplemented("Method outputPorts not overridden in inherited class.")

class SingleDataflowNode(DataflowNode):
    ### Public methods
    def inputPorts(self): return self.__numInputs
    def outputPorts(self): return self.__numOutputs

    def copy(self):
        obj = copy.copy(self)
        obj.__initConnections() # Nuke any links; they're incorrect now.
        return obj

    ### "Protected" interface (for use of derived classes
    def __init__(self, inputs=1, outputs=1, needThread=false):
        self.__numInputs = inputs
        self.__numOutputs = outputs

        # True if we need to get thread support when execute is
        # called on the graph this operator is a part of.
        self.__needThreading = needThread

        # Setup the basic connection tracking
        self.__initConnections(inputs, outputs)
        
    def signalEos(self, outputPort=0):
        assert self.__outputs[outputPort]
        myInputIndex = self.__outputs[outputPort].__inputs.index(self)
        self.__outputs[outputPort].eos(myInputIndex)
        self.__outputs[outputPort] = None

    def ignoreInput(self, numRecords=-1, inputPort=0):
        assert self.__inputs[inputPort]
        myOutputIndex = self.__inputs[inputPort].__outputs.index(self)
        if not self.__inputs[inputPort].seekOutput(numRecords, myOutputIndex):
            assert len(self.__inputs[inputPort].__ignoringOutRecords) > myOutputIndex
            self.__inputs[inputPort].__ignoringOutRecords[myOutputIndex] = numRecords
        
    def done(self):
        for i in range(self.__numInputs):
            self.ignoreInput(inputPort=i)
        for i in range(self.__numOutputs):
            self.signalEos(i)
        self.__needThreading = False

    def output(self, outputPort, rec):
        assert self.__outputs[outputPort] # Skip for performance?
        if self.__ignoringOutRecords[outputPort] != 0:
            self.__ignoringOutRecords[outputPort]--
        else:
            self.__outputs[outputPort].input(self.__outputNodeInputs[outputPort], rec)

    ### Stubs of functions that derived classes may choose to implement
    def eos(self, inputPort): pass
    def seekOutput(self, numRecords, outputPort): return False
    def execute(self, numrecords): pass
    def initialize(self): pass
    def input(self, inputPort, rec):
        raise NotImplemented("input method for SingleDataflowNode not implemented in derived class.")
        
    ### "Private" interface, for use of class methods and friends
    ### (CompositeDataflowNode, specifically)
    def __initConnections(self):
        # Used for both init and copy

        # Will be filled in with the operators in question
        self.__inputs = [None,] * self.__numInputs
        self.__outputs = [None,] * self.__numOutputs
        # Input port # on peer corresponding to our output port.
        self.__outputNodeInputs = [None,] * self.__numOutputs

        # Non-zero if automatically processing an ignoreInput from
        # that output operators
        self.__ignoringOutRecords = [0,] * self.__numOutputs

    @staticmethod
    def __link(source, dest):
        """Make a link between the actual operators passed (side effects
        args).  Both SOURCE and DEST are tuples of the form (op, port)."""
        (sourceop, sourceport) = source
        (destop, destport) = dest
        self.__checkOpArg(sourceop, "First argument to DataflowNode.__link")
        self.__checkOpArg(destop, "Second argument to DataflowNode.__link")
            
        op1.__outputs[sourceport] = op2
        op1.__outputNodeInputs[sourceport] = destport
        op2.__inputs[destport] = op1
        

def CompositeDataflowNode(DataflowNode):
    # Public interface

    # Constructor is considered public; may be called via:
    # CompositeDataflowNode() -- Null container
    # CompositeDataflowNode(op) -- Wrapper around single operator
    # CompositeDataflowNode(op, op) -- Links two ops

    def inputPorts(self):
        return len(self.__inputPorts)

    def outputPorts(self):
        return len(self.__outputPorts)

    def run(self):
        """Run the dataflow graph contained in this object."""
        ### Check:
        ###	* Graph self-contained
        ###	* No cycles
        ###	* Not disjoint (do I really care?)
        ### Get list of thread support needed.
        ### Call those items in the right order.
        assert self.inputPorts() == 0
        assert self.outputPorts() == 0

        self.__checkForCycles()

        # Arguably disjoint graphs should be ok; I could imagine cases
        # in which you'd want a composite operator that did two things
        # in parallel.  But the current construction mechanism doesn't
        # allow for disjoint graphs, and so asserting for it seems a wise
        # idea
        self.__checkConnected()

        # Initialize the graph
        for n in self.__subOperators:
            n.initialize()

        # Get list of drivers
        drivers = [n for n in self.__subOperators
                   if n._SingleDataflowNode_needThreading]

        # Drive them
        if len(drivers) == 1:
            drivers[0].execute()
        else:
            while drivers:
                driverCopy = drivers[:]
                for d in driverCopy:
                    d.execute(1)
                    if not d._SingleDataflowNode_needThreading:
                        drivers.remove(d)

    def nodeList(self):
        return [op.copy() for op in self.__subOperators]

    def internalLinks(self):
        """Returns the links between the simple operator that form
        this composite operator.  Links are of the form
        ((source_op_idx, source_port), (dest_op_idx, dest_port)).
        The op_idx are indices into the list returned by nodeList()."""
        linklist = []
        for (sourceNodeIdx, sourceNode) in enumerate(self.__subOperators):
            for sourcePort in range(sourceNode.outputs()):
                destNode = sourceNode._DataflowNode_outputs
                if destNode is not None:
                    destNodeIdx = self.__subOperators.index(destNode)
                    destport = destNode._DataflowNode_inputs.index(sourceNode)
                    linklist.append((sourceNodeIdx, sourcePort), (destNodeIdx, destPort))
        return linklist

    def externalInputs(self): return self.__inputPorts[:]
    def externalOutputs(self): return self.__outputPorts[:]

    def internalLink(self, outputport, inputport):
        # For creating links within already existing graphs; i.e. merges
        raise NotImplemented("CompositeDataflowNode.internalLink not yet implemented.")
        

    def copy(self):
        obj = CompositeDataflowNode()
        # Safe to make shallow copy as entries are tuples, which are immutable
        obj.__outputPorts = self.__outputPorts[:]
        obj.__inputPorts = self.__inputPorts[:]

        # New copy of list
        obj.__subOperators = [o.copy() for o in self.__subOperators[:]]

        # Re-create internal links
        for l in self.internalLinks():
            (sourceidx, sourceport, destidx, destport) = l
            DataflowNode._DataflowNode_link(
                (obj.__subOperators[sourceidx], sourceport),
                (obj.__subOperators[destidx], destport)
                )

        return obj

    # Protected (null; this is a final class not intended for inheritance).

    # Private
    def __init__(self, *args):
        self.__subOperators = []
        self.__inputPorts = []  # Will look like (opidx#, iport#)
        self.__outputPorts = [] # Will look like (opidx#, oport#)

        if len(args) == 0:
            return # Composite op with no components
        elif len(args) == 1:
            __checkOpArg(args[0], "First argument to composite node constructor");
            self.__initFromSingleton(self, args[0])
        else:
            self.__initFromList(args)

    def __initFromSingleton(self, op):
        op = op.copy()
        if isinstance(op, CompositeDataflowNode):
            self.__subOperators = op.__subOperators
            self.__inputPorts = op.__inputPorts
            self.__outputPorts = op.__outputPorts
        else:
            self.__subOperators = [op.copy()]
            self.__inputPorts = [(0, i) for i in range(len(op.inputPorts()))]
            self.__outputPorts = [(0, i) for i in range(len(op.outputPorts()))]

    eSerial = 1
    eParallel = 2
    def __initFromList(self, nodes, links=eSerial):
        """Create a composite DFN from the passed in nodes and
        inter-node links specified.  NODES should be a list of
        DataFlowNodes (either single or composite).  LINKS may be
        eSerial, eParallel, or a list of the form ((sourcenodeindex,
        sourceport), (destnodeindex, destport)).  If eSerial, each
        pair of adjacent nodes in the node list must have matching
        inputs and outputs, which will be connected.  If eParallel, no
        connections are done--all input links for all nodes will be
        presented by the composite node (in the order passed) and the
        same will be true for the output links.  """

        # Validate nodes
        for n in nodes:
            if not isinstance(node, DataflowNode):
                raise BadInputArguments("Argument NODES to CompositeDataflowNode constructor contains invalid node %s" % node)

        # Validate link list
        if (links != self.eSerial and links != self.eParallel &&
            not isinstance(links, list)):
            raise BadInputArguments("Argument LINKS to CompositeDataflowNode constructor has invalid value: %s" % links)
        if isinstance(links, list):
            for l in links:
                if not (0 <= l[0][0] < len(nodes)):
                    raise BadInputArguments("Link %s in CompositeDataflowNode constructor has invalid source node index." % l)
                if not 0 <= l[0][1] < nodes[l[0][0]].outputPorts():
                    raise BadInputArguments("Link %s in CompositeDataflowNode constructor has invalid source port index." % l)
                if not (0 <= l[1][0] < len(nodes)):
                    raise BadInputArguments("Link %s in CompositeDataflowNode constructor has invalid destination node index." % l)
                if not 0 <= l[1][1] < nodes[l[1][0]].inputPorts():
                    raise BadInputArguments("Link %s in CompositeDataflowNode constructor has invalid destination port index." % l)

        # Turn everything composite
        nodes = [CompositeDataflowNode(n) for n in nodes]

        # Verify eSerial requirement & create real link list from
        # symbolic args
        if links==eParallel:
            links = []          # No extra links to form
        if links==eSerial:
            links = []
            for i in range(len(nodes)-1):
                if nodes[i].outputPorts() != nodes[i+1].inputPorts():
                    raise BadInputArguments("""
Serial CompositeDataflowNode creation requirement failure:
%s op number of outputs (%d) is different from %s op number of inputs (%d)"""
                                            % (nodes[i].repr(),
                                               nodes[i].outputPorts(),
                                               nodes[i+1].repr(),
                                               nodes[i+1].inputPorts()))
                links += [((i, j), (i+1,j)) for j in range(nodes.[i].outputPorts())]

        # Copy everything in, recording offsets
        offsets = reduce(lambda x, y: x + [x[-1]+y,],
                         [len(n.__SubOperators) for n in nodes],
                         [0])
        self.__subOperators = reduce(lambda x, y: x+y,
                                     [n.__subOperators for n in nodes])
        
        # Create internal links from each of the arguments
        for (i,n) in enumerate(nodes):
            node_offset = offsets[i]
            links = n.internalLinks()
            SingleDataflowNode._SingleDataflowNode_link(
                (self.__subOperators[node_offset+links[0][0]], links[0][1]),
                (self.__subOperators[node_offset+links[1][0]], links[0][1])
                )
                
        # Create lists of lists of mappings.  The outer list is
        # indexed by composite operator index (from nodes), and the
 	# inner list by composite operator port number, and the
        # results of the mapping are simple operator indices within
        # the newly created operator. 
        #
        # cnode: Composite node whose ports are being transformed
        # cnode_i: Index of that composite node within nodes
        # ni, pi: Node and port indices within that composite node
        input_port_mappings = [
            [(ni+offsets[cnode_i], pi)
             for ni, pi in cnode.__inputPorts]
            for cnode, cnode_i in enumerate(nodes)]
        output_port_mappings = [
            [(ni+offsets[cnode_i], pi)
             for ni, pi in cnode.__outputPorts]
            for cnode, cnode_i in enumerate(nodes)]

        # Execute each link in the links list, modifying the input
        # and output port mappings as you go.
        for source, dest in links:
            (sourcecnode, sourcecport) = source
            (destcnode, destcport) = dest

            # Removing the descriptor keeps the mappings accurate as
            # the internal links are made
            sourceportdescr = output_port_mappings[sourcecnode].pop(sourcecport)
            destportdescr = input_port_mappings[destcnode].pop(destcport)
            SingleDataflowNode._SingleDataflowNode_link(
                (self.__subOperators[sourceportdescr[0]], sourceportdescr[1]),
                (self.__subOperators(destportdescr[0]], destportdescr[1])
                )

        # What's left in the mappings, flatted, should describe the remaining
        # open ports
        self.__inputPorts = reduce(lambda x, y: x+y, input_port_mappings)
        self.__outputPorts = reduce(lambda x, y: x+y, output_port_mappings)

    def __checkForCycles(self):
        # Get a list of links (no ports; you don't care)
        links = [(s[0], d[0]) for (s, d) in self.internalLinks()]
        lastLinksLength = 0
        
        # Repeatedly prune link list of references to nodes that have no
        # inputs
        while lastLinksLength != len(links):
            lastLinksLength = len(links)
            destNodes = set(zip(*links)[1])
            links = [l for l in links if l[0] in destNodes or l[1] in destNodes]

        if len(links) != 0:
            # We have a loser!
            # The right way to do this error message would be to output
            # all the cycles detected.  But that would take a fair amount
            # of work for a rare event, and one in which the user can probably
            # figure out exactly what's going on just from the list of nodes.
            # So I'll put off the more complicate error message until I need
            # it.
            nodeList = zip(*links)
            nodeList = nodeList[0] + nodeList[1] # Flatten
            nodeList = dict.fromkeys(nodeList).keys() # Uniquify
            msg = "Cycle detected among DataflowNodes: ("
            msg += ", ".join([self.__subOperators[n].repr() for n in nodeList])
            msg += ")"
            raise BadGraphConfig(msg)

    def __checkConnected(self):
        # Idea is to start from a random link, and explore in
        # an undirected fashion from that link, marking nodes as
        # visited.  If there are unvisited nodes when we're done, we're
        # disjoint
        nodes = set(range(len(self.__subOperators)))
        if len(nodes) == 1: return

        links = self.internalLinks()
        nodesVisited = set(links.pop()) # Starts with two nodes
        nodesVisitedLastSize = -1
        while nodesVisitedLastSize != len(nodesVisited):
            nodesVisitedLastSize = len(nodesVisited)
            for l in links:
                if l[0] in nodesVisited or l[1] in nodesVisited:
                    links.remove(l)
                    nodesVisited.add(l[0])
                    nodesVisited.add(l[1])
                    
        if links:
            # Graph is Disjoint
            nodesNotVisited = nodes - nodesVisited

            msg = "Disjoint sets of nodes found.  \n"
            msg += ("First set: (" +
                    ", ".join([self.__subOperators[o].repr()
                               for o in nodesVisited])
                    + ")\n")
            msg += ("First set: (" +
                    ", ".join([self.__subOperators[o].repr()
                               for o in nodesNotVisited])
                    + ")\n")
            raise BadGraphConfig(msg)

    @staticmethod
    def __checkOpArg(op, argdesc):
        if (not isinstance(op, DataflowNode) and
            (len(op) != 2 or not isinstance(op[0], DataflowNode)
             or not isinstance(op[1], int))):
            raise BadInputArguments("%s isn't DataflowNode or (DataflowNode,int) tuple" % argdesc)

