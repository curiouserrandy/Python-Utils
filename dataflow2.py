
import copy
from sets import *

class DataflowNode(object):
    """Interface class to define type.  
In C++ this would be an abstract base class, in Java an interface."""
    # Exceptions to throw
    class BadInputArguments(Exception): pass
    class NotImplemented(Exception): pass
    class BadGraphConfig(Exception): pass

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
        # I'll spare myself an "Ow" in anticipation of this routine
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
        elif len(args) == 2:
            __checkOpArg(args[0], "First argument to composite node constructor");
            __checkOpArg(args[1], "Second argument to composite node constructor");
            self.__initFromDual(args[0], args[1])
        else:
            raise BadInputArguments("Too many arguments to CompositeDataflowNode constructor: " + args.repr())

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

    def __initFromDual(self, inputOp, outputOp):
        # Break out source and dest port info
        destport = sourceport = 0
        if not isinstance(op1, DataflowNode):
            sourceport = op1[1]
            op1 = op1[0]
        if not isinstance(op2, DataflowNode):
            destport = op2[1]
            op2 = op2[0]

        # Force into the Composite<->Composite init case
        # Copy while doing so to make safe for editing.
        if not isinstance(op1, CompositeDataflowNode):
            op1 = CompositeDataflowNode(op1)
        else:
            op1 = op1.copy()
        if not isinstance(op2, CompositeDataflowNode):
            op2 = CompositeDataflowNode(op1)
        else:
            op2 = op2.copy()
            
        # Create new node list.  Because of the copy, the suboperators
        # can be addressed directly.
        self.__subOperators = op1.__subOperators + op2.__subOperators

        # Modify the op2 port lists to take into account new operator
        # offsets
        offset = len(op1.__subOperators)
        op2iPortList = [(iport[0]+op2SubOpOffset, iport[1])
                        for iport in op2.__inputPorts]
        op2oPortList = [(oport[0]+op2oPortList, oport[1])
                        for oport in op2.__outputPorts]

        # The final operators output port lists are the output port list
        # of the source node, with the output port involved in the
        # link replaced with the output ports of the destination node.
        # The same thing applies in reverse for the input ports.
        outPortList = op1.__outputPorts
        outPortList[sourceport:sourceport] = op2oPortList
        self.__outputPorts = outPortList
        inPortList = op2iPortList
        inPortList[destport:destport] = op1.__inputPorts
        self.__inputPorts = inPortList

        # Make the actual link that all this was about
        sourcetuple = op1.__outputPorts[sourceport]]
        desttuple = op2.__inputPorts[destport]
        desttuple = (desttuple[0] + offset, desttuple[1])
        DataflowNode._DataflowNode_link(
            (self.__subOperators[sourcetuple[0]], sourcetuple[1]),
            (self.__subOperators[desttuple[0]], desttuple[1])
            )

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

