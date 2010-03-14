
import copy

## TODO: Don't forget to put in documentation for all of these
## TODO: Confirm what exception I should be inheriting from
## TODO: Split DataflowNode out into interface, simple, and composite

class DataflowNode(object):
    # Exceptions to throw
    def BadInputArguments(Exception):
        pass

    # Public methods
    def inputPorts(self): return self.__numInputs
    def outputPorts(self): return self.__numOutputs

    def copy(self):
        obj = copy.copy(self)
        obj.__initConnections() # Nuke any links; they're incorrect now.
        return obj

    # "Protected" interface (for use of derived classes
    def __init__(self, inputs=1, outputs=1, needThread=false):
        self.__numInputs = inputs
        self.__numOutputs = outputs

        # True if we need to get thread support when execute is
        # called on the graph this operator is a part of.
        self.__needThreading = needThread

        self.__initConnections(inputs, outputs)
        
    # "Private" interface, for use of class methods and friends
    # (CompositeDataflowNode, specifically)
    def __initConnections(self):
        # Used for both init and copy

        # Will be filled in with the operators in question
        self.__inputs = [None,] * self.__numInputs
        self.__outputs = [None,] * self.__numOutputs

        # True if EOS called by that input operator
        self.__eosSeenOnIn = [False,] * self.__numInputs

        # Non-zero if automatically processing an ignoreInput from
        # that output operators
        self.__ignoringOutRecords = [0,] * self.__numOutputs

    @staticmethod
    def __link(op1, op2):
        self.__checkOpArg(op1, "First argument to DataflowNode.__link")
        self.__checkOpArg(op2, "Second argument to DataflowNode.__link")
            
        # Simplistic single node link
        op1 = op1.copy()
        op2 = op2.copy()

        op1.__outputs[sourceport] = op2
        op2.__inputs[destport] = op1
        

def CompositeDataflowNode(DataflowNode):
    # Public interface

    # Constructor is considered public; may be called via:
    # CompositeDataflowNode() -- Null container
    # CompositeDataflowNode(op) -- Wrapper around single operator
    # CompositeDataflowNode(op, op) -- Links two ops

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

    def externalInputs(self):
        return self.__inputPorts[:]

    def externalOutputs(self):
        return self.__outputPorts[:]

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

    @staticmethod
    def __checkOpArg(op, argdesc):
        if (not isinstance(op, DataflowNode) and
            (len(op) != 2 or not isinstance(op[0], DataflowNode)
             or not isinstance(op[1], int))):
            raise BadInputArguments("%s isn't DataflowNode or (DataflowNode,int) tuple" % argdesc)

