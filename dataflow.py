import operator

### Setting up a quick dataflow structure model for handling
### Originally inspired for handling transformations in importing mail
### messages for the mail database project (see Projects/MailSys) but
### useful enough that I wanted it globa.

### XXX: There probably are improvements to make in the base abstraction 
### around handling multiple intput and output links.  Oh, well.

### XXX: Some logging in the base abstraction would be good.  For the 
### current implementation, central logging showing input and output 
### along each link would be grand.  I think this requires inputs to 
### pass through the base class and call up, which is fine.  

### Naming conventions for internally/externally called functions and
### should/shouldn't be overridden: 
### If they're external I want them to be easy to call, so I don't want
### to distinguish override/non-override in the name.  I'll follow that
### pattern internally as well.  Internal functions will have an _ prepended.

### XXX: Should have classes export schema and have that schema checked on
### linkage.

### XXX: You may want tagged inputs and outputs.  Heck, you may want
### both tagged and numbered outputs; numbered for multiple items of
### the same type, and tagged for different categories.  

### XXX: Specify interface more completely (specifically to superclasses, and
### to external functions).  

### XXX: Might want to think about operator overloading to link DFNs
### (possibly mimic building a list; stream DFN container?  Any good 
### syntactic sugar for splits?)

class DataflowNode(object):
    """Base class for node in a dataflow network.  Takes an input record,
does some type of transformation on it, and outputs some other record.  
Default action is just to pass things through.

Note that only input, _localEos, and _validate_link are intended to be overridden by 
descendants."""
    def __init__(self):
        self.outputFunctions = []
        self.outputEos = []
        # Default to a single input.  If there are more from other DFNs,
        # the array will expand automatically, and it currently doesn't
        # make sense to have no inputs for a DFN.  
        self.eosSeen = [False,]  

    # input and eos are both called by both user and internal links
    def input(self, record, inputLink=0):
        "Default behavior is assertion exception; descendants should override."
        assert False, "DataflowNode class not meant to be used directly."

    def eos(self, inputLink=0):
        self.eosSeen[inputLink] = True
        if reduce(operator.and_, filter(lambda x: operator.is_not(x, None),
                                        self.eosSeen)):
            self._localEos()
            for f in self.outputEos:
                if f: f()

    def setupInput(self, inputLink):
        """Setup a specific external input for multi-external input
        nodes."""
        assert inputLink > 1
        self.eosSeen += \
            [None,] * max(0,inputLink - len(self.eosSeen) + 1)
        self.eosSeen[inputLink] = False

    def _firstOpenOutput(self):
        """Used by subclasses to do auto-linking of multiple outputs."""
        for i in range(len(self.outputFunctions)):
            if self.outputFunctions is None:
                return i
        return len(self.outputFunctions)

    def _validate_link(self, linknum, input_p):
        """Should be overridden if only some links are valid."""
        return True

    def _localEos(self):
        """Internal function called when eos has been seen on all inputs.
Descendants may override to get this notification."""
        pass

    def _output(self, record, links=None):
        """Internal method for outputing a record conditional on output func.
        links is a list of outputs to output on; defaults to the specical value
        None, meaning all of them."""
        if links is None: links = range(len(self.outputFunctions))
        for l in links:
            if self.outputFunctions[l]:
                self.outputFunctions[l](record)

    @staticmethod
    def link(outputNode, inputNode, outputLink=0, inputLink=0):
        assert outputNode._validate_link(outputLink, False)
        assert inputNode._validate_link(inputLink, True)
        outputNode.outputFunctions += \
            [None,] * max(0,outputLink - len(outputNode.outputFunctions) + 1)
        outputNode.outputEos += \
            [None,] * max(0,outputLink - len(outputNode.outputEos) + 1)
        inputNode.eosSeen += \
            [None,] * max(0,inputLink - len(inputNode.eosSeen) + 1)
        outputNode.outputFunctions[outputLink] = \
            lambda record: inputNode.input(record, inputLink=inputLink)
        outputNode.outputEos[outputLink] = \
            lambda: inputNode.eos(inputLink=inputLink)
        inputNode.eosSeen[inputLink] = False

# Utility dataflow classes
class StreamDFN(DataflowNode):
    """Easy class for binding together a single list of data flow nodes."""
    def __init__(self):
        DataflowNode.__init__(self)
        self.start = None
        self.end = None

    def prepend(self, node):
        if self.start:
            DataflowNode.link(node, self.start)
            self.start = node
        else:
            self.start = self.end = node
        
    def append(self, node):
        if self.end:
            DataflowNode.link(self.end, node)
            self.end = node
        else:
            self.start = self.end = node

    def _validate_link(self, linknum, input_p):
        return linknum == 0     # One input, one output

    def input(self, record, inputLink=0):
        assert inputLink == 0
        if self.start:
            self.start.input(record)
        else:
            self._output(record)

    def _localEos(self):
        if self.start:
            self.start.eos()

class SplitDFN(DataflowNode):
    """Split the input into as many outputs as are linked."""
    def __init__(self):
        DataflowNode.__init__(self)

    def _validate_link(self, linknum, input_p):
        return linknum == 0 or not input_p     # One input, any num outputs

    def input(self, record, inputLink=0):
        self._output(record)

    def addOutput(self, downstreamNode, outputLink=0):
        DataflowNode.link(self, downstreamNode, outputLink,
                          self._firstOpenOutput())

class SinkDFN(DataflowNode):
    """Accepts input and dumps it to a specified function."""
    def __init__(self, sinkFunc=None, eosFunc=None):
        DataflowNode.__init__(self)
        self.sinkFunc = sinkFunc
        self.eosFunc = eosFunc

    def _validate_link(self, linknum, input_p):
        return input_p     	# Any input, no outputs

    def input(self, record, inputLink=0):
        if self.sinkFunc: self.sinkFunc(record)

    def _localEos(self):
        if self.eosFunc: self.eosFunc()

class RecordIntervalDFN(DataflowNode):
    """Only transmit a specified interval of records from input to output."""
    def __init__(self, interval):
        """Only transmit records whose record number falls in the given
        interval from input to output.  -1 for the end of the interval means
        no limit."""
        DataflowNode.__init__(self)
        assert isinstance(interval[0], int) and isinstance(interval[1], int)
        self.interval = interval
        self.recordNum = 0

    def _validate_link(self, linknum, input_p):
        return linknum == 0     # One input, one output

    def input(self, record, inputLink=0):
        if (self.recordNum >= self.interval[0]
            and (self.interval[1] == -1 or self.recordNum < self.interval[1])):
            self._output(record)
        self.recordNum += 1

class ByteIntervalDFN(DataflowNode):
    """Only transmit a specified byte interval (where input/output is in text strings)."""
    def __init__(self, interval):
        """Only transmit bytes whose position in the stream falls in the given
        interval from input to output.  -1 for the end of the interval means
        no limit."""
        DataflowNode.__init__(self)
        self.interval = interval
        self.byteNum = 0

    def _validate_link(self, linknum, input_p):
        return linknum == 0     # One input, one output

    def input(self, record, inputLink=0):
        strlen = len(record)
        startInStr = max(0, self.interval[0] - self.byteNum)
        endInStr = min(strlen, self.interval[1] - self.byteNum) if self.interval[1] != -1 else strlen
        self.byteNum += len(record)
        if startInStr > strlen or endInStr <= startInStr:
            return
        self._output(record[startInStr:endInStr])

class BatchRecordDFN(DataflowNode):
    """Pass on records input->output in batches.  A batchsize of 0 means to
    wait until end of stream."""
    def __init__(self, batchsize):
        DataflowNode.__init__(self)
        self.batchsize = batchsize
        self.recordlist = []

    def _validate_link(self, linknum, input_p):
        return linknum == 0     # One input, one output

    def _push(self):
        self._output(self.recordlist)
        self.recordlist = []

    def input(self, record, inputLink=0):
        self.recordlist += (record,)
        if self.batchsize and len(self.recordlist) >= self.batchsize:
            self._push()

    def _localEos(self):
        if self.recordlist: self._push()

