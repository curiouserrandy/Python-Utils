#!/usr/bin/env python

import pstats
import operator
import re
import math

# For reference, pstats code is in
# /Library/Frameworks/Python.framework/Versions/2.6/lib/python2.6/pstats.py

class Cgstats(pstats.Stats):
    """Decorads the pstats.Stats class with routines to provide call
    graph information."""
    def call_graph(self, *restrictions):
        """Return the call graph of the stats object.  If specified, only
        functions matching the restrictions are returned.  What is returned
        is a dictionary.  Keys in the dictionary are tuples of the form
        (caller, callee), where caller and callee each are tuples of
        the form (filename, lineno, rtnname).  Values are tuples of
        the form (ncalls, nprimcalls, total time, cumulative time)."""

        list = self.fcn_list[:] if self.fcn_list else self.stats.keys()
        for r in restrictions:
            list, msg = self.eval_print_amount(r, list, "")

        cgdict = {}
        for f in list:
            cc, nc, tt, ct, callers = self.stats[f]
            for caller in callers.keys():
                if caller in list:
                    cgdict[(caller, f)] = callers[caller]

        return cgdict

    # XXX: Would be nice to include data on each routine in the box
    # XXX: Would be nice to identify methods by class name (AST experience
    # opportunity?)
    def save_dot_callgraph(self, outfile, *restrictions):
        """Outputs the call graph returned by call_graph as a dot file."""
        cg = self.call_graph(*restrictions)
        edges = cg.keys()

        # Get the routines and batch them per file.
        keylist = sorted(reduce(operator.add, zip(*edges)))
        keylist = dict.fromkeys(keylist).keys()
        filelist = [k[0] for k in keylist]
        filelist = dict.fromkeys(filelist).keys()
        filekeylist = {}
        for f in filelist:
            filekeylist[f] = filter(lambda x: x[0] == f, keylist)

        # Normalization for total time
        max_total_time = max([self.stats[k][2] for k in keylist])
        print "Max Total time: ", max_total_time

        # Output the graph
        outf = open(outfile, "w")
        print >> outf, "digraph callgraph {"
        indent = " " * 4
        print >> outf, indent, "node [shape=box];"

        # Output all the subgraphs and nodes
        subgraph_suffix = 0
        node_suffix = 0
        node_name_dict = {}
        for f in filelist:
            if f != "~":        # The built-ins aren't put in a box
                print >> outf, indent, "subgraph cluster_%d {" % subgraph_suffix
                subgraph_suffix += 1
                indent += "    "
                print >> outf, indent, 'label = "%s";' % f
                print >> outf, indent, 'style=filled;'
                print >> outf, indent, "color=lightgrey;"
            for k in filekeylist[f]:
                # A couple of label transformations
                label = k[2]
                label = re.sub(r"\<method '(\w+)' of '(\w+)' objects\>",
                               "<\\2.\\1>", label)
                label = re.sub(r"\<built-in method (\w+)\>",
                               "<<\\1>>", label)
                
                # Figure out it's total/cumtime
                (time) = self.stats[k][2]

                print >> outf, indent, 
                print >> outf, ('node_%d [label="%s\\n tt = %f", style=filled, fillcolor="#FF%02X%02X"];'
                       % (node_suffix, label, time,
                          int((1 - time/max_total_time) * 255),
                          int((1 - time/max_total_time) * 255)))
                node_name_dict[k] = node_suffix
                node_suffix += 1
            if f!= "~":
                indent = indent[:-4]
                print >> outf, indent, "}"
        print >> outf

        # Output all the edges
        for e in edges:
            (from_node, to_node) = e
            callcount = cg[e][0]
            print >> outf, indent, 'node_%d -> node_%d [headlabel="%d"];' % (node_name_dict[from_node], node_name_dict[to_node], callcount)
            
        print >> outf, "};"
        outf.close()

        # XXX: Would be nice to print a readable text representation of the
        # call graph.

    
# XXX Notes from testing:
#	* Color really helps for focussing attention
#	* There's too much in the graph as shown; pycallgraph
# 	  does well at by default leaving out certain things.
#	* Sorting by "cum" and chopping is useful
#	* Notes on total time in each box would be useful
#	* Maybe try sub-records by module?        

if __name__ == "__main__":
    p = Cgstats("/Users/randy/Projects/MailSys/tmp/overhead.prof")
    p.strip_dirs().sort_stats("cum").save_dot_callgraph("/Users/randy/tmp/cg.dot", 50)

## Redirect C-c c to the above

## Local Variables: **
## compile-command: "./cgpstats.py" **
## End: **

