// Copyright 2016 The ksched Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"nickren/firmament-go/pkg/scheduling/algorithms/mcmf"
	"nickren/firmament-go/pkg/scheduling/algorithms/utils"
	"os"
	"os/exec"
	"time"

	"nickren/firmament-go/pkg/scheduling/dimacs"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	"nickren/firmament-go/pkg/scheduling/flowmanager"
)

var (
	FlowlesslyBinary    = "/usr/local/bin/flowlessly/flow_scheduler"
	FlowlesslyAlgorithm = "successive_shortest_path"
	Incremental         = true
)

type Solver interface {
	Solve() flowmanager.TaskMapping
	MCMFSolve(graph *flowgraph.Graph) flowmanager.TaskMapping
	WriteGraph(file string)
}

type flowlesslySolver struct {
	isSolverStarted bool
	gm              flowmanager.GraphManager
	toSolver        io.Writer
	toConsole       io.Writer
	fromSolver      io.Reader
}

// Returns new solver initialized with the graph manager
func NewSolver(gm flowmanager.GraphManager) Solver {
	// TODO: Do the fields toSolver and fromSolver need to be initialized?
	return &flowlesslySolver{
		gm:              gm,
		isSolverStarted: false,
	}
}


// NOTE: assume we don't have debug flag
// NOTE: assume we only do incremental flow
// Note: assume Solve() is called iteratively and sequentially without concurrency.
func (fs *flowlesslySolver) MCMFSolve(graph *flowgraph.Graph) flowmanager.TaskMapping {
	fs.WriteGraph("mcmf_before")
	start := time.Now()
	copyGraph := flowgraph.ModifyGraphFromTotalToIncremental(graph)
	elapsed := time.Since(start)
	fmt.Printf("copy graph took %s\n", elapsed)
	start = time.Now()
	maxFlow, minCost := mcmf.SuccessiveShortestPathWithDijkstra(copyGraph, copyGraph.SourceID, copyGraph.SinkID)
	elapsed = time.Since(start)
	fmt.Printf("mcmf took %s\n", elapsed)
	fmt.Printf("maxFlow %v, minCost %v\n", maxFlow, minCost)

	tm := make(map[flowgraph.NodeID]flowgraph.NodeID)
	start = time.Now()
	scheduleResult := utils.ExtractScheduleResult(copyGraph, copyGraph.SourceID)
	elapsed = time.Since(start)
	fmt.Printf("extract result took %s\n", elapsed)
	var totalFlow uint64 = 0
	for mapping, flow := range scheduleResult {
		if flow != 0 {
			totalFlow += flow
		} else {
			if mapping.ResourceId == 0 {
			}
		}
	}
	fmt.Printf("before the repair, total flow is %v\n", totalFlow)
	start = time.Now()
	scheduleResult, repairCount := utils.GreedyRepairFlow(copyGraph, scheduleResult, copyGraph.SinkID)
	elapsed = time.Since(start)
	fmt.Printf("greedy repair took %s\n", elapsed)
	fmt.Printf("After the greedy repair, %v tasks got repaired\n", repairCount)
	totalFlow = 0
	for mapping, flow := range scheduleResult {
		if flow != 0 {
			totalFlow += flow
			tm[copyGraph.CopyIdToOriginalIdMap[mapping.TaskId]] = copyGraph.CopyIdToOriginalIdMap[mapping.ResourceId]
		} else {
			if mapping.ResourceId == 0 {
			}
		}
	}
	fmt.Printf("after the repair, total flow is %v, length of tm is %v\n", totalFlow, len(tm))

	utils.ExamCostModel(copyGraph, tm)
	return tm
}

func (fs *flowlesslySolver) Solve() flowmanager.TaskMapping {
	// Note: combine all the first time logic into this once function.
	// This is different from original cpp code.
	if !fs.isSolverStarted {
		fs.isSolverStarted = true

		// Uncomment once we run real sollver.
		fs.startSolver()

		// We must export graph and read from STDOUT/STDERR in parallel
		// Otherwise, the solver might block if STDOUT/STDERR buffer gets full.
		// (For example, if it outputs lots of warnings on STDERR.)

		// go fs.writeGraph()
		fs.WriteGraph("")

		// remove it.. once we run real sollver.
		//os.Exit(1)

		tm := fs.readTaskMapping()
		// fmt.Printf("TaskMappings:%v\n", tm)
		// Exporter should have already finished writing because reading goroutine
		// have also finished.
		return tm
	}

	fs.gm.UpdateAllCostsToUnscheduledAggs()
	fs.writeIncremental()
	tm := fs.readTaskMapping()
	return tm
}

func (fs *flowlesslySolver) startSolver() {
	binaryStr, args := fs.getBinConfig()

	var err error
	cmd := exec.Command(binaryStr, args...)
	fs.toSolver, err = cmd.StdinPipe()
	if err != nil {
		panic(err)
	}
	fs.fromSolver, err = cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}
	fs.toConsole = os.Stdout
	if err := cmd.Start(); err != nil {
		panic(err)
	}
}

func (fs *flowlesslySolver) WriteGraph(file string) {
	// TODO: make sure proper locking on graph, manager
	outputFile, _ := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	defer outputFile.Close()
	fs.toSolver = outputFile
	dimacs.Export(fs.gm.GraphChangeManager().Graph(), fs.toSolver)
	//dimacs.Export(fs.gm.GraphChangeManager().Graph(), fs.toConsole)
	fs.gm.GraphChangeManager().ResetChanges()
}

func (fs *flowlesslySolver) writeIncremental() {
	// TODO: make sure proper locking on graph, manager
	dimacs.ExportIncremental(fs.gm.GraphChangeManager().GetOptimizedGraphChanges(), fs.toSolver)
	//dimacs.ExportIncremental(fs.gm.GraphChangeManager().GetOptimizedGraphChanges(), fs.toConsole)
	fs.gm.GraphChangeManager().ResetChanges()
}

func (fs *flowlesslySolver) readTaskMapping() flowmanager.TaskMapping {
	// TODO: make sure proper locking on graph, manager
	extractedFlow := fs.readFlowGraph()
	return fs.parseFlowToMapping(extractedFlow)
}

// readFlowGraph returns a map of dst to a list of its corresponding src and flow capacity.
func (fs *flowlesslySolver) readFlowGraph() map[flowgraph.NodeID]flowPairMap {
	// The dstToSrcAndFlow map stores the flow pairs responsible for sending flow into the dst node
	// As a multimap it is keyed by the dst node where the flow is being sent.
	// The value is a map of flowpairs showing where all the flows to this dst are coming from
	dstToSrcAndFlow := make(map[flowgraph.NodeID]flowPairMap)
	scanner := bufio.NewScanner(fs.fromSolver)
	for scanner.Scan() {
		line := scanner.Text()
		//fmt.Printf("Line Read:%s\n", line)
		switch line[0] {
		case 'f':
			var src, dst, flowCap uint64
			var discard string
			n, err := fmt.Sscanf(line, "%s %d %d %d", &discard, &src, &dst, &flowCap)
			if err != nil {
				panic(err)
			}
			if n != 4 {
				panic("expected reading 4 items")
			}

			// fmt.Printf("discard:%s src:%d dst:%d flowCap:%d\n", discard, src, dst, flowCap)

			if flowCap > 0 {
				pair := &flowPair{flowgraph.NodeID(src), flowCap}
				// If a flow map for this dst does not exist, then make one
				if dstToSrcAndFlow[flowgraph.NodeID(dst)] == nil {
					dstToSrcAndFlow[flowgraph.NodeID(dst)] = make(flowPairMap)
				}
				dstToSrcAndFlow[flowgraph.NodeID(dst)][pair.srcNodeID] = pair
			}
		case 'c':
			if line == "c EOI" {
				// fmt.Printf("Adj List:%v\n", dstToSrcAndFlow)
				return dstToSrcAndFlow
			} else if line == "c ALGORITHM TIME" {
				// Ignore. This is metrics of runtime.
			}
		case 's':
			// we don't care about cost
		default:
			panic("unknown: " + line)
		}
	}
	panic("wrong state")
}

// Maps worker|root tasks to leaves. It expects a extracted_flow containing
// only the arcs with positive flow (i.e. what ReadFlowGraph returns).
func (fs *flowlesslySolver) parseFlowToMapping(extractedFlow map[flowgraph.NodeID]flowPairMap) flowmanager.TaskMapping {
	// fmt.Printf("Extracted Flow:%v\n", extractedFlow)

	taskToPU := flowmanager.TaskMapping{}
	// Note: recording a node's PUs so that a node can assign the PUs to its source itself
	puIDs := make(map[flowgraph.NodeID][]flowgraph.NodeID)
	visited := make(map[flowgraph.NodeID]bool)
	toVisit := make([]flowgraph.NodeID, 0) // fifo queue
	leafIDs := fs.gm.LeafNodeIDs()
	sink := fs.gm.SinkNode()

	for leafID := range leafIDs {
		visited[leafID] = true
		// Get the flowPairMap for the sink
		flowPairMap, ok := extractedFlow[sink.ID]
		if !ok {
			continue
		}
		// Check if the current leaf contributes a flow pair
		flowPair, ok := flowPairMap[leafID]
		if !ok {
			continue
		}

		for i := uint64(0); i < flowPair.flow; i++ {
			puIDs[leafID] = append(puIDs[leafID], leafID)
		}
		toVisit = append(toVisit, leafID)
	}

	// a variant of breath-frist search
	for len(toVisit) != 0 {
		nodeID := toVisit[0]
		toVisit = toVisit[1:]
		visited[nodeID] = true

		if fs.gm.GraphChangeManager().Graph().Node(nodeID).IsTaskNode() {
			// fmt.Printf("Task Node found\n")
			// record the task mapping between task node and PU.
			if len(puIDs[nodeID]) != 1 {
				log.Panicf("Task Node to Resource Node should be 1:1 mapping")
			}
			taskToPU[nodeID] = puIDs[nodeID][0]
			continue
		}

		toVisit = addPUToSourceNodes(extractedFlow, puIDs, nodeID, visited, toVisit)
	}

	return taskToPU
}

func addPUToSourceNodes(extractedFlow map[flowgraph.NodeID]flowPairMap, puIDs map[flowgraph.NodeID][]flowgraph.NodeID, nodeID flowgraph.NodeID, visited map[flowgraph.NodeID]bool, toVisit []flowgraph.NodeID) []flowgraph.NodeID {
	iter := 0
	srcFlowsMap, ok := extractedFlow[nodeID]
	if !ok {
		return toVisit
	}
	// search each source and assign all its downstream PUs to them.
	for _, srcFlowPair := range srcFlowsMap {
		// TODO: CHange this logic for map instead of slice
		// Populate the PUs vector at the source of the arc with as many PU
		// entries from the incoming set of PU IDs as there's flow on the arc.
		for ; srcFlowPair.flow > 0; srcFlowPair.flow-- {
			if iter == len(puIDs[nodeID]) {
				break
			}
			// It's an incoming arc with flow on it.
			// Add the PU to the PUs vector of the source node.
			puIDs[srcFlowPair.srcNodeID] = append(puIDs[srcFlowPair.srcNodeID], puIDs[nodeID][iter])
			iter++
		}
		if !visited[srcFlowPair.srcNodeID] {
			toVisit = append(toVisit, srcFlowPair.srcNodeID)
			visited[srcFlowPair.srcNodeID] = true
		}

		if iter == len(puIDs[nodeID]) {
			// No more PUs left to assign
			break
		}
	}
	return toVisit
}

// TODO: We can definitely make it cleaner. But currently we just copy the code.
func (fs *flowlesslySolver) getBinConfig() (string, []string) {
	args := []string{
		"--graph_has_node_types=true",
		fmt.Sprintf("--algorithm=%s", FlowlesslyAlgorithm),
		"--print_assignments=false",
		"--debug_output=true",
		"--graph_has_node_types=true",
	}
	if !Incremental {
		args = append(args, "--daemon=false")
	}

	return FlowlesslyBinary, args
}
