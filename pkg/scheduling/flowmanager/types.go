package flowmanager

import "nickren/firmament-go/pkg/scheduling/flowgraph"

// TaskMapping is a 1:1 mapping from Task Node to Resource Node
type TaskMapping map[flowgraph.NodeID]flowgraph.NodeID
