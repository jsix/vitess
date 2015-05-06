// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// Task implementations can be executed by the scheduler.
// Tasks can emit new tasks by returning them. The new tasks will be added
// to the execution graph directly after this task.
type Task interface {
	run(parameters map[string]string) (newTaskContainers []*pb.TaskContainer, output string, err error)
}
