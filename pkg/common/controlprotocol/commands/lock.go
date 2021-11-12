/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package commands

import "time"

// CommandLock provides common meta-data for adding/removing locks before/after commands.
type CommandLock struct {
	Token       string        `json:"token"`       // A Token Identifying The Owner Of The Lock
	Timeout     time.Duration `json:"timeout"`     // Custom Lock Timeout, If 0 Then Default Timeout May Apply
	LockBefore  bool          `json:"lockBefore"`  // Optionally Lock The Resource Before The Command
	UnlockAfter bool          `json:"unlockAfter"` // Optionally Unlock The Resource After The Command
}

// NewCommandLock is a convenience constructor for the CommandLock struct.
func NewCommandLock(token string, timeout time.Duration, lockBefore bool, unlockAfter bool) *CommandLock {
	return &CommandLock{
		Token:       token,
		Timeout:     timeout,
		LockBefore:  lockBefore,
		UnlockAfter: unlockAfter,
	}
}
