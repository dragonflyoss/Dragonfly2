/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storage

// Task contains content for task.
type Task struct {
	// ID is task id.
	ID string `csv:"id"`

	// URL is task download url.
	URL string `csv:"url"`

	// Type is task type.
	Type string `csv:"type"`

	// ContentLength is task total content length.
	ContentLength int64 `csv:"contentLength"`

	// TotalPieceCount is total piece count.
	TotalPieceCount int32 `csv:"totalPieceCount"`

	// BackToSourceLimit is back-to-source limit.
	BackToSourceLimit int32 `csv:"backToSourceLimit"`

	// BackToSourcePeerCount is back-to-source peer count.
	BackToSourcePeerCount int32 `csv:"backToSourcePeerCount"`

	// State is the download state of the task.
	State string `csv:"state"`

	// CreatedAt is peer create nanosecond time.
	CreatedAt int64 `csv:"createdAt"`

	// UpdatedAt is peer update nanosecond time.
	UpdatedAt int64 `csv:"updatedAt"`
}

// Host contains content for host.
type Host struct {
	// ID is host id.
	ID string `csv:"id"`

	// Type is host type.
	Type string `csv:"type"`

	// Hostname is host name.
	Hostname string `csv:"hostname"`

	// IP is host ip.
	IP string `csv:"ip"`

	// Port is grpc service port.
	Port int32 `csv:"port"`

	// DownloadPort is piece downloading port.
	DownloadPort int32 `csv:"downloadPort"`

	// Host OS.
	OS string `csv:"os"`

	// Host platform.
	Platform string `csv:"platform"`

	// Host platform family.
	PlatformFamily string `csv:"platformFamily"`

	// Host platform version.
	PlatformVersion string `csv:"platformVersion"`

	// Host kernel version.
	KernelVersion string `csv:"kernelVersion"`

	// ConcurrentUploadLimit is concurrent upload limit count.
	ConcurrentUploadLimit int32 `csv:"concurrentUploadLimit"`

	// ConcurrentUploadCount is concurrent upload count.
	ConcurrentUploadCount int32 `csv:"concurrentUploadCount"`

	// UploadCount is total upload count.
	UploadCount int64 `csv:"uploadCount"`

	// UploadFailedCount is upload failed count.
	UploadFailedCount int64 `csv:"uploadFailedCount"`

	// CPU Stat.
	CPU CPU `csv:"cpu"`

	// Memory Stat.
	Memory Memory `csv:"memory"`

	// Network Stat.
	Network Network `csv:"network"`

	// Disk Stat.
	Disk Disk `csv:"disk"`

	// Build information.
	Build Build `csv:"build"`

	// CreatedAt is peer create nanosecond time.
	CreatedAt int64 `csv:"createdAt"`

	// UpdatedAt is peer update nanosecond time.
	UpdatedAt int64 `csv:"updatedAt"`
}

// CPU contains content for cpu.
type CPU struct {
	// Number of logical cores in the system.
	LogicalCount uint32 `csv:"logicalCount"`

	// Number of physical cores in the system.
	PhysicalCount uint32 `csv:"physicalCount"`

	// Percent calculates the percentage of cpu used.
	Percent float64 `csv:"percent"`

	// Calculates the percentage of cpu used by process.
	ProcessPercent float64 `csv:"processPercent"`

	// Times contains the amounts of time the CPU has spent performing different kinds of work.
	Times CPUTimes `csv:"times"`
}

// CPUTimes contains content for cpu times.
type CPUTimes struct {
	// CPU time of user.
	User float64 `csv:"user"`

	// CPU time of system.
	System float64 `csv:"system"`

	// CPU time of idle.
	Idle float64 `csv:"idle"`

	// CPU time of nice.
	Nice float64 `csv:"nice"`

	// CPU time of iowait.
	Iowait float64 `csv:"iowait"`

	// CPU time of irq.
	Irq float64 `csv:"irq"`

	// CPU time of softirq.
	Softirq float64 `csv:"softirq"`

	// CPU time of steal.
	Steal float64 `csv:"steal"`

	// CPU time of guest.
	Guest float64 `csv:"guest"`

	// CPU time of guest nice.
	GuestNice float64 `csv:"guestNice"`
}

// Memory contains content for memory.
type Memory struct {
	// Total amount of RAM on this system.
	Total uint64 `csv:"total"`

	// RAM available for programs to allocate.
	Available uint64 `csv:"available"`

	// RAM used by programs.
	Used uint64 `csv:"used"`

	// Percentage of RAM used by programs.
	UsedPercent float64 `csv:"usedPercent"`

	// Calculates the percentage of memory used by process.
	ProcessUsedPercent float64 `csv:"processUsedPercent"`

	// This is the kernel's notion of free memory.
	Free uint64 `csv:"free"`
}

// Network contains content for network.
type Network struct {
	// Return count of tcp connections opened and status is ESTABLISHED.
	TCPConnectionCount uint32 `csv:"tcpConnectionCount"`

	// Return count of upload tcp connections opened and status is ESTABLISHED.
	UploadTCPConnectionCount uint32 `csv:"uploadTCPConnectionCount"`

	// Security domain for network.
	SecurityDomain string `csv:"securityDomain"`

	// Location path(area|country|province|city|...).
	Location string `csv:"location"`

	// IDC where the peer host is located
	IDC string `csv:"idc"`

	// Network topology(switch|router|...).
	NetTopology string `csv:"netTopology"`
}

// Build contains content for build.
type Build struct {
	// Git version.
	GitVersion string `csv:"gitVersion"`

	// Git commit.
	GitCommit string `csv:"gitCommit"`

	// Golang version.
	GoVersion string `csv:"goVersion"`

	// Build platform.
	Platform string `csv:"platform"`
}

// Disk contains content for disk.
type Disk struct {
	// Total amount of disk on the data path of dragonfly.
	Total uint64 `csv:"total"`

	// Free amount of disk on the data path of dragonfly.
	Free uint64 `csv:"free"`

	// Used amount of disk on the data path of dragonfly.
	Used uint64 `csv:"used"`

	// Used percent of disk on the data path of dragonfly directory.
	UsedPercent float64 `csv:"usedPercent"`

	// Total amount of indoes on the data path of dragonfly directory.
	InodesTotal uint64 `csv:"inodesTotal"`

	// Used amount of indoes on the data path of dragonfly directory.
	InodesUsed uint64 `csv:"inodesUsed"`

	// Free amount of indoes on the data path of dragonfly directory.
	InodesFree uint64 `csv:"inodesFree"`

	// Used percent of indoes on the data path of dragonfly directory.
	InodesUsedPercent float64 `csv:"inodesUsedPercent"`
}

// Parent contains content for parent.
type Parent struct {
	// ID is peer id.
	ID string `csv:"id"`

	// Tag is peer tag.
	Tag string `csv:"tag"`

	// Application is peer application.
	Application string `csv:"application"`

	// State is the download state of the peer.
	State string `csv:"state"`

	// Cost is the task download time(millisecond).
	Cost uint32 `csv:"cost"`

	// Host is peer host.
	Host Host `csv:"host"`

	// CreatedAt is peer create nanosecond time.
	CreatedAt int64 `csv:"createdAt"`

	// UpdatedAt is peer update nanosecond time.
	UpdatedAt int64 `csv:"updatedAt"`
}

// Record contains content for record.
type Record struct {
	// ID is peer id.
	ID string `csv:"id"`

	// Tag is peer tag.
	Tag string `csv:"tag"`

	// Application is peer application.
	Application string `csv:"application"`

	// State is the download state of the peer.
	State string `csv:"state"`

	// Cost is the task download time(millisecond).
	Cost uint32 `csv:"cost"`

	// Task is peer task.
	Task Task `csv:"task"`

	// Host is peer host.
	Host Host `csv:"host"`

	// Parents is peer parents.
	Parents []Parent `csv:"parents" csv[]:"20"`

	// CreatedAt is peer create nanosecond time.
	CreatedAt int64 `csv:"createdAt"`

	// UpdatedAt is peer update nanosecond time.
	UpdatedAt int64 `csv:"updatedAt"`
}
