package mgr

var (
	cdnManager      = createCDNManager()
	taskManager     = createTaskManager()
	hostManager     = createHostManager()
	peerTaskManager = createPeerTaskManager()
)

func GetCDNManager() *CDNManager {
	return cdnManager
}

func GetTaskManager() *TaskManager {
	return taskManager
}

func GetHostManager() *HostManager {
	return hostManager
}

func GetPeerTaskManager() *PeerTaskManager {
	return peerTaskManager
}

