package mgr

var (
	cdnManager      = CreateCDNManager()
	taskManager     = CreateTaskManager()
	hostManager     = CreateHostManager()
	peerTaskManager = CreatePeerTaskManager()
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
