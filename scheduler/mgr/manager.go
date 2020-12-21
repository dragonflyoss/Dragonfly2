package mgr

var (
	taskManager = CreateTaskManager()
	hostManager = CreateHostManager()
	peerTaskManager = CreatePeerTaskManager()
)

func GetTaskManager() *TaskManager {
	return taskManager
}


func GetHostManager() *HostManager {
	return hostManager
}

func GetPeerTaskManager() *PeerTaskManager {
	return peerTaskManager
}
