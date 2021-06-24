// +build darwin

package config

var config = Config{
	Server: &ServerConfig{
		GRPC: &TCPListenConfig{
			PortRange: TCPListenPortRange{
				Start: 65003,
				End:   65003,
			},
		},
		REST: &RestConfig{
			Addr: ":8080",
		},
	},
}
