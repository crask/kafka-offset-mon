package main

type ServerManager struct {
	HttpServers     []*HttpServer
	InfluxdbSyncers []*InfluxdbSyncer
}

func (this *ServerManager) AddHttpServer(server *HttpServer) {
	this.HttpServers = append(this.HttpServers, server)
}

func (this *ServerManager) AddInfluxdbSyncer(server *InfluxdbSyncer) {
	this.InfluxdbSyncers = append(this.InfluxdbSyncers, server)
}

func (this *ServerManager) Init() error {
	for _, server := range this.HttpServers {
		err := server.Init()
		if err != nil {
			return err
		}
	}

	for _, server := range this.InfluxdbSyncers {
		err := server.Init()
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *ServerManager) Start() error {
	for _, server := range this.HttpServers {
		err := server.Start()
		if err != nil {
			return err
		}
	}

	for _, server := range this.InfluxdbSyncers {
		err := server.Start()
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *ServerManager) Close() error {
	for _, server := range this.HttpServers {
		err := server.Close()
		if err != nil {
			return err
		}
	}

	for _, server := range this.InfluxdbSyncers {
		err := server.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
