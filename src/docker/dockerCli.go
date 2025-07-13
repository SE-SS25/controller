package docker

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	goutils "github.com/linusgith/goutils/pkg/env_utils"
	"go.uber.org/zap"
)

// DInterface provides an interface to interact with Docker containers for the migration worker.
// It allows for creating migration worker containers.
type DInterface struct {
	logger      *zap.Logger
	client      *dockerclient.Client
	workerChan  chan CreateRequest
	mWorkerChan chan CreateRequest
}

// CreateRequest represents a request to create migration worker.
type CreateRequest struct {
	ctx          context.Context
	workerId     string
	ResponseChan chan error
}

func New(logger *zap.Logger) (DInterface, error) {

	clientOpt := dockerclient.WithHost("unix:///var/run/docker.sock")

	client, err := dockerclient.NewClientWithOpts(clientOpt, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return DInterface{}, fmt.Errorf("error creating a new docker client: %w", err)
	}

	dockerInterface := DInterface{
		logger:      logger,
		client:      client,
		workerChan:  make(chan CreateRequest, 10),
		mWorkerChan: make(chan CreateRequest, 10),
	}

	return dockerInterface, nil
}

// Ping checks if the Docker client is able to communicate with the Docker daemon.
func (d *DInterface) Ping(ctx context.Context) error {

	_, err := d.client.Ping(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Run starts the main loop of the DInterface, which listens for requests to create migration workers.
func (d *DInterface) Run() {

	for {
		req := <-d.mWorkerChan //accept requests to create migration worker
		d.logger.Info("received request to start new migration worker")

		funcRes := make(chan error, 1)
		go func() {
			funcRes <- d.startMigrationWorker(req)
		}()

		//either the context is canceled or we get a result from the create migration worker func
		select {
		case <-req.ctx.Done():
			req.ResponseChan <- req.ctx.Err()
		case e := <-funcRes:
			if e != nil {
				req.ResponseChan <- fmt.Errorf("there was an error creating the migration worker: %v", e)
				continue
			}
			req.ResponseChan <- nil
		}
	}
}

// SendMWorkerRequest sends a request to create a migration worker with a specific worker ID.
func (d *DInterface) SendMWorkerRequest(ctx context.Context, workerId string) CreateRequest {

	respChannel := make(chan error, 1)

	req := CreateRequest{
		ctx:          ctx,
		workerId:     workerId,
		ResponseChan: respChannel,
	}

	d.mWorkerChan <- req

	return req

}

// startMigrationWorker creates and starts a Docker container for the migration worker.
func (d *DInterface) startMigrationWorker(req CreateRequest) error {

	ctx := req.ctx
	traceID := ctx.Value("traceID")

	imageTag := goutils.NoLog().ParseEnvStringPanic("M_WORKER_IMAGE_TAG")

	//name the container with prefix and shortened uuid (may have stolen this from hyperfaas)
	containerNamePrefix := goutils.NoLog().ParseEnvStringPanic("M_WORKER_CONTAINER_PREFIX")
	shortenedUUID := uuid.New().String()[0:8]
	containerName := containerNamePrefix + "-" + shortenedUUID

	//I am assuming here that the image already exists locally and does not have to be pulled

	containerConfig := createContainerConfig(imageTag, req.workerId)
	hostConfig := createHostConfig()

	_, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, &network.NetworkingConfig{}, nil, containerName)
	if err != nil {
		return fmt.Errorf("could not create container: %w", err)
	}

	err = d.client.ContainerStart(ctx, containerName, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("could not start container: %w", err)
	}

	d.logger.Debug("successfully started migration worker", zap.String("containerName", containerName), zap.Any("traceID", traceID))

	return nil

}

// createContainerConfig creates a container configuration for the migration worker.
func createContainerConfig(imageTag string, workerId string) *container.Config {
	return &container.Config{
		Image: imageTag,
		ExposedPorts: nat.PortSet{
			"50052/tcp": struct{}{},
		},
		Env: []string{
			"PG_CONN=" + goutils.NoLog().ParseEnvStringPanic("PG_CONN"),
			"UUID=" + workerId,
			"APP_ENV=" + goutils.NoLog().ParseEnvStringPanic("APP_ENV"),
			"RETRIES=" + "5",
			"HEARTBEAT_BACKOFF=" + "3",
			"BACKOFF_TYPE=" + "exp",
			"INIT_RETRY_BACKOFF=" + "15ms",
			"MAX_BACKOFF=" + "5m",
			"HEARTBEAT_BACKOFF=" + "3s",
		},
	}
}

// createHostConfig creates a host configuration for the migration worker container.
func createHostConfig() *container.HostConfig {
	return &container.HostConfig{
		AutoRemove:  false, //TODO
		NetworkMode: "matrix-kingdom",
	}
}
