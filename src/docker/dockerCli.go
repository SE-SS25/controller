package docker

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/goforj/godump"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"os"
)

type DInterface struct {
	logger      *zap.Logger
	client      *dockerclient.Client
	workerChan  chan CreateRequest
	mWorkerChan chan CreateRequest
}

type CreateRequest struct {
	ctx          context.Context
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

func (d *DInterface) Ping(ctx context.Context) error {

	_, err := d.client.Ping(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (d *DInterface) Run() {

	for {
		select {
		case req := <-d.workerChan: //accept requests to create workers

			d.logger.Info("received request to start new worker")

			funcRes := make(chan error, 1)
			go func() {
				//TODO
			}()

			//either the context is canceled or we get a result from the create worker func
			select {
			case <-req.ctx.Done():
				req.ResponseChan <- req.ctx.Err()
			case e := <-funcRes:
				req.ResponseChan <- fmt.Errorf("there was an error creating the migration worker: %v", e)
			}

		case req := <-d.mWorkerChan: //accept requests to create migration worker
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
}

func (d *DInterface) SendWorkerRequest(ctx context.Context) CreateRequest {

	respChannel := make(chan error, 1)

	req := CreateRequest{
		ctx:          ctx,
		ResponseChan: respChannel,
	}

	d.workerChan <- req

	return req

}

func (d *DInterface) SendMWorkerRequest(ctx context.Context) CreateRequest {

	respChannel := make(chan error, 1)

	req := CreateRequest{
		ctx:          ctx,
		ResponseChan: respChannel,
	}

	d.mWorkerChan <- req

	return req

}

func (d *DInterface) startMigrationWorker(req CreateRequest) error {

	ctx := req.ctx
	traceID := ctx.Value("traceID")

	imageTag := os.Getenv("M_WORKER_IMAGE_TAG")

	//name the container with prefix and shortened uuid (may have stolen this from hyperfaas)
	containerNamePrefix := os.Getenv("M_WORKER_CONTAINER_PREFIX")
	shortenedUUID := uuid.New().String()[0:8]
	containerName := containerNamePrefix + "-" + shortenedUUID

	//I am assuming here that the image already exists locally and does not have to be pulled

	containerConfig := createContainerConfig(imageTag)
	hostConfig := createHostConfig()

	createInfo, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, &network.NetworkingConfig{}, nil, containerName)
	if err != nil {
		return fmt.Errorf("could not create container: %w", err)
	}

	d.logger.Debug("successfully created docker container for migration worker",
		zap.String("info", godump.DumpStr(createInfo)),
		zap.String("containerConfig", godump.DumpStr(containerConfig)),
		zap.String("hostConfig", godump.DumpStr(hostConfig)),
		zap.Any("traceID", traceID),
	)

	err = d.client.ContainerStart(ctx, containerName, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("could not start container: %w", err)
	}

	d.logger.Debug("successfully started migration worker", zap.String("containerName", containerName))

	return nil

}

func createContainerConfig(imageTag string) *container.Config {
	return &container.Config{
		Image: imageTag,
		ExposedPorts: nat.PortSet{
			"50052/tcp": struct{}{},
		},
		Env: []string{},
		//TODO max backoff, uuid for worker, backoff strategy and initial backoff so we are consistent

	}
}

func createHostConfig() *container.HostConfig {
	return &container.HostConfig{
		AutoRemove:  false, //TODO
		NetworkMode: "matrix-kingdom",
	}
}
