package docker

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/daemon/config"
	"github.com/docker/go-connections/nat"
	"go.uber.org/zap"
	"os"
)

type DInterface struct {
	Logger *zap.Logger
	client *dockerclient.Client
}

func New(logger *zap.Logger) (DInterface, error) {

	clientOpt := dockerclient.WithHost("unix:///var/run/docker.sock")

	client, err := dockerclient.NewClientWithOpts(clientOpt, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return DInterface{}, fmt.Errorf("error creating a new docker client: %w", err)
	}

	dockerInterface := DInterface{
		Logger: logger,
		client: client,
	}

	return dockerInterface, nil
}

func (c *DInterface) StartMigrationWorker(ctx context.Context) error {

	imageTag := os.Getenv("M_WORKER_IMAGE_TAG")
	//TODO what to do with reader output
	reader, err := c.client.ImagePull(ctx, imageTag, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("could not pull image tag for migration worker %w", err)
	}

	//TODO maybe lofg this info somewhere? do i want this component to log?
	createInfo, err := c.client.ContainerCreate(ctx, createContainerConfig(imageTag), createHostConfig(), &network.NetworkingConfig{}, nil, "containerName") //TODO
	if err != nil {
		return fmt.Errorf("could not create container: %w", err)
	}

	err = c.client.ContainerStart(ctx, "containerName", container.StartOptions{})
	if err != nil {
		return fmt.Errorf("could not start container: %w", err)
	}

	return nil

}

func createContainerConfig(imageTag string) *container.Config {
	return &container.Config{
		Image: imageTag,
		ExposedPorts: nat.PortSet{
			"50052/tcp": struct{}{},
		},
		Env: []string{}, //TODO
	}
}

func createHostConfig() *container.HostConfig {
	return &container.HostConfig{
		AutoRemove:  true,
		NetworkMode: "networkName", //TODO
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
				Source: "function-logs",
				Target: "/logs/",
			},
		},
		Resources: container.Resources{}, //TODO
	}
}
