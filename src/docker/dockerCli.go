package docker

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/goforj/godump"
	"github.com/google/uuid"
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

	//name the container with prefix and shortened uuid
	containerNamePrefix := os.Getenv("M_WORKER_CONTAINER_PREFIX")
	shortenedUUID := uuid.New().String()[0:8]
	containerName := containerNamePrefix + "-" + shortenedUUID

	_, err := c.client.ImagePull(ctx, imageTag, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("could not pull image tag for migration worker %w", err)
	}

	containerConfig := createContainerConfig(imageTag)
	hostConfig := createHostConfig()

	createInfo, err := c.client.ContainerCreate(ctx, createContainerConfig(imageTag), createHostConfig(), &network.NetworkingConfig{}, nil, "containerName") //TODO
	if err != nil {
		return fmt.Errorf("could not create container: %w", err)
	}

	c.Logger.Debug("successfully created docker container for migration worker",
		zap.String("info", godump.DumpStr(createInfo)),
		zap.String("containerConfig", godump.DumpStr(containerConfig)),
		zap.String("hostConfig", godump.DumpStr(hostConfig)),
	)

	err = c.client.ContainerStart(ctx, containerName, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("could not start container: %w", err)
	}

	c.Logger.Debug("successfully started migration worker", zap.String("containerName", containerName))

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
