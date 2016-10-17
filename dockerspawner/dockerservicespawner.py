"""
A Spawner for JupyterHub that runs each user's server in a separate docker
service. The original "container" variable names are kept for convenience,
but should be taken to mean "service"
"""

from textwrap import dedent
from pprint import pformat

import docker
from docker.errors import APIError
from tornado import gen

from dockerspawner import DockerSpawner
from traitlets import Unicode


class DockerServiceSpawner(DockerSpawner):

    network_name = Unicode(
        "",
        config=True,
        help=dedent(
            """
            The name of the docker overlay network for all services. You must
            create this network yourself.
            """
        )
    )

    @gen.coroutine
    def poll(self):
        """Check for task in `docker service ls`"""
        container = yield self.get_container()
        if not container:
            self.log.warn("service not found")
            return ""
        tasks = yield self.docker(
            'tasks', {'service': self.container_id})
        if len(tasks) > 1:
            m = "Service '%s' has %d tasks, expected 1" % (
                self.container_name, len(tasks))
            self.log.error(m)
            raise Exception(m)
        task = tasks[0]
        self.log.debug(
            "Service task %s status: %s",
            self.container_id[:7],
            pformat(task['Status']['State']),
        )

        if task['Status']['State'] == "running":
            return None
        else:
            return (
                "State={Status[State]}, "
                "Message='{Status[Message]}', "
                "UpdatedAt={UpdatedAt}".format(**task)
            )

    @gen.coroutine
    def get_container(self):
        """Get the service, assert the service has one replica
        """
        self.log.debug("Getting service '%s'", self.container_name)
        try:
            service = yield self.docker(
                'inspect_service', self.container_name
            )
            self.container_id = service['ID']
        except APIError as e:
            if e.response.status_code == 404:
                self.log.info(
                    "Service '%s' does not exist", self.container_name)
                service = None
                self.container_id = ''
            else:
                raise
        if service:
            nrep = service['Spec']['Mode']['Replicated']['Replicas']
            m = ''
            if nrep != 1:
                m = "Service '%s' has %d replicas, expected 1" % (
                    self.container_name, nrep)
                self.log.error(m)
                raise Exception(m)
        return service

    @gen.coroutine
    def start(self, image=None, extra_create_kwargs=None):
        """Start the single-user server in a docker service. You can override
        the default parameters passed to `create_service` through the
        `extra_create_kwargs` dictionary.

        Per-instance `extra_create_kwargs` take precedence over their global
        counterparts.
        """
        if not self.use_internal_ip:
            raise ValueError('use_internal_ip must be True')

        service = yield self.get_container()
        if service is None:
            image = image or self.container_image

            mounts = [docker.types.Mount(
                source=k, target=v['bind'], type='bind',
                read_only=(v['mode'] == 'ro'))
                for (k, v) in self.volume_binds.items()]

            # build the dictionary of keyword arguments for create_service
            create_kwargs = dict(
                image=image,
                env=['%s=%s' % kv for kv in self.get_env().items()],
                mounts=mounts)
            create_kwargs.update(self.extra_create_kwargs)
            if extra_create_kwargs:
                create_kwargs.update(extra_create_kwargs)

            contspec = docker.types.ContainerSpec(**create_kwargs)
            template = docker.types.TaskTemplate(contspec)
            self.log.debug("Starting service [%s] with config: %s",
                           self.container_name, template)

            # create the service
            resp = yield self.docker(
                'create_service', template, name=self.container_name,
                networks=[{'Target': self.network_name}])
            self.container_id = resp['ID']
            self.log.info(
                "Created service '%s' (id: %s) from image %s",
                self.container_name, self.container_id, image)
        else:
            self.log.info(
                "Found existing service '%s' (id: %s)",
                self.container_name, self.container_id)

        ip, port = yield self.get_ip_and_port()
        # store on user for pre-jupyterhub-0.7:
        self.user.server.ip = ip
        self.user.server.port = port
        # jupyterhub 0.7 prefers returning ip, port:
        return (ip, port)

    @gen.coroutine
    def get_ip_and_port(self):
        """Queries Docker daemon for service's IP on the overlay network
        Only works with use_internal_ip=True, auto port-forwarding is not
        supported.
        """
        # Docker swarm allows lookups by service-name, there's no need to
        # get the IP
        ip = self.container_name
        port = self.container_port
        return ip, port

    @gen.coroutine
    def stop(self, now=False):
        """Stop the service
        """
        self.log.info(
            "Stopping service %s (id: %s)",
            self.container_name, self.container_id[:7])
        yield self.docker('remove_service', self.container_name)

        # docker service automatically removes containers
        self.clear_state()
