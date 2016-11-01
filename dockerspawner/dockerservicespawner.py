"""
A Spawner for JupyterHub that runs each user's server in a separate docker
service. The original "container" variable names are kept for convenience,
but should be taken to mean "service"

https://github.com/jupyterhub/jupyterhub/blob/master/docs/source/spawners.md
"""

import socket
import pwd

from textwrap import dedent
from time import sleep
from pprint import pformat

import docker
from docker.errors import APIError
from tornado import gen

from dockerspawner import DockerSpawner
from traitlets import Int,Integer, Unicode


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

    container_timeout = Int(
        120,
        min=0,
        config=True,
        help=dedent(
            """
            Maximum timeout (seconds) when resolving a container's IP address.
            This must be large enough to allow time for Docker Swarm to create
            a new service/container.
            """
        )
    )

    log_driver = Unicode(
        "",
        config=True,
        help=dedent(
            """
            The log driver. Driver options aren't currently supported.
            """
        )
    )

    host_homedir_format_string = Unicode(
        "/home/{username}",
        config=True,
        help=dedent(
            """
            Format string for the path to the user's home directory on the host.
            The format string should include a `username` variable, which will
            be formatted with the user's username.
            """
        )
    )

    image_homedir_format_string = Unicode(
        "/home/{username}",
        config=True,
        help=dedent(
            """
            Format string for the path to the user's home directory
            inside the image.  The format string should include a
            `username` variable, which will be formatted with the
            user's username.
            """
        )
    )

    user_id = Integer(-1,
                      help=dedent(
                          """
                          If system users are being used, then we need to know their user id
                          in order to mount the home directory.

                          User IDs are looked up in two ways:

                          1. stored in the state dict (authenticator can write here)
                          2. lookup via pwd
                          """
                      )
                      )

    @property
    def host_homedir(self):
        """
        Path to the volume containing the user's home directory on the host.
        """
        return self.host_homedir_format_string.format(username=self.user.name)

    @property
    def homedir(self):
        """
        Path to the user's home directory in the docker image.
        """
        return self.image_homedir_format_string.format(username=self.user.name)

    @gen.coroutine
    def poll(self):
        """Check for task in `docker service ls`"""
        container = yield self.get_container()
        if not container:
            self.log.warn("service not found")
            return ""
        tasks = yield self.docker('tasks', {
            'service': self.container_id, 'desired-state': 'running'})
        if len(tasks) != 1:
            m = "Service '%s' has %d tasks, expected 1" % (
                self.container_name, len(tasks))
            self.log.error(m)
            raise Exception(m)
        task = tasks[0]
        self.log.debug(
            "Service task '%s' (%s) status: %s",
            self.container_name,
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

    def get_env(self):
        env = super(DockerSpawner, self).get_env()
        env.update(dict(
            USER=self.user.name,
            USER_ID=self.user_id,
            HOME=self.homedir
        ))
        return env

    def _user_id_default(self):
        """
        Get user_id from pwd lookup by name

        If the authenticator stores user_id in the user state dict,
        this will never be called, which is necessary if
        the system users are not on the Hub system (i.e. Hub itself is in a container).
        """
        return pwd.getpwnam(self.user.name).pw_uid

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

            template_kwargs = dict()
            if self.log_driver:
                template_kwargs['log_driver'] = docker.types.DriverConfig(
                    self.log_driver)

            contspec = docker.types.ContainerSpec(**create_kwargs)
            template = docker.types.TaskTemplate(
                contspec, **template_kwargs)
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
        t = 0
        port = self.container_port
        while t <= self.container_timeout:
            try:
                # Lookup service IP using Docker swarm DNS
                ip = socket.gethostbyname(self.container_name)
                return ip, port
            except socket.gaierror:
                if t > self.container_timeout:
                    break
                self.log.debug(
                    "Unable to get IP for service '%s' after %d s, retrying",
                    self.container_name, t)
                sleep(2)
                t += 2

        m = "Failed to get IP for Service '%s' after %d s" % (
            self.container_name, self.container_timeout)
        self.log.error(m)
        raise Exception(m)

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
