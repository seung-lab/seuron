import os
import shutil
import json
import socket
from time import sleep
from docker import APIClient as Client
from docker.types import Mount
import docker
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.providers.docker.hooks.docker import DockerHook
from airflow.utils.file import TemporaryDirectory

import threading

def _ensure_unicode(s):
    try:
        return s.decode("utf-8")
    except AttributeError:
        return s


def humanize_bytes(bytesize, precision=2):
    """
    Humanize byte size figures

    https://gist.github.com/moird/3684595
    """
    abbrevs = (
        (1 << 50, 'PB'),
        (1 << 40, 'TB'),
        (1 << 30, 'GB'),
        (1 << 20, 'MB'),
        (1 << 10, 'kB'),
        (1, 'bytes')
    )
    if bytesize == 1:
        return '1 byte'
    for factor, suffix in abbrevs:
        if bytesize >= factor:
            break
    if factor == 1:
        precision = 0
    return '%.*f %s' % (precision, bytesize / float(factor), suffix)


# this is taken directly from docker client:
#   https://github.com/docker/docker/blob/28a7577a029780e4533faf3d057ec9f6c7a10948/api/client/stats.go#L309
def calculate_cpu_percent(d):
    cpu_percent = 0.0
    try:
        cpu_count = len(d["cpu_stats"]["cpu_usage"]["percpu_usage"])
        cpu_delta = float(d["cpu_stats"]["cpu_usage"]["total_usage"]) - \
                    float(d["precpu_stats"]["cpu_usage"]["total_usage"])
        system_delta = float(d["cpu_stats"]["system_cpu_usage"]) - \
                       float(d["precpu_stats"]["system_cpu_usage"])
    except KeyError:
        return cpu_percent

    if system_delta > 0.0:
        cpu_percent = cpu_delta / system_delta * 100.0 * cpu_count
    return cpu_percent


# again taken directly from docker:
#   https://github.com/docker/cli/blob/2bfac7fcdafeafbd2f450abb6d1bb3106e4f3ccb/cli/command/container/stats_helpers.go#L168
# precpu_stats in 1.13+ is completely broken, doesn't contain any values
def calculate_cpu_percent2(d, previous_cpu, previous_system):
    # import json
    # du = json.dumps(d, indent=2)
    # logger.debug("XXX: %s", du)
    cpu_percent = 0.0
    cpu_total = float(d["cpu_stats"]["cpu_usage"]["total_usage"])
    cpu_delta = cpu_total - previous_cpu
    cpu_system = float(d["cpu_stats"]["system_cpu_usage"])
    system_delta = cpu_system - previous_system
    online_cpus = d["cpu_stats"].get("online_cpus", len(d["cpu_stats"]["cpu_usage"].get("percpu_usage", [None])))
    if system_delta > 0.0:
        cpu_percent = (cpu_delta / system_delta) * online_cpus * 100.0
    return cpu_percent, cpu_system, cpu_total

def calculate_blkio_bytes(d):
    """

    :param d:
    :return: (read_bytes, wrote_bytes), ints
    """
    bytes_stats = graceful_chain_get(d, "blkio_stats", "io_service_bytes_recursive")
    if not bytes_stats:
        return 0, 0
    r = 0
    w = 0
    for s in bytes_stats:
        if s["op"] == "Read":
            r += s["value"]
        elif s["op"] == "Write":
            w += s["value"]
    return r, w


def calculate_network_bytes(d):
    """

    :param d:
    :return: (received_bytes, transceived_bytes), ints
    """
    networks = graceful_chain_get(d, "networks")
    if not networks:
        return 0, 0
    r = 0
    t = 0
    for if_name, data in networks.items():
        r += data["rx_bytes"]
        t += data["tx_bytes"]
    return r, t


def graceful_chain_get(d, *args):
    t = d
    for a in args:
        try:
            t = t[a]
        except (KeyError, ValueError, TypeError):
            return None
    return t


class DockerConfigurableOperator(DockerOperator):
    """
    This is modified from https://github.com/apache/incubator-airflow/blob/1.8.2/airflow/operators/docker_operator.py
    with the exception that we are able to inject container and host arguments
    before the container is run.
    """ # noqa
    def __init__(self, container_args=None, host_args=None, qos=True, *args, **kwargs):
        if container_args is None:
            self.container_args = {}
        else:
            self.container_args = container_args

        if host_args is None:
            self.host_args = {}
        else:
            self.host_args = host_args

        self.qos = qos

        super().__init__(*args, **kwargs)

    def _read_task_stats(self):
        cpu_total = 0.0
        cpu_system = 0.0
        cpu_percent = 0.0
        unhealthy_count = 0
        while True:
            try:
                x = self.cli.stats(container=self.container['Id'], decode=False, stream=False)
            except docker.errors.APIError:
                sleep(60)


            blk_read, blk_write = calculate_blkio_bytes(x)
            net_r, net_w = calculate_network_bytes(x)
            try:
                mem_current = x["memory_stats"]["usage"]
                mem_total = x["memory_stats"]["limit"]
            except KeyError:
                mem_current = 0
                mem_total = 1

            try:
                cpu_percent, cpu_system, cpu_total = calculate_cpu_percent2(x, cpu_total, cpu_system)
            except KeyError:
                cpu_percent = calculate_cpu_percent(x)

            self.log.info("cpu: {:.2f}%, mem: {} ({:.2f}%), blk read/write: {}/{}, net rx/tx: {}/{}".
                          format(cpu_percent,
                                 humanize_bytes(mem_current),
                                 (mem_current / mem_total) * 100.0,
                                 humanize_bytes(blk_read),
                                 humanize_bytes(blk_write),
                                 humanize_bytes(net_r),
                                 humanize_bytes(net_w)))

            try:
                info = self.cli.inspect_container(container=self.container['Id'])
                health_status = info['State']['Health']['Status']
            except:
                health_status = 'none'

            if health_status == 'healthy' or health_status == 'starting':
                unhealthy_count = 0
            elif health_status == 'unhealthy':
                unhealthy_count += 1
                self.log.info('Health check Failed')
            else:
                if cpu_percent < 5:
                    unhealthy_count += 1
                    self.log.info('No health status, container is idle, assuming it is unhealthy.')
                else:
                    unhealthy_count = 0

            if self.qos and unhealthy_count > 5:
                self.log.info('Health check failed 5 times, stop the container')
                self.cli.stop(self.container['Id'], timeout=1)

            sleep(60)

    # This needs to be updated whenever we update to a new version of airflow!
    def execute(self, context):
        self.log.info('Starting docker container from image %s', self.image)

        if ':' not in self.image:
            image = self.image + ':latest'
        else:
            image = self.image

        if self.force_pull or len(self.cli.images(name=image)) == 0:
            self.log.info('Pulling docker image %s', image)
            for l in self.cli.pull(image, stream=True):
                output = json.loads(l.decode('utf-8'))
                try:
                    self.log.info("%s", output['status'])
                except KeyError:
                    pass

        network_id = None
        if os.environ.get("VENDOR", None) == "LocalDockerCompose":
            self.log.info("Mount extra volumes specified by docker composer")
            container_id = socket.gethostname()
            info = self.cli.inspect_container(container=container_id)
            mounts = info['Mounts']
            for m in mounts:
                if m['Source'] == '/tmp' or m['Source'] == '/var/run/docker.sock':
                    continue
                self.log.info(f"mount {m['Source']} to {m['Destination']}")
                self.mounts.append(Mount(source=m['Source'], target=m['Destination'], type=m['Type']))
            self.log.info("Try to connect to the network created by docker composer")
            networks = info['NetworkSettings']['Networks']
            for n in networks:
                network_id = networks[n].get('NetworkID', None)
                if network_id:
                    self.log.info(f"use network: {n} ({network_id})")
                    break

        cpu_shares = int(round(self.cpus * 1024))

        with TemporaryDirectory(prefix='airflowtmp') as host_tmp_dir:
            self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
            self.mounts.append(
                Mount(source=host_tmp_dir, target=self.tmp_dir, type='bind')
            )

            host_args = {
                'mounts': self.mounts,
                'cpu_shares': cpu_shares,
                'mem_limit': self.mem_limit,
                'cap_add': ["IPC_LOCK", "SYS_NICE"],
                'network_mode': self.network_mode
            }
            host_args.update(self.host_args)

            container_args = {
                'command': self.format_command(self.command),
                'environment': self.environment,
                'host_config': self.cli.create_host_config(**host_args),
                'image': image,
                'user': self.user,
                'working_dir': self.working_dir
            }

            container_args.update(self.container_args)

            self.container = self.cli.create_container(**container_args)

            self.cli.start(self.container['Id'])

            if network_id:
                self.cli.connect_container_to_network(self.container['Id'], network_id)

            log_reader = threading.Thread(
                target=self._read_task_stats,
            )

            log_reader.daemon = True
            log_reader.start()

            line = ''
            res_lines = []
            for line in self.cli.logs(
                    container=self.container['Id'], stream=True):
                if hasattr(line, 'decode'):
                    line = line.decode('utf-8')
                line = line.strip()
                res_lines.append(line)
                self.log.info(line)

            exit_code = self.cli.wait(self.container['Id'])['StatusCode']
            if exit_code != 0:
                raise AirflowException('docker container failed')

            if self.do_xcom_push:
                return res_lines if self.xcom_all else line


class DockerRemovableContainer(DockerConfigurableOperator):
    """
    This manually removes the container after it has exited.
    This is *NOT* to be confused with docker host config with *auto_remove*
    AutoRemove is done on docker side and will automatically remove the
    container along with its logs automatically before we can display the logs
    and get the exit code!
    """
    def __init__(self,
                 remove=True,
                 *args, **kwargs):
        self.remove = remove
        super().__init__(*args, **kwargs)

    def execute(self, context):
        try:
            return super().execute(context)
        finally:
            if self.cli and self.container and self.remove:
                self.cli.stop(self.container, timeout=1)
                self.cli.remove_container(self.container)


class DockerWithVariablesOperator(DockerRemovableContainer):
    DEFAULT_MOUNT_POINT = '/run/variables'

    def __init__(self,
                 variables,
                 mount_point=DEFAULT_MOUNT_POINT,
                 *args, **kwargs):
        self.variables = variables
        if mount_point:
            self.mount_point = mount_point
        else:
            self.mount_point=self.DEFAULT_MOUNT_POINT,
        super().__init__(*args, **kwargs)

    def execute(self, context):
        with TemporaryDirectory(prefix='dockervariables') as tmp_var_dir:
            try:
                shutil.copy('/tmp/sysinfo.txt', tmp_var_dir)
            except FileNotFoundError:
                self.log.info('No sysinfo file found, skip')
            for key in self.variables:
                value = Variable.get(key)
                with open(os.path.join(tmp_var_dir, key), 'w') as value_file:
                    # import pdb
                    # pdb.set_trace()
                    value_file.write(value)

            if self.variables:
                self.mounts.append(
                    Mount(source=tmp_var_dir, target=self.mount_point, type='bind')
                )
            return super().execute(context)


class CustomPlugin(AirflowPlugin):
    name = "docker_plugin"
    operators = [DockerRemovableContainer, DockerWithVariablesOperator,
                 DockerConfigurableOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
