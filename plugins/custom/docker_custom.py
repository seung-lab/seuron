import os
import shutil
import json
from time import sleep
from docker import APIClient as Client
import docker
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
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
    online_cpus = d["cpu_stats"].get("online_cpus", len(d["cpu_stats"]["cpu_usage"]["percpu_usage"]))
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
        idle_count = 0
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

            if cpu_percent < 5:
                idle_count += 1
            else:
                idle_count = 0

            if self.qos and idle_count > 5:
                self.log.info('Nothing happened in 5 minutes, stop the container')
                self.cli.stop(self.container['Id'], timeout=1)

            sleep(60)

    # This needs to be updated whenever we update to a new version of airflow!
    def execute(self, context):
        self.log.info('Starting docker container from image %s', self.image)

        tls_config = self._DockerOperator__get_tls_config()

        if self.docker_conn_id:
            self.cli = self.get_hook().get_conn()
        else:
            self.cli = Client(
                base_url=self.docker_url,
                version=self.api_version,
                tls=tls_config
            )

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

        cpu_shares = int(round(self.cpus * 1024))

        with TemporaryDirectory(prefix='airflowtmp') as host_tmp_dir:
            self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
            self.volumes.append('{0}:{1}'.format(host_tmp_dir, self.tmp_dir))

            host_args = {
                'binds': self.volumes,
                'cpu_shares': cpu_shares,
                'mem_limit': self.mem_limit,
                'network_mode': self.network_mode
            }
            host_args.update(self.host_args)

            container_args = {
                'command': self.get_command(),
                'environment': self.environment,
                'host_config': self.cli.create_host_config(**host_args),
                'image': image,
                'user': self.user,
                'working_dir': self.working_dir
            }

            container_args.update(self.container_args)

            self.container = self.cli.create_container(**container_args)

            self.cli.start(self.container['Id'])

            log_reader = threading.Thread(
                target=self._read_task_stats,
            )

            log_reader.daemon = True
            log_reader.start()

            line = ''
            for line in self.cli.logs(
                    container=self.container['Id'], stream=True):
                line = line.strip()
                if hasattr(line, 'decode'):
                    line = line.decode('utf-8')
                self.log.info(line)

            exit_code = self.cli.wait(self.container['Id'])['StatusCode']
            if exit_code != 0:
                raise AirflowException('docker container failed')

            if self.xcom_push_flag:
                return self.cli.logs(
                    container=self.container['Id']) if self.xcom_all else str(
                        line)


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
        self.mount_point = mount_point
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
            self.volumes.append('{0}:{1}'.format(tmp_var_dir,
                                                 self.mount_point))
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
