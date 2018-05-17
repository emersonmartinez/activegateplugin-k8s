import sys
import os
import json
import logging
import time
import requests
import ruxit.api.exceptions
from ruxit.api.base_plugin import RemoteBasePlugin
from requests.exceptions import ReadTimeout
from urllib.parse import urlparse
from collections import defaultdict, namedtuple

logger = logging.getLogger(__name__)


#class RemoteKubernetesPlugin():
class RemoteKubernetesPlugin(RemoteBasePlugin):
    def initialize(self, **kwargs):
        self.args = {}

        self.args['id'] = kwargs['config']['id']
        self.args['url'] = kwargs['config']['url']
        self.args['token'] = kwargs['config']['token']
        self.args['metrics'] = self.initialize_metrics(kwargs['json_config']['metrics'])

        if 'debug' in kwargs:
            self.args['debug'] = True
        else:
            self.args['debug'] = False

        return

    @staticmethod
    def initialize_metrics(json_config_metrics):
        results = []

        for metric in json_config_metrics:
            result = {'entity': 'CUSTOM_DEVICE', 'key': metric['timeseries']['key'],
                      'dimensions': metric['timeseries']['dimensions'], 'type': metric['source']['type'],
                      'relative': metric['source']['relative']}

            results.append(result)

        return results

    def query(self, **kwargs):
        try:
            print("--- Begin execution ---")

            start_time = time.time()

            data = {'cluster': self.query_cluster(),
                    'nodes': self.query_nodes(),
                    'services': self.query_services(),
                    'pods': self.query_pods()}

            self.report_topology(data)

            end_time = time.time()

            print("--- Statistics ---")
            print("--- Execution time: %s seconds ---" % (end_time - start_time))
        except Exception as exc:
            print("Exception: " + str(exc))

        return

    def report_topology(self, data):
        group = self.report_topology_group(data['cluster']['cluster_id'], data['cluster']['cluster_name'])

        reported_group_metrics = []

        for node in data["nodes"]:
            element = self.report_topology_element(group, node['node_id'], node['node_name'] + str(" (") + str(node['node_role']) + str(")"))

            if not self.args['debug']:
                element.report_property('node_instance_type', node['node_instance_type'])
                element.report_property('node_hostname', node['node_hostname'])
                element.report_property('node_creation_timestamp', node['node_creation_timestamp'])
                element.report_property('node_info_machine_id', node['node_info_machine_id'])
                element.report_property('node_info_system_uuid', node['node_info_system_uuid'])
                element.report_property('node_info_boot_id', node['node_info_boot_id'])
                element.report_property('node_info_kernel_version', node['node_info_kernel_version'])
                element.report_property('node_info_os_image', node['node_info_os_image'])
                element.report_property('node_info_container_runtime_version', node['node_info_container_runtime_version'])
                element.report_property('node_info_kubelet_version', node['node_info_kubelet_version'])
                element.report_property('node_info_kube_proxy_version', node['node_info_kube_proxy_version'])
                element.report_property('node_info_operating_system', node['node_info_operating_system'])
                element.report_property('node_info_architecture', node['node_info_architecture'])

            for pod in data["pods"]:
                reported_element_metrics = []

                for metric in pod['pod_metrics']:
                    if self.is_reportable(data, metric, node['node_name'], pod['pod_name']):
                        self.report_topology_metric(element, metric)
                        reported_element_metrics.append(metric)
                        reported_group_metrics.append(metric)

                self.report_topology_custom_element_metrics(element, reported_element_metrics)

        self.report_topology_custom_group_metrics(group, reported_group_metrics)

        return

    def report_topology_group(self, id, name):
        if self.args['debug']:
            print("Create topology group: " + str(id) + " " + str(name))
            return name

        topology_group = self.topology_builder.create_group(id, name)

        return topology_group

    def report_topology_element(self, group, id, name):
        if self.args['debug']:
            print("Create topology +-- element: " + " " + str(group) + " " + str(id) + " " + str(name))
            return name

        topology_element = group.create_element(id, name)

        return topology_element

    def report_topology_metrics(self, element, metrics):
        for metric in metrics:
            self.report_topology_metric(element, metric)

        return

    def report_topology_metric(self, element, metric):
        if self.args['debug']:
            print("Create topology +---- metric: " + " " + str(metric))
            return

        if metric['relative']:
            element.relative(key=metric['key'], value=metric['value'], dimensions=metric['dimensions'])
        else:
            element.absolute(key=metric['key'], value=metric['value'], dimensions=metric['dimensions'])

        return

    def is_reportable(self, data, metric, reporting_node_name, reporting_pod_name):
        if len(metric['dimensions']) > 0:
            for dimension in metric['dimensions']:
                if dimension == 'pod':
                    pod_name = metric['dimensions'][dimension]
                    for pod in data['pods']:
                        if pod['pod_name'] == pod_name:
                            if pod['pod_node_name'] == reporting_node_name:
                                return True

                if dimension == 'deployment':
                    return True
        else:
            for pod in data['pods']:
                if pod['pod_name'] == reporting_pod_name:
                    if pod['pod_node_name'] == reporting_node_name:
                        return True

        return False

    @staticmethod
    def exists_metric(metrics, key):
        for metric in metrics:
            if metric['key'] == key:
                return True

        return False

    def report_topology_custom_element_metrics(self, element, metrics):
        if self.exists_metric(metrics, "kube_pod_container_status_ready"):
            custom_pods_ready = {'entity': 'CUSTOM_DEVICE', 'key': "custom_pods_ready", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}
            custom_pods_not_ready = {'entity': 'CUSTOM_DEVICE', 'key': "custom_pods_not_ready", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}
            custom_pods_total = {'entity': 'CUSTOM_DEVICE', 'key': "custom_pods_total", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}

            for metric in metrics:
                if metric['key'] == "kube_pod_container_status_ready":
                    custom_pods_total['value'] = int(custom_pods_total['value']) + 1
                    if int(metric['value']) == 1:
                        custom_pods_ready['value'] = int(custom_pods_ready['value']) + 1
                    else:
                        custom_pods_not_ready['value'] = int(custom_pods_not_ready['value']) + 1

            self.report_topology_metric(element, custom_pods_ready)
            self.report_topology_metric(element, custom_pods_not_ready)
            self.report_topology_metric(element, custom_pods_total)

        if self.exists_metric(metrics, "kube_pod_container_status_ready"):
            custom_deployments_available = {'entity': 'CUSTOM_DEVICE', 'key': "custom_deployments_available", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}

            for metric in metrics:
                if metric['key'] == "kube_deployment_status_replicas_available":
                    if int(metric['value']) == 1:
                        custom_deployments_available['value'] = int(custom_deployments_available['value']) + 1

            self.report_topology_metric(element, custom_deployments_available)

        if self.exists_metric(metrics, "kube_pod_container_status_ready"):
            custom_deployments_unavailable = {'entity': 'CUSTOM_DEVICE', 'key': "custom_deployments_unavailable", 'type': "KubernetesStats", 'relative': False, 'dimensions': {}, 'value': 0}

            for metric in metrics:
                if metric['key'] == "kube_deployment_status_replicas_unavailable":
                    if int(metric['value']) == 1:
                        custom_deployments_unavailable['value'] = int(custom_deployments_unavailable['value']) + 1

            self.report_topology_metric(element, custom_deployments_unavailable)

        return

    def report_topology_custom_group_metrics(self, group, metrics):
        return

    def query_cluster(self):
        cluster = {}

        cluster['cluster_id'] = str(self.args['id'])
        cluster['cluster_url'] = str(self.args['url'])
        cluster['cluster_name'] = str(self.args['id']) + " (" + str(self.args['url']) + ")"

        return cluster

    def query_nodes(self):
        nodes = []

        url = str(self.args['url']) + "/api/v1/nodes"
        content = self.query_url(url)
        json_data = json.loads(content)

        for item in json_data['items']:
            self_link = item['metadata']['selfLink']
            nodes.append(self.query_node(self_link))

        return nodes

    def query_node(self, self_link):
        node = {}

        url = str(self.args['url']) + str(self_link)
        content = self.query_url(url)
        json_data = json.loads(content)

        node['node_id'] = json_data['metadata']['uid']
        node['node_name'] = json_data['metadata']['name']

        try:
            node['node_role'] = json_data['metadata']['labels']['kubernetes.io/role']
        except Exception as exc:
            print("Exception: " + str(exc))
            node['node_role'] = "node"

        node['node_instance_type'] = json_data['metadata']['labels']['beta.kubernetes.io/instance-type']
        node['node_hostname'] = json_data['metadata']['labels']['kubernetes.io/hostname']
        node['node_creation_timestamp'] = json_data['metadata']['creationTimestamp']
        node['node_info_machine_id'] = json_data['status']['nodeInfo']['machineID']
        node['node_info_system_uuid'] = json_data['status']['nodeInfo']['systemUUID']
        node['node_info_boot_id'] = json_data['status']['nodeInfo']['bootID']
        node['node_info_kernel_version'] = json_data['status']['nodeInfo']['kernelVersion']
        node['node_info_os_image'] = json_data['status']['nodeInfo']['osImage']
        node['node_info_container_runtime_version'] = json_data['status']['nodeInfo']['containerRuntimeVersion']
        node['node_info_kubelet_version'] = json_data['status']['nodeInfo']['kubeletVersion']
        node['node_info_kube_proxy_version'] = json_data['status']['nodeInfo']['kubeProxyVersion']
        node['node_info_operating_system'] = json_data['status']['nodeInfo']['operatingSystem']
        node['node_info_architecture'] = json_data['status']['nodeInfo']['architecture']

        return node

    def query_services(self):
        services = []

        url = str(self.args['url']) + "/api/v1/services"
        content = self.query_url(url)
        json_data = json.loads(content)

        for item in json_data['items']:
            self_link = item['metadata']['selfLink']
            services.append(self.query_service(self_link))

        return services

    def query_service(self, self_link):
        service = {}

        url = str(self.args['url']) + str(self_link)
        content = self.query_url(url)
        json_data = json.loads(content)

        service['service_id'] = json_data['metadata']['uid']
        service['service_self_link'] = json_data['metadata']['selfLink']

        return service

    def query_pods(self):
        pods = []

        url = str(self.args['url']) + "/api/v1/pods"
        content = self.query_url(url)
        json_data = json.loads(content)

        for item in json_data['items']:
            self_link = item['metadata']['selfLink']
            pods.append(self.query_pod(self_link))

        return pods

    def query_pod(self, self_link):
        pod = {}

        url = str(self.args['url']) + str(self_link)
        content = self.query_url(url)
        json_data = json.loads(content)

        pod['pod_id'] = json_data['metadata']['uid']
        pod['pod_name'] = json_data['metadata']['name']
        pod['pod_self_link'] = json_data['metadata']['selfLink']

        try:
            pod['pod_node_name'] = json_data['spec']['nodeName']
        except Exception as exc:
            print("Exception: " + str(exc))
            pod['pod_node_name'] = None

        pod['pod_metrics_endpoint'] = str(self.args['url']) + json_data['metadata']['selfLink'] + str('/proxy/metrics')

        pod['pod_metrics'] = self.query_metrics(pod['pod_metrics_endpoint'])

        return pod

    def query_metrics(self, metrics_endpoint):
        try:
            results = []

            content = self.query_url(metrics_endpoint)
            lines = content.split('\n')

            if not lines[0].startswith('#'):
                return []

            parsed_metrics = self.parse(lines)

            for parsed_metric in parsed_metrics:
                for metric in self.args['metrics']:
                    if parsed_metric['key'] == metric['key']:
                        result = {}

                        result['entity'] = 'CUSTOM_DEVICE'
                        result['key'] = metric['key']
                        result['type'] = metric['type']
                        result['relative'] = metric['relative']

                        result['value'] = parsed_metric['value']

                        dimensions = {}
                        for dimension in parsed_metric['dimensions']:
                            dimensions[dimension['key']] = dimension['value']

                        result['dimensions'] = dimensions

                        results.append(result)

        except Exception as exc:
            print("Exception: " + str(exc))
            results = []

        return results

    def query_url(self, url):
        if self.args['debug']:
            print(url)

        content = None

        max_retries = 2
        retries = 0
        while retries < max_retries:
            retries = retries + 1
            try:
                r = requests.get(url, verify=False, headers={"Authorization": "Bearer " + self.args['token']}, timeout=2)
                content = r.content.decode('UTF-8')

                if self.args['debug']:
                    print(content)

                return content
            except Exception as exc:
                print("Exception: " + str(exc))
                continue

        return content

    def parse(self, lines):
        metrics = []

        for line in lines:
            if line.startswith("#") or line == '':
                continue

            metric = {'key': self.parse_key(line),
                      'dimensions': self.parse_dimensions(line),
                      'value': self.parse_value(line)}

            metrics.append(metric)

        return metrics

    def parse_key(self, line):
        return str(line.split(' ')[0].split('{')[0])

    def parse_value(self, line):
        return str(line.split(' ')[1])

    def parse_dimensions(self, line):
        dimensions = []

        if len(line.split(' ')[0].split('{')) >= 2:
            dims_temp = str(str(line.split(' ')[0].split('{')[1])).replace('{', '').replace('}', '').replace('"', '').split(',')

            for dim_temp in dims_temp:
                if len(dim_temp.split('=')) >= 2:
                    dim_key = dim_temp.split('=')[0]
                    dim_value = dim_temp.split('=')[1]

                    dimension = {}
                    dimension['key'] = dim_key
                    dimension['value'] = dim_value

                    dimensions.append(dimension)

        return dimensions


#class Test:
#    @staticmethod
#    def test1():
#        id = "k8s_1"
#        url = "https://api.k8s.dev.dynatracelabs.com"
#        token = "[BEARER TOKEN]"
#        file = open("plugin.json")
#        json_config = json.load(file)
#        plugin = RemoteKubernetesPlugin()
#        plugin.initialize(config={"id": id, "url": url, "token": token}, json_config=json_config, debug=True)
#        plugin.query()
#
#
#if __name__ == "__main__":
#    Test.test1()
