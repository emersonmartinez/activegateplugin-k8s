# Dynatrace Kubernetes ActiveGate Plugin

Maintainer: dominik.sachsenhofer@dynatrace.com ([Twitter](https://twitter.com/sachsenhofer))

This is the home of Dynatrace Kubernetes ActiveGate Plugin. This plugin can be used to monitor a Kubernetes Cluster and import metrics from Prometheus endpoints. __It is released as a Developer Preview. It is intended to provide early-stage insights into new features until the Dynatrace Kubernetes Integration/Dashboard becomes available.__

<br>
<br>

## Disclaimer

__The Dynatrace Kubernetes ActiveGate Plugin and the ActiveGate Plugin technology are currently in EAP.__

__Early Access releases provide early-stage insight into new features and functionality of the Dynatrace Platform. They enable you to provide feedback that can significantly impact our direction and implementation.__

__While Early Access releases aren't ready to be used to build production solutions, they're at a stage where you can test and tinker with an implementation. As we receive feedback and iterate on a project, we anticipate breaking changes without advanced warning, so Early Access releases should not be used in a user-facing manner or applied to production environments.__

<br>
<br>


## Overview

The Dynatrace Kubernetes ActiveGate Plugin is a remote based plugin that runs on the Dynatrace ActiveGate.
The plugin systematically requests the Kubernetes API server to get information about nodes, services, deployments and pods on the Kubernetes cluster. In addition, it scrapes Prometheus endpoints to integrate cluster metrics into Dynatrace.

![Img1](./assets/img1.png)

<br>
<br>

## 1 Usage

__Requirements__:

- Kubernetes 1.8+

- Dynatrace tenant (1.145+)

- Dynatrace feature flag (com.compuware.apm.webuiff.enable remote plugins monitoring.irm.feature [enable remote plugins monitoring]) must be enabled

- ActiveGate Server (Operating System): Windows

- ActiveGate Server (Memory): at least 2 GB

<br>

## 1.1 Create Dynatrace Access

__1.1.1 Create ServiceAccount, ClusterRole, ClusterRoleBinding:__

Create the following resources on your Kubernetes cluster:

```
kubectl create -f https://raw.githubusercontent.com/dynatrace-innovationlab/activegateplugin-k8s/master/deploy/activegateplugin-service-account.yaml
kubectl create -f https://raw.githubusercontent.com/dynatrace-innovationlab/activegateplugin-k8s/master/deploy/activegateplugin-cluster-role.yaml
kubectl create -f https://raw.githubusercontent.com/dynatrace-innovationlab/activegateplugin-k8s/master/deploy/activegateplugin-cluster-role-binding.yaml
```

Done!

<br>
<br>

## 1.2 Install Dynatrace ActiveGate Server

__1.2.1 Download:__

In __Dynatrace UI__, go to __Deploy Dynatrace - Start Installation - Install Dynatrace Security Gateway - Windows - Download securitygateway.exe__

<br>

__1.2.2 Install:__

Install the ActiveGate Server on a Windows host using the following install flag:

```
C:\Users\Administrator> Dynatrace-Security-Gateway-Windows-1.143.76.exe REMOTE_PLUGIN_SHOULD_INSTALL="true"
```

Then follow the steps in the installer.

Done!

<br>
<br>

## 1.3 Deploy Dynatrace ActiveGate Plugin on the ActiveGate Server

__1.3.1 Upload plugin to ActiveGate Server:__

On your ActiveGate server, upload the __unzipped plugin__ folder to the __plugin_deployment directory__:

__C:\Program Files\dynatrace\gateway\components\plugin_deployment\activegateplugin-k8s__

<br>

__1.3.2 Restart Dynatrace Remote Plugin Agent:__

On your __ActiveGate server__, go to __Server Manager - Services__, search for __Dynatrace Remote Plugin Agent__ and restart the service.

Done!

<br>
<br>

## 1.4 Deploy Dynatrace ActiveGate Plugin on Dynatrace

__1.4.1 Get secret:__

Execute the following command to get the name of the secret:

```
$ kubectl describe serviceaccount dynatrace -n kube-system
Name:                dynatrace
Namespace:           default
Labels:              <none>
Annotations:         <none>
Image pull secrets:  <none>
Mountable secrets:   dynatrace-token-s4ttd
Tokens:              dynatrace-token-s4ttd
Events:              <none>
```

<br>

__1.4.2 Get token:__

Execute the following command to get the token.

```
$ kubectl describe secret dynatrace-token-s4ttd -n kube-system
Name:         dynatrace-token-s4ttd
Namespace:    kube-system
Labels:       <none>
Annotations:  kubernetes.io/service-account.name=dynatrace
              kubernetes.io/service-account.uid=919966d2-28f9-11e8-b142-02ea51c0cda0

Type:  kubernetes.io/service-account-token

Data
====
ca.crt:     1042 bytes
namespace:  11 bytes
token:      eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybwV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJteW5hdHJhY2UtdG9rZW4teGp0ODIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpYxUtYWNjb3VudC5uYW1lIjoiZHluYXRyYWNlIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQudWlkIjoiNzg0ZWUzMDgtMzk3MS0xMWU4LWI0NzYtMxIzN2M4OWFkYzA4Iiwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50Omt1YmUtc3lzdGVtOmR5bmF0cmFjZSJ9.XEsjaIAR2nAKJL-apRdkzAOwfBzDqX3O9VpMZ1Tq7FPLZ4Fp-cQEAYezT-MYNN-USpPSAF20fjPYxVqI_-u2Ey7fuJsg_dLTISN7znSbPwfRTJxyH2zUOjmNQiM5zP08XV2G8gcn0mNs5ae7SRSeU1JGH9GGdnFQ_y7R5IL4HtnZv_KKT1cCWbwV1bGJNfYlBfyQGnmsHyBrjJMuaNtFpGzQvgekMAoWaDaFCNdHxNgYj5cymjoz1faSkC9RxUmpnR27yFEb_1eZ-u3Csb8yke6o6vSqMW3YY7HxGJAo-BK-utS_fIMs6XOPkq0pHx5TremXB7GyNt6KhGAaXW4t6A
```

<br>

__1.4.3 Upload plugin to Dynatrace:__

In __Dynatrace UI__, go to __Settings - Monitored technologies - Custom plugins - Upload ActiveGate plugin__

Then upload __zipped plugin__ folder to Dynatrace.

<br>

__1.4.4 Configure plugin:__

```
- Endpoint: [ENDPOINT]
- ID: [ID]
- URL: [URL]
- Bearer Token: [TOKEN]
```  

Example values:

```
ENDPOINT=Endpoint1 (custom name)
ID=k8s_cluster_1 (custom name)
URL=https://api.k8s.dev.dynatracelabs.com:8080 (URL to the Kubernetes API-Server)
TOKEN=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybwV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJteW5hdHJhY2UtdG9rZW4teGp0ODIiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpYxUtYWNjb3VudC5uYW1lIjoiZHluYXRyYWNlIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQudWlkIjoiNzg0ZWUzMDgtMzk3MS0xMWU4LWI0NzYtMxIzN2M4OWFkYzA4Iiwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50Omt1YmUtc3lzdGVtOmR5bmF0cmFjZSJ9.XEsjaIAR2nAKJL-UpRdkzAOwfBzDqX3O9VpMZ1Tq7FPLZ4Fp-cQEAYezT-MYNN-USpPSAF20fjPYxVqI_-u2Ey7fuJsg_dLTISN7znSbPwfRTJxyH2zaOjmNQiM5zP08XV2G8gcn0mNs5ae7SRSeU1JGH9GGdnFQ_y7R5IL4HtnZv_KKT1cCWbwV1bGJNfYlBfyQGnmsHyBrjJMuaNtFpGzQvgekMAoWaDaFCNdHxNgYj5cymjoz1faSkC9RxUmpnR27yFEb_1eZ-u3Csb8yke6o6vSqMW3YY7HxGJAo-BK-utS_fIMs6XOPkq0pHx5TremXB7GyNt6KhGAaXW4t6A (Bearer Token)
```

Done!
<br>
<br>

## 1.5 Install Prometheus-Operator

In order to get useful metrics, you have to install the Prometheus-Operator (kube-prometheus) in the Kubernetes cluster. You can use Helm charts to install Prometheus. Execute the following commands in your terminal:

__1.5.1 Install Helm/Tiller:__

See https://github.com/kubernetes/helm#install

<br>

__1.5.2 Create a ClusterRoleBinding and initialize Helm/Tiller:__

```
cat <<EOF | kubectl create -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: tiller
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1beta1
kind: ClusterRoleBinding
metadata:
  name: tiller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: tiller
    namespace: kube-system
EOF

helm init --service-account tiller
```

<br>

__1.6.3 Install Prometheus-Operator:__

Then install the Prometheus-Operator:

```
helm repo add coreos https://s3-eu-west-1.amazonaws.com/coreos-charts/stable/

helm install coreos/prometheus-operator --name prometheus-operator --namespace monitoring
helm install coreos/kube-prometheus --name kube-prometheus --set global.rbacEnable=true --namespace monitoring
```

Done!

<br>

## Screenshots

![Img2](./assets/img2.png)
![Img3](./assets/img3.png)
![Img4](./assets/img4.png)
![Img5](./assets/img5.png)
![Img6](./assets/img6.png)

<br>

## Troubleshooting


<br>

__Problem 1:__

If you use a Google KubernetesEngine Cluster and you run into this issue:

```
Error from server (Forbidden): error when creating "STDIN": clusterroles.rbac.authorization.k8s.io "dynatrace" is forbidden: attempt to grant extra privileges: [PolicyRule{Resources:["nodes"], APIGroups:[""], Verbs:["get"]} PolicyRule{Resources:["nodes"], APIGroups:[""], Verbs:["list"]} PolicyRule{Resources:["nodes"], APIGroups:[""], Verbs:["watch"]} PolicyRule{Resources:["services"], APIGroups:[""], Verbs:["get"]} PolicyRule{Resources:["services"], APIGroups:[""], Verbs:["list"]} PolicyRule{Resources:["services"], APIGroups:[""], Verbs:["watch"]} PolicyRule{Resources:["endpoints"], APIGroups:[""], Verbs:["get"]} PolicyRule{Resources:["endpoints"], APIGroups:[""], Verbs:["list"]} PolicyRule{Resources:["endpoints"], APIGroups:[""], Verbs:["watch"]} PolicyRule{Resources:["pods"], APIGroups:[""], Verbs:["get"]} PolicyRule{Resources:["pods"], APIGroups:[""], Verbs:["list"]} PolicyRule{Resources:["pods"], APIGroups:[""], Verbs:["watch"]} PolicyRule{Resources:["configmaps"], APIGroups:[""], Verbs:["get"]} PolicyRule{NonResourceURLs:["/metrics"], Verbs:["get"]}] user=&{dominik.sachsenhofer@gmail.com  [system:authenticated] map[authenticator:[GKE]]} ownerrules=[PolicyRule{Resources:["selfsubjectaccessreviews"], APIGroups:["authorization.k8s.io"], Verbs:["create"]} PolicyRule{NonResourceURLs:["/api" "/api/*" "/apis" "/apis/*" "/healthz" "/swagger-2.0.0.pb-v1" "/swagger.json" "/swaggerapi" "/swaggerapi/*" "/version"], Verbs:["get"]}] ruleResolutionErrors=[]
```

Then you need to do the following additional steps first:

Get current google identity:

```
$ gcloud info | grep Account
Account: [EMAIL]
```

Grant cluster-admin to your current identity

```
$ kubectl create clusterrolebinding [NAME]-cluster-admin-binding --clusterrole=cluster-admin --user=[EMAIL]
Clusterrolebinding "[NAME]-cluster-admin-binding" created
```

Example values:

```
[NAME]=dominik
[EMAIL]=dominik.sachsenhofer@dynatrace.com
```

## Limitations

Limitations:

- The Dynatrace ActiveGateway must be installed on a __Windows host__. This is a requirement of the ActiveGate Plugin technology. Linux support is coming soon.

<br>

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details on submitting changes.

<br>

## License

Dynatrace Kubernetes ActiveGate Plugin is under Apache 2.0 license. See [LICENSE](LICENSE) for details.

