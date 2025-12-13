# Usage Guide

## Installation

Running Helm Lint...
==> Linting helm/longbow
[INFO] Chart.yaml: icon is recommended
[ERROR] templates/: template: longbow/templates/hpa.yaml:1:14: executing
"longbow/templates/hpa.yaml" at <.Values.autoscaling.enabled>: nil pointer
evaluating interface {}.enabled

replicaCount: 1

image:
  repository: ghcr.io/23skdu/longbow
  pullPolicy: IfNotPresent
  tag: "latest"

imagePullSecrets: []
nameOverride: ""
fullnameOverride: ""

serviceAccount:
  create: true
  annotations: {}
  name: ""

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"
  prometheus.io/path: "/metrics"

podSecurityContext: {}

securityContext: {}

service:
  type: ClusterIP
  port: 3000
  metricsPort: 9090

metrics:
  enabled: true
  port: 9090

resources: {}

nodeSelector: {}

tolerations: []

affinity: {}

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts:
    - host: chart-example.local
      paths:
        - path: /
          pathType: ImplementationSpecific
  tls: []

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts:
    - host: chart-example.local
      paths:
        - path: /
          pathType: ImplementationSpecific
  tls: []

httpRoute:
  enabled: false
  gateway: {}
  hostnames: []
  rules: []
{{- if .Values.httpRoute.enabled -}}
{{- $fullName := include "longbow.fullname" . -}}
{{- $svcPort := .Values.service.port -}}
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: {{ $fullName }}
  labels:
    {{- include "longbow.labels" . | nindent 4 }}
  {{- with .Values.httpRoute.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  parentRefs:
    {{- with .Values.httpRoute.parentRefs }}
      {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .Values.httpRoute.hostnames }}
  hostnames:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  rules:
    {{- range .Values.httpRoute.rules }}
    {{- with .matches }}
    - matches:
      {{- toYaml . | nindent 8 }}
    {{- end }}
    {{- with .filters }}
      filters:
      {{- toYaml . | nindent 8 }}
    {{- end }}
      backendRefs:
        - name: {{ $fullName }}
          port: {{ $svcPort }}
          weight: 1
    {{- end }}
{{- end }}
replicaCount: 1

image:
  repository: ghcr.io/23skdu/longbow
  pullPolicy: IfNotPresent
  tag: "latest"

imagePullSecrets: []
nameOverride: ""
fullnameOverride: ""

serviceAccount:
  create: true
  annotations: {}
  name: ""

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"
  prometheus.io/path: "/metrics"

podSecurityContext: {}

securityContext: {}

service:
  type: ClusterIP
  port: 3000
  metricsPort: 9090

metrics:
  enabled: true
  port: 9090

resources: {}

nodeSelector: {}

tolerations: []

affinity: {}

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts:
    - host: chart-example.local
      paths:
        - path: /
          pathType: ImplementationSpecific
  tls: []

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts:
    - host: chart-example.local
      paths:
        - path: /
          pathType: ImplementationSpecific
  tls: []

httpRoute:
  enabled: false
  gateway: {}
  hostnames: []
  rules: []
--- TEMPLATE ---
{{- if .Values.httpRoute.enabled -}}
{{- $fullName := include "longbow.fullname" . -}}
{{- $svcPort := .Values.service.port -}}
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: {{ $fullName }}
  labels:
    {{- include "longbow.labels" . | nindent 4 }}
  {{- with .Values.httpRoute.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  parentRefs:
    {{- with .Values.httpRoute.parentRefs }}
      {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .Values.httpRoute.hostnames }}
  hostnames:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  rules:
    {{- range .Values.httpRoute.rules }}
    {{- with .matches }}
    - matches:
      {{- toYaml . | nindent 8 }}
    {{- end }}
    {{- with .filters }}
      filters:
      {{- toYaml . | nindent 8 }}
    {{- end }}
      backendRefs:
        - name: {{ $fullName }}
          port: {{ $svcPort }}
          weight: 1
    {{- end }}
{{- end }}
helm/longbow:
charts
Chart.yaml
templates
values.yaml

helm/longbow/charts:

helm/longbow/templates:
deployment.yaml
_helpers.tpl
hpa.yaml
httproute.yaml
ingress.yaml
NOTES.txt
serviceaccount.yaml
service.yaml
tests

helm/longbow/templates/tests:
test-connection.yaml
replicaCount: 1

image:
  repository: ghcr.io/23skdu/longbow
  pullPolicy: IfNotPresent
  tag: "latest"

imagePullSecrets: []
nameOverride: ""
fullnameOverride: ""

serviceAccount:
  create: true
  annotations: {}
  name: ""

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"
  prometheus.io/path: "/metrics"

podSecurityContext: {}

securityContext: {}

service:
  type: ClusterIP
  port: 3000
  metricsPort: 9090

metrics:
  enabled: true
  port: 9090

resources: {}

nodeSelector: {}

tolerations: []

affinity: {}

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts:
    - host: chart-example.local
      paths:
        - path: /
          pathType: ImplementationSpecific
  tls: []

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts:
    - host: chart-example.local
      paths:
        - path: /
          pathType: ImplementationSpecific
  tls: []

httpRoute:
  enabled: false
  gateway: {}
  hostnames: []
  rules: []
==> Linting helm/longbow
[INFO] Chart.yaml: icon is recommended
[ERROR] templates/: template: longbow/templates/service.yaml:14:18: executing
"longbow/templates/service.yaml" at <.Values.metrics.enabled>: nil pointer
evaluating interface {}.enabled

==> Linting helm/longbow
[INFO] Chart.yaml: icon is recommended
[ERROR] templates/: template: longbow/templates/service.yaml:14:18: executing
"longbow/templates/service.yaml" at <.Values.metrics.enabled>: nil pointer
evaluating interface {}.enabled

==> Linting helm/longbow
[INFO] Chart.yaml: icon is recommended
[ERROR] templates/: template: longbow/templates/service.yaml:14:18: executing
"longbow/templates/service.yaml" at <.Values.metrics.enabled>: nil pointer
evaluating interface {}.enabled

Error: 1 chart(s) linted, 1 chart(s) failed
-rw-r--r-- 1 root root 833 Dec 13 07:36 helm/longbow/values.yaml

replicaCount: 1

image:
  repository: ghcr.io/23skdu/longbow
  pullPolicy: IfNotPresent
  tag: "0.0.2"

imagePullSecrets: []
nameOverride: ""
fullnameOverride: ""

serviceAccount:
  create: true
  annotations: {}
  name: ""

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"

podSecurityContext: {}

securityContext: {}

service:
  type: ClusterIP
  port: 3000
  metricsPort: 9090

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts:
    - host: chart-example.local
      paths:
        - path: /
          pathType: ImplementationSpecific
  tls: []

httpRoute:
  enabled: false
  gateway: {}
  hostnames: []
  rules: []

resources: {}

autoscaling:
  enabled: false
  minReplicas: 1
  maxReplicas: 100
  targetCPUUtilizationPercentage: 80

nodeSelector: {}

tolerations: []

affinity: {}
==> Linting helm/longbow
[INFO] Chart.yaml: icon is recommended
[ERROR] templates/: template: longbow/templates/service.yaml:14:18: executing
"longbow/templates/service.yaml" at <.Values.metrics.enabled>: nil pointer
evaluating interface {}.enabled

Running Helm Lint...
==> Linting helm/longbow
[INFO] Chart.yaml: icon is recommended
[ERROR] templates/: template: longbow/templates/service.yaml:14:18: executing
"longbow/templates/service.yaml" at <.Values.metrics.enabled>: nil pointer
evaluating interface {}.enabled

Helm lint failed!

## Client Example (Python)

