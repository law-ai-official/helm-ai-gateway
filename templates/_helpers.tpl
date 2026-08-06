{{/*
Chart-wide helpers. Kept small on purpose — one place for common labels, one for namespace resolution.
*/}}

{{- define "ai-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ai-gateway.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "ai-gateway.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ai-gateway.namespace" -}}
{{- default "ai-gateway" .Values.global.namespace -}}
{{- end -}}

{{/*
Common labels merged into every resource. Includes user-supplied global.labels last so
operators can override chart-provided values from Argo without editing templates.
*/}}
{{- define "ai-gateway.labels" -}}
app.kubernetes.io/name: {{ include "ai-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- with .Values.global.labels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Per-workload selector labels. Pass the service name in .name.
Usage:  {{ include "ai-gateway.selectorLabels" (dict "root" $ "name" "postgres") }}
*/}}
{{- define "ai-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end -}}
