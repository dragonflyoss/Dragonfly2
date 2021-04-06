{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "dragonfly.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "dragonfly.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{- end -}}
{{- end -}}

{{/*
Create a default fully qualified scheduler name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "dragonfly.scheduler.fullname" -}}
{{ template "dragonfly.fullname" . }}-{{ .Values.scheduler.name }}
{{- end -}}

{{/*
Create a default fully qualified cdnsystem name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "dragonfly.cdnsystem.fullname" -}}
{{ template "dragonfly.fullname" . }}-{{ .Values.cdnsystem.name }}
{{- end -}}{

{{/*
Create a default fully qualified client name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "dragonfly.client.fullname" -}}
{{ template "dragonfly.fullname" . }}-{{ .Values.client.name }}
{{- end -}}

