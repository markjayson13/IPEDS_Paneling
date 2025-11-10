# Manifest Baselines

This directory should contain a committed snapshot of trusted IPEDS manifests
under `manifests/baseline/`. The CI drift gate scrapes the latest manifests
into `manifests/current/` and compares them against the baseline to catch
upstream changes before panelization. Commit your baseline manifests (one
`*_manifest.csv` per year) before enabling the workflow.
