#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# This script generates the secrets for the k8s cluster

# It will read all key value pairs from the .env file in the root of this project, create
# a base64 encoded secret for each key value pair and output it in k8s yaml format.

# To run this script, run:

# ./deploy/k8s/secrets/gen_secrets.sh | kubectl apply -f -

cat <<'EOB'
apiVersion: v1
kind: Secret
metadata:
  name: teranode-secrets
type: Opaque
data:
EOB

commented="^[[:space:]]*#.*$"

while IFS= read -r secret || [[ -n "$secret" ]]; do
  # Skip lines starting with '#'
  if [[ $secret =~ $commented ]]; then
    continue
  fi
  
  KEY=$(echo $secret | cut -d '=' -f 1)
  VALUE=$(echo $secret | cut -d '=' -f 2 | base64)
  echo "$KEY: $VALUE"

done < "$DIR/../.env"  | sed 's/^/  /'
