          for file in *.yaml; do
              # Skip kustomization.yaml file
              if [ "$file" == "kustomization.yaml" ]; then
                  continue
              fi

              # Get filename without extension
              # Loop over all files in the directory
              # Remove everything after the first dot in the file name
              FILE_NAME="${file%%.*}"
              echo "$FILE_NAME"

              # Run kubectl commands
              kubectl rollout status -f "$file"
              kubectl get services -o wide -n "$FILE_NAME-service"
          done

