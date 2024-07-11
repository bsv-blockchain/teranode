FROM redhat/ubi8:8.9

# Install necessary packages (Lustre client, etc.)
RUN yum update -y \
    && curl https://fsx-lustre-client-repo-public-keys.s3.amazonaws.com/fsx-rpm-public-key.asc -o /tmp/fsx-rpm-public-key.asc \
    && rpm --import /tmp/fsx-rpm-public-key.asc \
    && sudo curl https://fsx-lustre-client-repo.s3.amazonaws.com/el/8/fsx-lustre-client.repo -o /etc/yum.repos.d/aws-fsx.repo \
    && yum clean all \
    && yum install -y kmod-lustre-client lustre-client \
    && yum clean all

# Copy the script to the container
COPY lustre-cleanup.sh /lustre-cleanup.sh

# Make the script executable
RUN chmod +x /lustre-cleanup.sh

# Set the script as the entrypoint
CMD ["/lustre-cleanup.sh"]