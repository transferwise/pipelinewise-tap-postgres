FROM docker.io/bitnami/postgresql:12

# Install wal2json
USER root
RUN install_packages curl ca-certificates gnupg && \
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null && \
    echo "deb http://apt.postgresql.org/pub/repos/apt buster-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    install_packages postgresql-12-wal2json && \
    cp /usr/lib/postgresql/12/lib/wal2json.so /opt/bitnami/postgresql/lib/wal2json.so
USER 1001
