FROM docker.io/bitnami/postgresql:12-debian-11

USER root

# Git SHA of v2.4
ENV WAL2JSON_COMMIT_ID=36fbee6cbb7e4bc1bf9ee4f55842ab51393e3ac0

# Compile the plugins from sources and install
RUN install_packages ca-certificates curl gcc git gnupg make pkgconf && \
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null && \
    echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    install_packages postgresql-server-dev-12 && \
    git clone https://github.com/eulerto/wal2json -b master --single-branch && \
    (cd /wal2json && git checkout $WAL2JSON_COMMIT_ID && make && make install) && \
    rm -rf wal2json

USER 1001
