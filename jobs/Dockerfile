# One image for all jobs: Spark + Iceberg JARs + this repo's code.
# Built by hand (README step 8). In production, CI would build it per commit
# and tag it with the commit hash.
FROM apache/spark:4.1.4-scala2.13-java17-python3-ubuntu

ARG GIT_SHA=dev
LABEL org.opencontainers.image.revision=$GIT_SHA \
      org.opencontainers.image.source=https://github.com/rmmbdev/demo-spark-polaris-iceberg-k8s

USER root
# Iceberg runtime + AWS bundle, downloaded into jobs/jars/ (README step 8).
# Baked in: no Maven download at job start, same JARs on every run.
COPY jars/*.jar /opt/spark/jars/
COPY src/ /opt/spark/app/
# Make `import lakehouse` work for any Python process in the image. A .pth
# file in site-packages survives entrypoints that reset PYTHONPATH.
RUN python3 -c "import os, site; d = site.getsitepackages()[0]; \
os.makedirs(d, exist_ok=True); \
open(os.path.join(d, 'lakehouse.pth'), 'w').write('/opt/spark/app\\n')"
ENV PYTHONPATH=/opt/spark/app \
    JOBS_VERSION=$GIT_SHA
# 185 is the image's non-root `spark` user.
USER 185
