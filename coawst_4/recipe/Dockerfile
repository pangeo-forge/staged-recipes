FROM pangeo/pangeo-notebook
USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install -y tzdata
USER ${NB_USER}
ENV PATH=/srv/conda/envs/notebook/bin:$PATH
ENV JUPYTERHUB_USER=pangeoforge

