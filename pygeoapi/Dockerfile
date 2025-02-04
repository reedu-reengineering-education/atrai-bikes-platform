FROM geopython/pygeoapi:latest

ENV PYGEOAPI_CONFIG=config.yml
ENV WSGI_WORKER_TIMEOUT=289067349086745908673459

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
        curl \
        git \
        gdal-bin\
        vim \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /osm_toolbox
RUN git clone https://github.com/52North/OpenSenseMapToolbox.git .
RUN pip install .

WORKDIR /pygeoapi

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
#RUN cp ids /osm_toolbox/ids
#RUN python3 /osm_toolbox/main.py --file /osm_toolbox/ids


RUN pip install . \
    && rm -rf ./process

RUN chmod +x /pygeoapi/entrypoint.sh

#RUN mkdir -p /pygeoapi/config

ENTRYPOINT ["/pygeoapi/entrypoint.sh", "run"]
