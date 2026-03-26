FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

RUN dnf install -y expat && dnf clean all

RUN pip install "rasterio>=1.3" "shapely>=2.0"

COPY api.py /var/task/api.py
COPY county_polygons.json /var/task/county_polygons.json

CMD ["api.handler"]