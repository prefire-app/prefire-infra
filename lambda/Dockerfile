FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

RUN dnf install -y expat && dnf clean all

RUN pip install "rasterio>=1.3"

COPY api.py /var/task/api.py

CMD ["api.handler"]