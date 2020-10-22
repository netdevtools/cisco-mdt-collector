FROM python:3.9

RUN pip install poetry
COPY cisco_mdt_collector /usr/local/share/cisco-mdt-collector/cisco_mdt_collector
COPY pyproject.toml /usr/local/share/cisco-mdt-collector/
WORKDIR /usr/local/share/cisco-mdt-collector

RUN poetry config virtualenvs.create false && poetry install
ENTRYPOINT ["cisco-mdt-collector"]
CMD ["--help"]
