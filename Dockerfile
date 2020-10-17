FROM python:3.9

ENV HTTPS_PROXY=$var_name
#RUN mkdir -p /usr/local/share/cisco-mdt-collector/
COPY requirements.txt /usr/local/share/cisco-mdt-collector/
RUN HTTPS_PROXY=http://10.10.20.50:3128 pip install -r /usr/local/share/cisco-mdt-collector/requirements.txt

COPY cisco-mdt-collector /usr/local/share/cisco-mdt-collector

ENTRYPOINT ["python", "/usr/local/share/cisco-mdt-collector/main.py"]
CMD ["--help"]