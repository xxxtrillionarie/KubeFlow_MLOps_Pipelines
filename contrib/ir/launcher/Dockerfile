FROM python:3.7
COPY requirements.txt /kfp/launcher/
RUN python3 -m pip install -r /kfp/launcher/requirements.txt

COPY src/* /kfp/launcher/
CMD python3 -u /kfp/launcher/launcher.py
