FROM python:2.7
MAINTAINER Sébastien Brennion LeanBI

RUN pip install boto
ENV TERM=xterm
ADD *.py /opt/

CMD ["python","/opt/transaction_data.py"]