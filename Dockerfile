FROM python:3.9.18-bookworm


WORKDIR /usr/app


COPY . /usr/app

RUN pip install --upgrade pip
RUN pip install -r /usr/app/requirementes.txt


CMD ["python", "export_begerman.py"]


#CMD ["python3", "-c", "while True: pass"]
