FROM python:3.9

WORKDIR /usr/src/etl

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

ARG DB_HOST

CMD [ "python3", "./main.py" ]