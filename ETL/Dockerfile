FROM python:3.9

WORKDIR /usr/src/etl

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3","./backfilling.py","&&","python3", "./main.py" ]