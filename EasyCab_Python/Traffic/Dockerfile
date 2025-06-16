FROM python:3.9

WORKDIR /Traffic
COPY . /Traffic/

COPY requirements.txt /Traffic/
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "traffic.py"]]