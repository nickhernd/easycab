FROM python:3.9

WORKDIR /Regestry
COPY . /Regestry/

COPY requirements.txt /Regestry/
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "registry.py"]