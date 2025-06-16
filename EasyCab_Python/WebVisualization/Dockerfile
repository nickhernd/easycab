FROM python:3.9

WORKDIR /WebVisualization
COPY . /WebVisualization/

COPY requirements.txt /WebVisualization/
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "web_visualization.py"]