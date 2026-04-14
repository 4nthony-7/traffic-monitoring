FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    streamlit==1.35.0 \
    kafka-python==2.0.2 \
    pandas==2.2.2 \
    altair==5.3.0
COPY app.py .
COPY style.css .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]
