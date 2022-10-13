# https://hub.docker.com/_/python
FROM python:3.7-slim

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./
# Install production dependencies.
RUN set -ex; \
    pip install -r requirements.txt
RUN pip install Flask gunicorn
ENV GOOGLE_APPLICATION_CREDENTIALS=arunachala-hcc-030919.json
ENV TZ=Asia/Kolkata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
CMD exec gunicorn --bind :8989 --workers 1 --threads 8 --timeout 0 --log-level=info App:app
 
#CMD exec python App.py
