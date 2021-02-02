FROM python:3.7-slim-stretch

RUN apt-get update
RUN apt-get install -y redis-server supervisor nginx
RUN pip3 install --upgrade pip
RUN pip3 install gunicorn

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY nginx/default /etc/nginx/sites-available/default
COPY docker-entrypoint.sh /entrypoint.sh

COPY . /app

RUN pip3 install -r ./app/requirements.txt

EXPOSE 9000 9001

ENTRYPOINT ["sh", "/entrypoint.sh"]