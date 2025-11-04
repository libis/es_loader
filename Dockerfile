#FROM ruby:3.1.4
FROM ruby:3.4.1


# Install gems
ENV APP_HOME="/app"
ENV HOME="/root"

RUN cp /usr/share/zoneinfo/CET /etc/localtime 

RUN mkdir $APP_HOME
WORKDIR $APP_HOME
COPY Gemfile ./
RUN gem install bundler
RUN bundle install

COPY ./src /app/src