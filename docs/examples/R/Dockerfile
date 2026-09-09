# use official R image
FROM docker.io/rocker/r-ver:4.4.0

# install packages
RUN install2.r --deps TRUE \
  argparser \
  dplyr

# set the working directory to /app
WORKDIR /app

# copy the file to run
COPY r_helloworld.r /app/r_helloworld.r

# default run command when starting container
CMD ["Rscript", "/app/r_helloworld.r"]
