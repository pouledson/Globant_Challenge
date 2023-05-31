FROM ubuntu

RUN apt update
RUN apt install python3-pip -y

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt


WORKDIR /globant_challenge

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]

