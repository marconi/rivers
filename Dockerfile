FROM google/golang
ADD . /gopath/src/github.com/marconi/rivers
RUN go get -t github.com/marconi/rivers
WORKDIR /gopath/src/github.com/marconi/rivers
