build:
	docker build -t cmd2s3 .
	docker run --rm cmd2s3 cat /go/bin/cmd2s3 > cmd2s3
	chmod u+x cmd2s3

clean:
	rm cmd2s3

.PHONY: clean
