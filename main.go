package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	args := os.Args[1:]
	if len(args) < 2 {
		log.Fatal("usage: cmd2s3 s3://bucket/key 'shell_command [shell_args]...'")
	}

	s3url, command := args[0], args[1]

	bucket, key, err := parseS3URL(s3url)
	if err != nil {
		log.Fatal("invalid URL: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Stderr = os.Stderr
	cmdStdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	// Note: This is what waits on the process and checks the exit status.
	// It's necessary because Reads on cmdStdout can race with Wait, so
	// the wait must come after.
	cmdStdout = readWithWaitError(cmdStdout, cmd.Wait)

	err = cmd.Start()
	if err != nil {
		log.Fatalf("Invoking shell command %q: %v", command, err)
	}

	sess := session.Must(session.NewSession())
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		// 128MiB per part (s3manager buffers these)
		u.PartSize = 128 * 1024 * 1024
		// Max 4 streams to s3 (=> max memory usage 512MiB).
		u.Concurrency = 4
	})

	resp, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:               bucket,
		Key:                  key,
		ServerSideEncryption: aws.String("AES256"),
		Body:                 cmdStdout,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Object uploaded: %v - %v", resp.Location, resp.UploadID)
}

func parseS3URL(urlStr string) (bucket, key *string, err error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, nil, err
	}
	if u.Scheme != "s3" {
		err = fmt.Errorf("only s3 urls supported, got: %q", urlStr)
		return nil, nil, err
	}
	bucket = aws.String(u.Host)
	path := ""
	if len(u.Path) > 0 {
		path = u.Path[1:]
	}
	key = aws.String(path)
	return bucket, key, nil
}

// readWithWaitError makes reads from r call wait when EOF is reached. If wait
// returns an error, that error is returned instead of EOF. This allows a
// process to simply copy from r, learning about non-zero exit status
// automatically along the way.
func readWithWaitError(r io.ReadCloser, wait func() error) io.ReadCloser {
	return &readWithWaitErrorImpl{r, wait}
}

type readWithWaitErrorImpl struct {
	io.ReadCloser
	wait func() error
}

func (r *readWithWaitErrorImpl) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err == io.EOF {
		// We hit EOF, wait on process and pass process exit status out
		// as error if there is one.
		err = r.wait()
		if err == nil { // Note: Unusual condition "==", not "!=".
			err = io.EOF
		}
	}
	return n, err
}

func uploadStream(bucket, key string, r io.Reader) error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	s3c := s3.New(sess)

	upload, err := s3c.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:               aws.String(bucket), //TODO
		Key:                  aws.String(key),    //TODO
		ServerSideEncryption: aws.String("AES256"),
	})
	if err != nil {
		return err
	}

	const (
		MiB       = 1 << 20
		chunkSize = 100 * MiB
	)
	parts, errors := chunkData(r, chunkSize)

	var haveErr bool
partsLoop:
	for {
		select {
		case part, ok := <-parts:
			if !ok {
				break partsLoop
			}

			_, err = s3c.UploadPart(&s3.UploadPartInput{
				Bucket:   upload.Bucket,
				Key:      upload.Key,
				UploadId: upload.UploadId,
				Body:     part,
			})

			if err != nil {
				goto abort
			}

		case err, haveErr = <-errors:
			if haveErr {
				goto abort
			}
		}
	}

	// Wait for error or channel to be closed.
	err, haveErr = <-errors
	if haveErr {
		goto abort
	}

	_, err = s3c.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   upload.Bucket,
		Key:      upload.Key,
		UploadId: upload.UploadId,
	})
	if err != nil {
		goto abort
	}

	return nil

abort:
	_, err2 := s3c.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		UploadId: upload.UploadId,
	})
	if err2 != nil {
		log.Printf("s3c.AbortMultipartUpload: %v", err)
	}
	return err
}

// chunkData splits the content in r into chunks of size sz or smaller.
func chunkData(r io.Reader, sz int64) (<-chan io.ReadSeeker, <-chan error) {
	chunks := make(chan io.ReadSeeker, 2)
	errors := make(chan error, 1)
	go func() {
		defer close(chunks)

		for {
			buf := &bytes.Buffer{}
			n, err := io.Copy(buf, io.LimitReader(r, sz))
			if err == io.EOF && n < sz {
				return
			}
			if err != nil && err != io.EOF {
				errors <- err
				return
			}
			chunks <- bytes.NewReader(buf.Bytes())
		}
	}()
	return chunks, errors
}
