package main

import (
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func s3FileUpload(data string) error {
	bucketname := "silvia-lambda-test"
	objectname := "test"

	f, err := os.OpenFile("/tmp/"+objectname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Cannot open file")
		return err
	}

	defer f.Close()

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-southeast-2")},
	)

	downloader := s3manager.NewDownloader(sess)

	numBytes, err := downloader.Download(f,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketname),
			Key:    aws.String(objectname),
		})
	if err != nil {
		log.Fatalf("#### Unable to download item %q, %v", objectname, err)
		return nil
	}

	log.Println("#### Downloaded", f.Name(), numBytes, "bytes")

	log.Print("##### Appending to file")

	if _, err = f.WriteString(data + "\n"); err != nil {
		log.Fatal(err)
		return err
	}

	uploader := s3manager.NewUploader(sess)

	file, err := os.Open("/tmp/" + objectname)
	if err != nil {
		log.Fatal("#### Cannot open file")
		return err
	}

	defer file.Close()

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketname),
		Key:    aws.String(objectname),
		Body:   file,
	})
	if err != nil {
		// Print the error and exit.
		return err
	}

	log.Printf("Successfully uploaded %q to %q\n", objectname, bucketname)
	return nil
}

func insert(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// var b string
	// err := re
	// if err != nil {
	// 	return events.APIGatewayProxyResponse{
	// 		StatusCode: 400,
	// 		Body:       "Invalid payload",
	// 	}, nil
	// }

	log.Printf("Received request #### %v", req.Body)

	//json.Marshal(movies)
	err := s3FileUpload(req.Body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       err.Error(),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(req.Body),
	}, nil
}

func main() {
	lambda.Start(insert)
	// s3FileUpload("test payload")
}
