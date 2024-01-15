package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/suite"
)

// todo - ench and optimize tests

const region = "ap-south-1"

type S3TestSuite struct {
	suite.Suite
}

func (s *S3TestSuite) SetupTest() {
	fmt.Println("test setup")
}

func (s *S3TestSuite) TearDownTest() {
	fmt.Println("test teardown")
}

func (s *S3TestSuite) TestListObjectsV2() {
	testcases := []struct {
		inputBucket  string
		inputMaxKeys int32
		outputErr    error
	}{
		{
			inputBucket:  "bucket1",
			inputMaxKeys: 12,
			outputErr:    nil,
		},
		{
			inputBucket:  "no-bucket",
			inputMaxKeys: 2,
			outputErr:    &types.NoSuchBucket{},
		},
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	s.Assert().Nil(err)
	s.Assert().Equal(region, cfg.Region)

	client := NewFromConfig(cfg)
	s.Assert().Equal(region, client.options.Region)

	for _, tc := range testcases {
		output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:  aws.String(tc.inputBucket),
			MaxKeys: &tc.inputMaxKeys,
		})

		s.Assert().Equal(tc.outputErr, err)
		if output != nil {
			s.Assert().Equal(tc.inputBucket, *output.Name)
			s.Assert().Equal(tc.inputMaxKeys, *output.MaxKeys)
			s.Assert().LessOrEqual(*output.KeyCount, *output.MaxKeys)
			s.Assert().LessOrEqual(*output.KeyCount, tc.inputMaxKeys)
			s.Assert().Equal(*output.KeyCount, int32(len(output.Contents)))
		}
	}
}

func (s *S3TestSuite) TestGetObject() {

	testcases := []struct {
		inputBucket string
		inputKey    string
		outputErr   error
	}{
		{
			inputBucket: "bucket1",
			inputKey:    "sample.txt",
			outputErr:   nil,
		},
		{
			inputBucket: "no-bucket",
			inputKey:    "sample.txt",
			outputErr:   &types.NoSuchBucket{},
		},
		{
			inputBucket: "bucket1",
			inputKey:    "no-key",
			outputErr:   &types.NoSuchKey{},
		},
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	s.Assert().Nil(err)
	s.Assert().Equal(region, cfg.Region)

	client := NewFromConfig(cfg)
	s.Assert().Equal(region, client.options.Region)

	for _, tc := range testcases {
		_, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(tc.inputBucket),
			Key:    aws.String(tc.inputKey),
		})
		s.Assert().Equal(tc.outputErr, err)
	}
}

func (s *S3TestSuite) TestBucketExists() {
	const bucketExists = "bucket1"
	const bucketNotExists = "b"

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	s.Assert().Nil(err)
	s.Assert().Equal(region, cfg.Region)

	client := NewFromConfig(cfg)
	s.Assert().Equal(region, client.options.Region)

	s.Assert().True(client.isBucketExists(bucketExists))
	s.Assert().False(client.isBucketExists(bucketNotExists))
}

func (s *S3TestSuite) TestPutObject() {

	testcases := []struct {
		inputBucket string
		inputKey    string
		inputData   string
		outputErr   error
	}{
		{
			inputBucket: "bucket1",
			inputKey:    "sample-01.txt",
			inputData:   "some data",
			outputErr:   nil,
		},
		{
			inputBucket: "no-bucket",
			inputKey:    "sample.txt",
			inputData:   "some data",
			outputErr:   &types.NoSuchBucket{},
		},
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	s.Assert().Nil(err)
	s.Assert().Equal(region, cfg.Region)

	client := NewFromConfig(cfg)
	s.Assert().Equal(region, client.options.Region)

	for _, tc := range testcases {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:        aws.String(tc.inputBucket),
			Key:           aws.String(tc.inputKey),
			Body:          strings.NewReader(tc.inputData),
			ContentLength: aws.Int64(int64(len([]byte(tc.inputData)))),
		})
		s.Assert().Equal(tc.outputErr, err)
	}
}

func TestS3TestSuite(t *testing.T) {
	suite.Run(t, new(S3TestSuite))
}
