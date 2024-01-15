package main

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// "/tmp/mockaws/<region>/<service>/.."
const rootPath = "/tmp/mockaws/"

type S3Mock struct {
	options s3.Options
}

func New(options s3.Options, optFns ...func(*s3.Options)) *S3Mock {
	for _, fn := range optFns {
		fn(&options)
	}

	s3 := &S3Mock{options: options}
	return s3
}

func NewFromConfig(cfg aws.Config, optFns ...func(*s3.Options)) *S3Mock {
	opts := s3.Options{
		Region:             cfg.Region,
		DefaultsMode:       cfg.DefaultsMode,
		RuntimeEnvironment: cfg.RuntimeEnvironment,
		HTTPClient:         cfg.HTTPClient,
		Credentials:        cfg.Credentials,
		APIOptions:         cfg.APIOptions,
		Logger:             cfg.Logger,
		ClientLogMode:      cfg.ClientLogMode,
		AppID:              cfg.AppID,
	}
	return New(opts, optFns...)
}

func (s *S3Mock) isBucketExists(bucket string) bool {
	p := rootPath + s.options.Region + "/s3/"
	entries, err := os.ReadDir(p)
	if err != nil {
		return false
	}

	for _, e := range entries {
		if e.Name() == bucket {
			return true
		}
	}
	return false
}

func (s *S3Mock) listContents(inp *s3.ListObjectsV2Input) ([]types.Object, error) {
	var s3Objects []types.Object

	dir := rootPath + s.options.Region + "/s3/" + *inp.Bucket

	objects, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	if int32(len(objects)) > *inp.MaxKeys {
		objects = objects[:*inp.MaxKeys]
	}

	for _, object := range objects {
		fileInfo, _ := object.Info()

		obj := types.Object{
			Key:          aws.String(object.Name()),
			LastModified: aws.Time(fileInfo.ModTime()),
			Size:         aws.Int64(fileInfo.Size()),
		}

		s3Objects = append(s3Objects, obj)
	}
	return s3Objects, nil
}

func (s *S3Mock) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if params == nil {
		params = &s3.ListObjectsV2Input{}
	}

	if !s.isBucketExists(*params.Bucket) {
		return nil, &types.NoSuchBucket{}
	}

	contents, err := s.listContents(params)
	if err != nil {
		return nil, err
	}

	out := &s3.ListObjectsV2Output{
		Name:     params.Bucket,
		MaxKeys:  params.MaxKeys,
		KeyCount: aws.Int32(int32(len(contents))),
		Contents: contents,
	}
	return out, nil
}

func (s *S3Mock) getContent(params *s3.GetObjectInput) ([]byte, fs.FileInfo, error) {
	path := rootPath + s.options.Region + "/s3/" + *params.Bucket + "/" + *params.Key

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, &types.NoSuchKey{}
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, nil, &types.NoSuchKey{}
	}

	var data []byte
	_, err = f.Read(data)
	if err != nil {
		return nil, nil, &types.NoSuchKey{}
	}
	return data, fileInfo, nil
}

func (s *S3Mock) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if params == nil {
		params = &s3.GetObjectInput{}
	}

	if !s.isBucketExists(*params.Bucket) {
		return nil, &types.NoSuchBucket{}
	}

	data, fileInfo, err := s.getContent(params)
	if err != nil {
		return nil, err
	}

	out := &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: aws.Int64(int64(len(data))),
		LastModified:  aws.Time(fileInfo.ModTime()),
	}

	return out, nil
}
