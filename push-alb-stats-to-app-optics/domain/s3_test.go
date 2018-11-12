package domain

import (
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
)

type mockS3Client struct {
	s3iface.S3API
	returnValue *s3.GetObjectOutput
	returnError error
}

func (m *mockS3Client) GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return m.returnValue, m.returnError
}

func TestSuccessfullyReturnsAStream(t *testing.T) {
	mockStream := struct{ io.ReadCloser }{}
	stream, err := GetAlbLogStream(&mockS3Client{returnValue: &s3.GetObjectOutput{Body: mockStream}}, "bucket", "subfolder/AWSLogs/12345678/elasticloadbalancing/zone/2018/11/06/12345678_elasticloadbalancing_zone_app.alb-name.12345689abcd_20181106T2355Z_ip_address_random.log.gz")
	assert.Nil(t, err)
	assert.Equal(t, mockStream, stream)
}

func TestReturnsAnErrorIfTheFileIsNotInTheCorrectFormat(t *testing.T) {
	stream, err := GetAlbLogStream(&mockS3Client{}, "bucket", "invalid-value.gz")
	assert.NotNil(t, err)
	assert.Nil(t, stream)
}

func TestAbortsIfAWSReturnsAnError(t *testing.T) {
	stream, err := GetAlbLogStream(&mockS3Client{returnError: errors.New("error")}, "bucket", "subfolder/AWSLogs/12345678/elasticloadbalancing/zone/2018/11/06/12345678_elasticloadbalancing_zone_app.alb-name.12345689abcd_20181106T2355Z_ip_address_random.log.gz")
	assert.NotNil(t, err)
	assert.Nil(t, stream)
}
