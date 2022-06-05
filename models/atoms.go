package models

type AwsCredentials struct {
	KeyId        *string `json:"KeyId"`
	AccessKey    *string `json:"AccessKey"`
	Region       *string `json:"Region"`
	SessionToken *string `json:"SessionToken"`
}

type BucketParams struct {
	Name        string          `json:"Name"`
	Credentials *AwsCredentials `json:"Credentials"`
}

type SqsParams struct {
	Url         string          `json:"Url"`
	Credentials *AwsCredentials `json:"Credentials"`
}
