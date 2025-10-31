package job

// ApiCallerJobRequest represents the request to create an ApiCallerJob
type ApiCallerJobRequest struct {
	Schedule string
	Type     JobRunGuarantee
	API      string
}
