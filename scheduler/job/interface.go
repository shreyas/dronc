package job

type JobInterface interface {
	// Namespace returns the namespace used to construct the id for a job instance
	Namespace() string

	// AtMostOnce indicates if the job is scheduled to run at most once
	AtMostOnce() bool

	// AtLeastOnce indicates if the job is scheduled to run at least once
	AtLeastOnce() bool

	// NextOccurance returns the next scheduled occurrence time (in Unix timestamp) after the given time
	NextOccurance(after int64) (int64, error)
}
