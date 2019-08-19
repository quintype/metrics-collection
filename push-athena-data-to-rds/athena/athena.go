package athena

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

type errorMessage struct {
	message string
	err     error
}

func startAthenaQuery(athenaClient *athena.Athena, athenaQuery string, athenaDBName string, S3Location string) (*athena.StartQueryExecutionOutput, error) {
	var query athena.StartQueryExecutionInput

	var database athena.QueryExecutionContext
	database.SetDatabase(athenaDBName)

	var resultLocation athena.ResultConfiguration
	resultLocation.SetOutputLocation(S3Location)

	query.SetQueryString(athenaQuery)
	query.SetQueryExecutionContext(&database)
	query.SetResultConfiguration(&resultLocation)

	return athenaClient.StartQueryExecution(&query)
}

func runAthenaQuery(athenaClient *athena.Athena, queryInput athena.GetQueryExecutionInput) errorMessage {
	duration := time.Duration(2) * time.Second
	var exectionMessage errorMessage

	for {
		queryOutput, executionErr := athenaClient.GetQueryExecution(&queryInput)
		if executionErr != nil {
			exectionMessage.message = "Error Executing Athena Query"
			exectionMessage.err = executionErr
			return exectionMessage
		}
		if *queryOutput.QueryExecution.Status.State != "RUNNING" {
			break
		}
		fmt.Println("Running query")
		time.Sleep(duration)
	}

	queryOutput, executionErr := athenaClient.GetQueryExecution(&queryInput)

	if *queryOutput.QueryExecution.Status.State == "SUCCEEDED" {
		var resultInput athena.GetQueryResultsInput
		resultInput.SetQueryExecutionId(*queryInput.QueryExecutionId)

		_, err := athenaClient.GetQueryResults(&resultInput)

		if err != nil {
			fmt.Println(err)
			exectionMessage.message = "Error fetching the query results"
			exectionMessage.err = err
			return exectionMessage
		}

		exectionMessage.message = "Successfully executed the query."

		return exectionMessage
	}
	fmt.Println(*queryOutput.QueryExecution.Status.State)
	exectionMessage.message = "Error Executing Athena Query"
	exectionMessage.err = executionErr
	return exectionMessage
}

func SaveDataToS3(athenaQuery string, athenaDBName string, S3Location string) (string, errorMessage) {
	var errorMessage errorMessage

	awsConfig := &aws.Config{}
	awsConfig.WithRegion("us-east-1")

	sess := session.Must(session.NewSession(awsConfig))
	newAthenaClient := athena.New(sess, awsConfig)

	startExecutionResult, startExecutionErr := startAthenaQuery(newAthenaClient, athenaQuery, athenaDBName, S3Location)

	fmt.Println(startExecutionResult, startExecutionErr)

	var queryInput athena.GetQueryExecutionInput
	queryInput.SetQueryExecutionId(*startExecutionResult.QueryExecutionId)

	exectionResult := runAthenaQuery(newAthenaClient, queryInput)

	if exectionResult.err != nil {
		return "", exectionResult
	}

	return *startExecutionResult.QueryExecutionId, errorMessage
}
