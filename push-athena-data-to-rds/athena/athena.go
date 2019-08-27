package athena

import (
	"fmt"
	"push-athena-data-to-rds/types"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

func startAthenaQuery(athenaClient *athena.Athena, athenaQuery string, athenaDBName string, S3Location string) (*athena.StartQueryExecutionOutput, error) {
	var query athena.StartQueryExecutionInput

	var database athena.QueryExecutionContext
	database.SetDatabase(athenaDBName)

	var resultLocation athena.ResultConfiguration
	resultLocation.SetOutputLocation(S3Location)

	query.SetQueryString(athenaQuery)
	query.SetQueryExecutionContext(&database)

	fmt.Println(resultLocation)
	query.SetResultConfiguration(&resultLocation)

	return athenaClient.StartQueryExecution(&query)
}

func runAthenaQuery(athenaClient *athena.Athena, queryInput athena.GetQueryExecutionInput) types.ErrorMessage {
	duration := time.Duration(2) * time.Second
	var exectionMessage types.ErrorMessage

	for {
		queryOutput, executionErr := athenaClient.GetQueryExecution(&queryInput)
		if executionErr != nil {
			exectionMessage = types.ErrorMessage{Message: "Error Executing Athena Query", Err: executionErr}
			return exectionMessage
		}
		if *queryOutput.QueryExecution.Status.State != "RUNNING" {
			break
		}
		fmt.Println("Running query")
		time.Sleep(duration)
	}

	queryOutput, executionErr := athenaClient.GetQueryExecution(&queryInput)
	fmt.Println("output debugging")
	fmt.Println(queryOutput)

	if *queryOutput.QueryExecution.Status.State == "SUCCEEDED" {
		var resultInput athena.GetQueryResultsInput
		resultInput.SetQueryExecutionId(*queryInput.QueryExecutionId)

		_, err := athenaClient.GetQueryResults(&resultInput)

		if err != nil {
			exectionMessage = types.ErrorMessage{Message: "Error fetching the query results", Err: err}
			return exectionMessage
		}

		exectionMessage = types.ErrorMessage{Message: "Successfully executed the query.", Err: nil}

		return exectionMessage
	}
	exectionMessage = types.ErrorMessage{Message: "Error Executing Athena Query", Err: executionErr}
	return exectionMessage
}

func SaveDataToS3(athenaQuery string, athenaDBName string, S3Location string) (string, types.ErrorMessage) {
	var errorMessage types.ErrorMessage

	awsConfig := &aws.Config{}
	awsConfig.WithRegion("us-east-1")

	sess := session.Must(session.NewSession(awsConfig))
	newAthenaClient := athena.New(sess, awsConfig)

	startExecutionResult, startExecutionErr := startAthenaQuery(newAthenaClient, athenaQuery, athenaDBName, S3Location)

	fmt.Println(startExecutionResult, startExecutionErr)

	var queryInput athena.GetQueryExecutionInput
	queryInput.SetQueryExecutionId(*startExecutionResult.QueryExecutionId)

	exectionResult := runAthenaQuery(newAthenaClient, queryInput)

	if exectionResult.Err != nil {
		return "", exectionResult
	}

	return *startExecutionResult.QueryExecutionId, errorMessage
}
