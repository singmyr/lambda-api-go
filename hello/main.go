package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Response is of type APIGatewayProxyResponse since we're leveraging the
// AWS Lambda Proxy Request functionality (default behavior)
//
// https://serverless.com/framework/docs/providers/aws/events/apigateway/#lambda-proxy-integration
type Response events.APIGatewayProxyResponse

// ItemInfo has more data for our movie item
type ItemInfo struct {
	Plot   string  `json:"plot"`
	Rating float64 `json:"rating"`
}

// Item has fields for the DynamoDB keys (Year and Title) and an ItemInfo for more data
type Item struct {
	Year  int      `json:"year"`
	Title string   `json:"title"`
	Info  ItemInfo `json:"info"`
}

// Handler is our lambda handler invoked by the `lambda.Start` function call
func Handler(ctx context.Context) (Response, error) {
	var buf bytes.Buffer

	body, err := json.Marshal(map[string]interface{}{
		"message": "Go Serverless v1.0! Your function executed successfully!",
	})
	if err != nil {
		return Response{StatusCode: 404}, err
	}
	json.HTMLEscape(&buf, body)

	resp := Response{
		StatusCode:      200,
		IsBase64Encoded: false,
		Body:            buf.String(),
		Headers: map[string]string{
			"Content-Type":           "application/json",
			"X-MyCompany-Func-Reply": "hello-handler",
		},
	}

	items, err := ListByYear("1989") // request.PathParameters["year"]
	if err != nil {
		panic(fmt.Sprintf("Failed to find Item, %v", err))
	}

	// Make sure the Item isn't empty
	if len(items) == 0 {
		fmt.Println("Could not find movies with year ", "1989")
		return Response{Body: "asdf", StatusCode: 500}, nil
	}

	// Log and return result
	stringItems := "["
	for i := 0; i < len(items); i++ {
		jsonItem, _ := json.Marshal(items[i])
		stringItems += string(jsonItem)
		if i != len(items)-1 {
			stringItems += ",\n"
		}
	}
	stringItems += "]\n"
	fmt.Println("Found items: ", stringItems)
	return Response{Body: stringItems, StatusCode: 200}, nil

	return resp, nil
}

func ListByYear(year string) ([]Item, error) {
	// Build the Dynamo client object
	// sess := session.Must(session.NewSession())
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String("eu-west-1"),
			Credentials: credentials.NewStaticCredentialsFromCreds(credentials.Value{
				AccessKeyID:     "KEY",
				SecretAccessKey: "SECRET",
			}),
		},
		Profile: "default",
	}))
	fmt.Println(sess)
	svc := dynamodb.New(sess)
	items := []Item{}

	// Create the Expression to fill the input struct with.
	yearAsInt, err := strconv.Atoi(year)
	filt := expression.Name("year").Equal(expression.Value(yearAsInt))

	// Get back the title, year, and rating
	proj := expression.NamesList(expression.Name("title"), expression.Name("year"))

	expr, err := expression.NewBuilder().WithFilter(filt).WithProjection(proj).Build()

	if err != nil {
		fmt.Println("Got error building expression:")
		fmt.Println(err.Error())
		return items, err
	}

	fmt.Println(os.Getenv("TABLE_NAME"))
	// Build the query input parameters
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String("movies-dev"),
	}

	// Make the DynamoDB Query API call
	result, err := svc.Scan(params)
	fmt.Println("Result", result)

	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println((err.Error()))
		return items, err
	}

	numItems := 0
	for _, i := range result.Items {
		item := Item{}

		err = dynamodbattribute.UnmarshalMap(i, &item)

		if err != nil {
			fmt.Println("Got error unmarshalling:")
			fmt.Println(err.Error())
			return items, err
		}

		fmt.Println("Title: ", item.Title)
		items = append(items, item)
		numItems++
	}

	fmt.Println("Found", numItems, "movie(s) in year ", year)
	if err != nil {
		fmt.Println(err.Error())
		return items, err
	}

	return items, nil
}

func main() {
	lambda.Start(Handler)
}
