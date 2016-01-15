package main
import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/session"
)

func main(){
	GetItem()
}

func GetItem() {
	svc := dynamodb.New(session.New(),&aws.Config{Region: aws.String("us-east-1")})

	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{ // Required
			"productId": { // Required
				S:    aws.String("34061010"),
			},
			// More values...
		},
		TableName: aws.String("ProductRecommendation"), // Required
		AttributesToGet: []*string{
			aws.String("boughtWith"), // Required
			// More values...
		},
		ConsistentRead: aws.Bool(true),
		/*
		ExpressionAttributeNames: map[string]*string{
			"Key": aws.String("AttributeName"), // Required
			// More values...
		},
		ProjectionExpression:   aws.String("ProjectionExpression"),
		ReturnConsumedCapacity: aws.String("ReturnConsumedCapacity"),
		*/
	}
	resp, err:= svc.GetItem(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	// Pretty-print the response data.
	str:=resp.Item["boughtWith"].S
	fmt.Printf("%v",*str)

	fmt.Printf("%v",*str)

	fmt.Println("done")

}


