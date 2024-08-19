package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"log"
	"net/smtp"
	"os"
	"strings"
	"time"
)

type User struct {
	ID        string    `json:"id"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func sendEmail(message string, toAddress string) (response bool, err error) {
	fromAddress := os.Getenv("EMAIL")
	fromEmailPassword := os.Getenv("PASSWORD")
	smtpServer := os.Getenv("SMTP_SERVER")
	smtpPort := os.Getenv("SMTP_PORT")

	var auth = smtp.PlainAuth("", fromAddress, fromEmailPassword, smtpServer)
	err = smtp.SendMail(smtpServer+":"+smtpPort, auth, fromAddress, []string{toAddress}, []byte(message))
	log.Println("EMail sent successfully!")
	if err == nil {
		return true, nil
	}

	return false, err
}

func consume(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{os.Getenv("KAFKA_BROKER")},
		Topic:   "new-user",
		GroupID: "email-new-users",
	})

	for {
		msg, err := r.ReadMessage(ctx)
		log.Println("Consume message")
		if err != nil {
			panic("could not read message " + err.Error())
		}
		userData := msg.Value

		var user User

		err = json.Unmarshal(userData, &user)
		if err != nil {
			panic("could not parse userData " + err.Error())
		}

		subject := "Subject: Account created!\n\n"
		body := fmt.Sprintf("You account is now active and your ID is %s. Congrats!", user.ID)
		message := strings.Join([]string{subject, body}, " ")

		sendEmail(message, user.Email)
	}
}

func main() {
	consume(context.Background())
}
