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
	smptPort := os.Getenv("SMTP_PORT")

	var auth = smtp.PlainAuth("", fromAddress, fromEmailPassword, smtpServer)
	err = smtp.SendMail(smtpServer+":"+smptPort, auth, fromAddress, []string{toAddress}, []byte(message))
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
	//TIP Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined or highlighted text
	// to see how GoLand suggests fixing it.
	s := "gopher"
	fmt.Println("Hello and welcome, %s!", s)

	for i := 1; i <= 5; i++ {
		//TIP You can try debugging your code. We have set one <icon src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
		// for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>. To start your debugging session,
		// right-click your code in the editor and select the <b>Debug</b> option.
		fmt.Println("i =", 100/i)
	}
}

//TIP See GoLand help at <a href="https://www.jetbrains.com/help/go/">jetbrains.com/help/go/</a>.
// Also, you can try interactive lessons for GoLand by selecting 'Help | Learn IDE Features' from the main menu.
