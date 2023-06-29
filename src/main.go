package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	_ "github.com/lib/pq" // PostgreSQL driver
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

type accountType string

const (
	client   accountType = "client"
	employee accountType = "employee"
)

type DBHandler struct {
	db *sql.DB
}

var currentUsername string

func listenForNotifies(dburl string) {
	li := pq.NewListener(dburl, 10*time.Second, time.Minute, func(event pq.ListenerEventType, err error) {
		if err != nil {
			log.Fatal(err)
		}
	})

	if err := li.Listen("raise_notice"); err != nil {
		panic(err)
	}

	for {
		select {
		case n := <-li.Notify:
			// n.Extra contains the payload from the notification
			log.Println(n.Extra)
		case <-time.After(5 * time.Minute):
			if err := li.Ping(); err != nil {
				panic(err)
			}
		}
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	connStr := "postgres://postgres:postgres@localhost/postgres?sslmode=disable"
	go listenForNotifies(connStr)
	// Connect to the database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Ping the database to verify the connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to the PostgreSQL database")

	_, err = db.Exec("SET search_path TO kim_bank")
	if err != nil {
		log.Fatal(err)
	}

	dbHandler := DBHandler{db: db}

	var userOption string
	for true {
		fmt.Println("Choose one of the following options:")
		fmt.Println("1) register\n2) login\n3) deposit\n4) withdraw\n" +
			"5) transfer\n6) update balances\n7) check balance")

		userOption, _ = reader.ReadString('\n')
		userOption = strings.TrimSpace(userOption)

		switch userOption {
		case "1":
			dbHandler.handleRegister()
		case "2":
			dbHandler.handleLogin()
		case "3":
			dbHandler.handleDeposit()
		case "4":
			dbHandler.handleWithdraw()
		case "5":
			dbHandler.handleTransfer()
		case "6":
			dbHandler.handleUpdateBalances()
		case "7":
			dbHandler.handleCheckBalance()
		default:
			fmt.Println("Error: unknown user option!")
		}
	}

	// Close the database connection
	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (h *DBHandler) handleRegister() {
	var username, password, firstname, lastname, nationalID, dateOfBirth string
	var account accountType
	var interestRate int

	fmt.Println("Username: ")
	fmt.Scanf("%s", &username)
	fmt.Println("Password: ")
	fmt.Scanf("%s", &password)
	fmt.Println("Firstname: ")
	fmt.Scanf("%s", &firstname)
	fmt.Println("Lastname: ")
	fmt.Scanf("%s", &lastname)
	fmt.Println("National ID: ")
	fmt.Scanf("%s", &nationalID)
	fmt.Println("Date of birth (yy/mm/dd): ")
	fmt.Scanf("%s", &dateOfBirth)
	fmt.Println("Account type (client/employee): ")
	fmt.Scanf("%s", &account)
	for account != client && account != employee {
		fmt.Println("Account type (client/employee): ")
		fmt.Scanf("%s", &account)
	}

	fmt.Println("Interest rate: ")
	fmt.Scanf("%s", &interestRate)

	h.register(username, password, firstname, lastname, nationalID, dateOfBirth, interestRate, account)
}

func (h *DBHandler) register(username, password, firstname, lastname, nationalID, dateOfBirth string,
	interestRate int, account accountType) {
	res, err := h.db.Exec("CALL register($1, $2, $3, $4, $5, $6, $7, $8)",
		username, password, firstname, lastname, nationalID, dateOfBirth, account, interestRate)
	if err != nil {
		log.Errorf("Register error: %s", err)
		return
	}

	log.Infof("Successful register %+v\n", res)
	currentUsername = username
}

func (h *DBHandler) handleLogin() {
	var username, password string

	fmt.Println("Username:")
	fmt.Scanf("%s", &username)
	fmt.Println("Password:")
	fmt.Scanf("%s", &password)

	h.login(username, password)
}

func (h *DBHandler) login(username, password string) {
	res, err := h.db.Exec("CALL login($1, $2)", username, password)
	if err != nil {
		log.Errorf("Login error: %s", err)
		return
	}

	log.Infof("Successful login %+v\n", res)
	currentUsername = username
}

func (h *DBHandler) handleDeposit() {
	if currentUsername != "" {
		var amount int64
		fmt.Println("How much?")
		fmt.Scanf("%d", &amount)
		h.deposit(amount)
	} else {
		fmt.Println("Error: you must login first!")
	}
}

func (h *DBHandler) deposit(amount int64) {
	res, err := h.db.Exec("CALL deposit($1)", amount)
	if err != nil {
		log.Errorf("Deposit error: %s", err)
		return
	}

	log.Infof("Successful deposit %+v\n", res)
}

func (h *DBHandler) handleWithdraw() {
	if currentUsername != "" {
		var amount int64
		fmt.Println("How much?")
		fmt.Scanf("%d", &amount)
		h.withdraw(amount)
	} else {
		fmt.Println("Error: you must login first!")
	}
}

func (h *DBHandler) withdraw(amount int64) {
	res, err := h.db.Exec("CALL withdraw($1)", amount)
	if err != nil {
		log.Errorf("Withdraw error: %s", err)
		return
	}

	log.Infof("Successful withdraw %+v\n", res)
}

func (h *DBHandler) handleTransfer() {
	if currentUsername != "" {
		var amount int64
		var toAccount int
		fmt.Println("How much?")
		fmt.Scanf("%d", &amount)
		fmt.Println("To which account number?")
		fmt.Scanf("%d", &toAccount)
		h.transfer(amount, toAccount)
	} else {
		fmt.Println("Error: you must login first!")
	}
}

func (h *DBHandler) transfer(amount int64, toAccount int) {
	res, err := h.db.Exec("CALL transfer($1, $2)", amount, toAccount)
	if err != nil {
		log.Errorf("Transfer error: %s", err)
		return
	}

	log.Infof("Successful transfer %+v\n", res)
}

func (h *DBHandler) handleUpdateBalances() {
	h.updateBalances()
}

func (h *DBHandler) updateBalances() {
	res, err := h.db.Exec("CALL updateBalances()")
	if err != nil {
		log.Errorf("Update balances error: %s", err)
		return
	}

	log.Infof("Successful update balances %+v\n", res)
}

func (h *DBHandler) handleCheckBalance() {
	if currentUsername != "" {
		h.checkBalance()
	} else {
		fmt.Println("Error: you must login first!")
	}
}

func (h *DBHandler) checkBalance() {
	_, err := h.db.Exec("CALL checkBalance()")
	if err != nil {
		log.Errorf("Check balance error: %s", err)
		return
	}
}
