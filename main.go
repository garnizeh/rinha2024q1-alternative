package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"

	_ "github.com/mattn/go-sqlite3"
)

var dbRs, dbWs [5]*sql.DB

func main() {
	addr := strings.TrimSpace(os.Getenv("ADDR"))
	if addr == "" {
		addr = ":8080"
	}
	folder := strings.TrimSpace(os.Getenv("FOLDER"))
	if folder == "" {
		folder = "data"
	}
	to := strings.TrimSpace(os.Getenv("TIMEOUT"))
	if to == "" {
		to = "25"
	}
	timeout, err := strconv.Atoi(to)
	if err != nil {
		panic(fmt.Sprintf("invalid timeout value: %s", to))
	}

	dir, _ := os.ReadDir(folder)
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{folder, d.Name()}...))
	}

	for i := range 5 {
		if err := openDBs(folder, i, timeout); err != nil {
			panic(err)
		}

		defer func() {
			dbWs[i].Close()
			dbRs[i].Close()
		}()
	}

	app := fiber.New(fiber.Config{
		JSONEncoder:           json.Marshal,
		JSONDecoder:           json.Unmarshal,
		DisableStartupMessage: true,
		AppName:               "garnizeh",
	})

	app.Get("/clientes/:id/extrato", getExtrato(dbRs))
	app.Post("/clientes/:id/transacoes", postTransacao(dbWs))

	fmt.Println("addr [", addr, "] - folder [", folder, "] timeout [", timeout, "ms]")
	fmt.Println("cocoricó!")

	if err := app.Listen(addr); err != nil {
		panic(err)
	}
}

func openDBs(folder string, i, txTimeout int) error {
	// Create a DB writer instance to migrate
	dbM, err := sql.Open("sqlite3", fmt.Sprintf("%s/rinha-%d.db?_journal=wal&_txlock=immediate", folder, i))
	if err != nil {
		return err
	}
	defer dbM.Close()

	if err := migrate(dbM, i); err != nil {
		return err
	}

	// Create a DB writer instance
	dbW, err := sql.Open("sqlite3", fmt.Sprintf("%s/rinha-%d.db?_journal=wal&_txlock=immediate&_timeout=%d", folder, i, txTimeout))
	if err != nil {
		return err
	}
	dbW.SetMaxOpenConns(1)
	dbWs[i] = dbW

	// Create a DB reader instance
	dbR, err := sql.Open("sqlite3", fmt.Sprintf("%s/rinha-%d.db?_journal=wal&_txlock=deferred", folder, i))
	if err != nil {
		return err
	}
	dbRs[i] = dbR

	return nil
}

func migrate(db *sql.DB, id int) error {
	const (
		createCs  = `CREATE TABLE IF NOT EXISTS cs (l INT, b INT);`
		createTxs = `CREATE TABLE IF NOT EXISTS ts (v INT, t VARCHAR(1), d VARCHAR(10), ts TIMESTAMP);`
		insert    = `INSERT OR IGNORE INTO cs (l, b) VALUES (%d, 0);`
	)

	for range 10 {
		tx, err := db.Begin()
		if err != nil {
			time.Sleep(time.Second * time.Duration(1))
			continue
		}

		if _, err := tx.Exec(createCs); err != nil {
			return err
		}

		if _, err := tx.Exec(createTxs); err != nil {
			return err
		}

		var lim int
		switch id {
		case 0:
			lim = 100000
		case 1:
			lim = 80000
		case 2:
			lim = 1000000
		case 3:
			lim = 10000000
		case 4:
			lim = 500000
		}
		if _, err := tx.Exec(fmt.Sprintf(insert, lim)); err != nil {
			return err
		}

		err = tx.Commit()
		if err != nil {
			return err
		}

		return nil
	}

	return errors.New("failed to migrate")
}

func getExtrato(dbs [5]*sql.DB) func(*fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		id := c.Params("id")
		i, err := strconv.Atoi(id)
		if err != nil {
			return c.SendStatus(422)
		} else if i < 1 || i > 5 {
			return c.SendStatus(404)
		}

		return rx(c, dbs[i-1])
	}
}

func postTransacao(dbs [5]*sql.DB) func(*fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		type reqTransacao struct {
			Valor     int    `json:"valor"`
			Tipo      string `json:"tipo"`
			Descricao string `json:"descricao"`
		}

		id := c.Params("id")
		i, err := strconv.Atoi(id)
		if err != nil {
			return c.SendStatus(422)
		} else if i < 1 || i > 5 {
			return c.SendStatus(404)
		}

		var transacao reqTransacao
		if err := c.BodyParser(&transacao); err != nil {
			return c.SendStatus(422)
		}

		tam := len(transacao.Descricao)
		if transacao.Valor == 0 || (transacao.Tipo != "c" && transacao.Tipo != "d") || tam == 0 || tam > 10 {
			return c.SendStatus(422)
		}

		return tx(c, dbs[i-1], transacao.Valor, transacao.Tipo, transacao.Descricao)
	}
}

func rx(c *fiber.Ctx, db *sql.DB) error {
	const (
		pegaSaldo      = `SELECT l, b FROM cs`
		pegaTransacoes = `SELECT v, t, d, ts FROM ts ORDER BY rowid DESC LIMIT 10`
	)

	type saldo struct {
		Limite      int    `json:"limite"`
		Total       int    `json:"total"`
		DataExtrato string `json:"data_extrato"`
	}

	type transacao struct {
		Valor       int    `json:"valor"`
		Tipo        string `json:"tipo"`
		Descricao   string `json:"descricao"`
		RealizadaEm string `json:"realizada_em"`
	}

	var (
		transactions   = make([]transacao, 0, 10)
		balanceDetails saldo
	)

	rows, err := db.Query(pegaSaldo)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return c.SendStatus(404)
		}

		return c.SendStatus(500)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			l, t int
		)
		if err := rows.Scan(&l, &t); err != nil {
			return c.SendStatus(500)
		}

		balanceDetails.Limite = l
		balanceDetails.Total = t
		balanceDetails.DataExtrato = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if rows.Err() != nil {
		return c.SendStatus(500)
	}

	rows, err = db.Query(pegaTransacoes)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return c.SendStatus(500)
	}

	for rows.Next() {
		var (
			v        int
			t, d, ts string
		)
		if err := rows.Scan(&v, &t, &d, &ts); err != nil {
			return c.SendStatus(500)
		}

		transactions = append(transactions, transacao{
			Valor:       v,
			Tipo:        t,
			Descricao:   d,
			RealizadaEm: ts,
		})
	}
	if rows.Err() != nil {
		return c.SendStatus(500)
	}

	return c.Status(200).JSON(struct {
		Saldo             saldo       `json:"saldo"`
		UltimasTransacoes []transacao `json:"ultimas_transacoes"`
	}{
		Saldo:             balanceDetails,
		UltimasTransacoes: transactions,
	})
}

func tx(c *fiber.Ctx, db *sql.DB, valor int, tipo, descricao string) error {
	const (
		pegaSaldoAtualizar = `SELECT l, b FROM cs`
		insereTransacao    = `INSERT INTO ts(v, t, d, ts) VALUES ($1, $2, $3, DATE('now'))`
		atualizaSaldo      = `UPDATE cs SET b = $1`
	)

	var limite, saldo int

	tx, err := db.Begin()
	if err != nil {
		return c.SendStatus(500)
	}
	defer tx.Rollback()

	if err := tx.QueryRow(pegaSaldoAtualizar).Scan(&limite, &saldo); err != nil {
		if err == sql.ErrNoRows {
			return c.SendStatus(404)
		}

		return c.SendStatus(422)
	}

	if tipo == "c" {
		saldo += valor
	} else {
		saldo -= valor
		if -saldo > limite {
			return c.SendStatus(422)
		}
	}

	if _, err := tx.Exec(insereTransacao, valor, tipo, descricao); err != nil {
		return c.SendStatus(422)
	}
	if _, err := tx.Exec(atualizaSaldo, saldo); err != nil {
		return c.SendStatus(422)
	}

	if err = tx.Commit(); err != nil {
		return c.SendStatus(500)
	}

	return c.Status(200).JSON(struct {
		Limite int `json:"limite"`
		Saldo  int `json:"saldo"`
	}{
		Saldo:  saldo,
		Limite: limite,
	})
}
