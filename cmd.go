package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ish4n10/miniaturedb/sql/executor"
	"github.com/ish4n10/miniaturedb/sql/lexer"
	"github.com/ish4n10/miniaturedb/sql/parser"
	db "github.com/ish4n10/miniaturedb/storage"
)

const banner = `
 ███╗   ███╗██╗███╗   ██╗██╗ █████╗ ████████╗██╗   ██╗██████╗ ███████╗██████╗ ██████╗
 ████╗ ████║██║████╗  ██║██║██╔══██╗╚══██╔══╝██║   ██║██╔══██╗██╔════╝██╔══██╗██╔══██╗
 ██╔████╔██║██║██╔██╗ ██║██║███████║   ██║   ██║   ██║██████╔╝█████╗  ██║  ██║██████╔╝
 ██║╚██╔╝██║██║██║╚██╗██║██║██╔══██║   ██║   ██║   ██║██╔══██╗██╔══╝  ██║  ██║██╔══██╗
 ██║ ╚═╝ ██║██║██║ ╚████║██║██║  ██║   ██║   ╚██████╔╝██║  ██║███████╗██████╔╝██████╔╝
 ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═════╝
`

func main() {
	args := os.Args[1:]

	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "usage: miniaturedb <path>\n")
		os.Exit(1)
	}

	path := args[0]

	d, err := db.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening db: %v\n", err)
		os.Exit(1)
	}
	defer d.Close()

	exe := executor.NewExecutor(d)

	fmt.Println(banner)
	fmt.Printf("  miniaturedb \n")
	fmt.Printf("  opened: %s\n", path)
	fmt.Println("  type 'help' for commands, 'exit' to quit\n")

	scanner := bufio.NewScanner(os.Stdin)
	var buf strings.Builder

	for {
		if buf.Len() == 0 {
			fmt.Print("mdb> ")
		} else {
			fmt.Print("...> ")
		}

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			continue
		}

		switch strings.ToLower(line) {
		case "exit", "quit", "\\q":
			fmt.Println("bye.")
			return
		case "help", "\\h":
			printHelp()
			continue
		case "clear", "\\c":
			buf.Reset()
			continue
		}

		buf.WriteString(line)
		buf.WriteString(" ")

		if !strings.Contains(line, ";") {
			continue
		}

		query := strings.TrimSpace(buf.String())
		buf.Reset()

		result, err := runQuery(exe, query)
		if err != nil {
			fmt.Printf("ERROR: %v\n\n", err)
			continue
		}

		printResult(result)
	}
}

func runQuery(exe *executor.Executor, query string) (*executor.Result, error) {
	start := time.Now()

	lx := lexer.NewLexer(query)
	tokens, err := lx.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("syntax error: %v", err)
	}

	p := parser.NewParser(tokens)
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %v", err)
	}

	result, err := exe.Execute(stmt)
	if err != nil {
		return nil, err
	}

	result.Duration = time.Since(start)
	return result, nil
}

func printResult(r *executor.Result) {
	if len(r.Rows) == 0 {
		if r.Affected > 0 {
			fmt.Printf("OK (%d row affected)\n\n", r.Affected)
		} else {
			fmt.Println("OK\n")
		}
		fmt.Printf("%d row(s) (%.3f ms)\n\n", len(r.Rows), float64(r.Duration.Microseconds())/1000.0)

		return
	}

	// measure column widths
	keyWidth := len("key")
	valWidth := len("value")
	for _, row := range r.Rows {
		if len(row.Key) > keyWidth {
			keyWidth = len(row.Key)
		}
		if len(row.Value) > valWidth {
			valWidth = len(row.Value)
		}
	}
	if valWidth > 60 {
		valWidth = 60
	}

	border := fmt.Sprintf("+-%s-+-%s-+",
		strings.Repeat("-", keyWidth),
		strings.Repeat("-", valWidth),
	)

	fmt.Println(border)
	fmt.Printf("| %-*s | %-*s |\n", keyWidth, "key", valWidth, "value")
	fmt.Println(border)

	for _, row := range r.Rows {
		val := row.Value
		if len(val) > valWidth {
			val = val[:valWidth-3] + "..."
		}
		fmt.Printf("| %-*s | %-*s |\n", keyWidth, row.Key, valWidth, val)
	}

	fmt.Println(border)
	fmt.Printf("%d row(s)\n\n", len(r.Rows))
	fmt.Printf("%d row(s) (%.3f ms)\n\n", len(r.Rows), float64(r.Duration.Microseconds())/1000.0)

}

func printHelp() {
	fmt.Println(`
COMMANDS
  help, \h          show this help
  exit, \q          quit
  clear, \c         clear current input buffer

SQL
  CREATE TABLE <name>;
  DROP TABLE <name>;
  INSERT INTO <table> (key, value) VALUES ('<key>', '<value>');
  SELECT * FROM <table>;
  SELECT * FROM <table> WHERE key = '<key>';
  SELECT * FROM <table> WHERE key >= '<key>';
  SELECT * FROM <table> WHERE key > '<key>';
  SELECT * FROM <table> WHERE key <= '<key>';
  SELECT * FROM <table> WHERE key < '<key>';
  DELETE FROM <table> WHERE key = '<key>';

NOTES
  - statements must end with ;
  - multi-line input supported (keep typing until ;)
  - keys are lexicographically ordered
`)
}
