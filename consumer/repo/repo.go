package repo

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"kafka_project/models"
)

type RepoConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBname   string
}

type Processor interface {
	Process(d *models.TDocument) (*models.TDocument, error)
}
type Repo struct {
	conn *pgx.Conn
}

func NewDocumentProcessor(c *pgx.Conn) *Repo {
	return &Repo{
		conn: c,
	}
}

func ConnectToDatabase(config RepoConfig) *pgx.Conn {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", config.User, config.Password, config.Host, config.Port, config.DBname)

	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		log.Error("ERROR connection database:", err)
	}

	err = conn.Ping(context.Background())
	if err != nil {
		log.Error("ERROR ping DB:", err)
	}
	return conn
}

func (repo *Repo) CloseConnection(ctx context.Context) {
	err := repo.conn.Close(ctx)
	if err != nil {
		// Handle error
		log.Error("ERROR close connection database")
	}
}

func (p *Repo) Process(d *models.TDocument) (*models.TDocument, error) {

	ctx := context.Background()

	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback(ctx)

	var existingDoc models.TDocument

	err = tx.QueryRow(ctx, "SELECT url, pubdate, fetchtime, text, firstfetchtime FROM test_db WHERE url = $1", d.Url).Scan(
		&existingDoc.Url, &existingDoc.PubDate, &existingDoc.FetchTime, &existingDoc.Text, &existingDoc.FirstFetchTime)

	switch {

	case err == pgx.ErrNoRows:
		// Если документа еще нет, добавляем его
		d.FirstFetchTime = d.FetchTime
		_, err = tx.Exec(ctx, "INSERT INTO test_db (url, pubdate, fetchtime, text, firstfetchtime) VALUES ($1, $2, $3, $4, $5)",
			d.Url, d.PubDate, d.FetchTime, d.Text, d.FirstFetchTime)
		if err != nil {
			log.Error("Error INSERT INTO TABLE: ", err)
			return nil, err
		}

	case err != nil:
		if err != nil {
			log.Error("Error SELECT FROM TABLE: ", err)
			return nil, err
		}
		return nil, err

	default:
		if d.FetchTime == existingDoc.FetchTime && d.Url == existingDoc.Url {
			return nil, nil
		}
		//Обновляем поля
		if d.FetchTime > existingDoc.FetchTime {
			existingDoc.Text = d.Text
			existingDoc.FetchTime = d.FetchTime
		}
		if d.FetchTime < existingDoc.FirstFetchTime {
			existingDoc.FirstFetchTime = d.FetchTime
			existingDoc.PubDate = d.PubDate
		}
		if d.FetchTime < existingDoc.PubDate {
			existingDoc.PubDate = d.PubDate
		}

		_, err = tx.Exec(ctx, "UPDATE test_db SET pubdate = $1, fetchtime = $2, text = $3, firstfetchtime = $4 WHERE url = $5",
			existingDoc.PubDate, existingDoc.FetchTime, existingDoc.Text, existingDoc.FirstFetchTime, existingDoc.Url)
		if err != nil {
			log.Error("Error UPDATE TABLE: ", err)
			return nil, err
		}

	}
	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return &existingDoc, nil
}
func (p *Repo) ReadValues(ctx context.Context) ([]string, error) {

	var vals []string

	query := "SELECT value FROM test_db"
	rows, err := p.conn.Query(ctx, query)
	if err != nil {
		return []string{}, err
	}
	for rows.Next() {
		var value string
		err := rows.Scan(&value)
		if err != nil {
			return []string{}, err
		}
		vals = append(vals, value)
	}

	return vals, nil
}
