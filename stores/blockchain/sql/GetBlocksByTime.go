package sql

import (
	"context"
	"fmt"
	"time"
)

func (s *SQL) GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error) {

	q := `
    SELECT hash FROM blocks
	WHERE inserted_at >='%s' AND inserted_at <= '%s'
    `
	// var Hash []byte
	fromTimeString := fromTime.Format("2006-01-02 15:04:05 +0000")
	toTimeString := toTime.Format("2006-01-02 15:04:05 +0000")

	formattedQuery := fmt.Sprintf(q, fromTimeString, toTimeString)

	rows, err := s.db.QueryContext(ctx, formattedQuery)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	// blocks := make([]*model.Block, 0)
	hashes := make([][]byte, 0)
	for rows.Next() {
		// block := &model.Block{}
		hash := []byte{}
		err := rows.Scan(
			&hash,
		)
		if err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			return nil, err
		}
		hashes = append(hashes, hash)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return hashes, nil
}
