package models

type Student struct {
	Id           int64   `json:"id"`
	Name         string  `json:"name"`
	Age          int64   `json:"age"`
	AverageScore float64 `json:"average_score"`
}
