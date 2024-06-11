package belajar_golang_gorm

import (
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strconv"
	"testing"
)

func OpenConnection() *gorm.DB {
	dialect := mysql.Open("root:@tcp(localhost:3306)/belajar_golang_gorm?charset=utf8&parseTime=True&loc=Local")
	db, err := gorm.Open(
		dialect, &gorm.Config{
			Logger: logger.Default.LogMode(logger.Info),
		},
	)
	if err != nil {
		panic("Failed to create a connection to database")
	}

	return db
}

var db = OpenConnection()

func TestOpenConnection(t *testing.T) {
	assert.NotNil(t, db)
}

func TestExecuteSQL(t *testing.T) {
	err := db.Exec("INSERT INTO sample(id, name) values(? , ?)", 1, "Golang").Error
	assert.Nil(t, err)

	err = db.Exec("INSERT INTO sample(id, name) values(? , ?)", 2, "Python").Error
	assert.Nil(t, err)

	err = db.Exec("INSERT INTO sample(id, name) values(? , ?)", 3, "Java").Error
	assert.Nil(t, err)
}

type Sample struct {
	ID   string
	Name string
}

func TestRawSQL(t *testing.T) {
	var sample Sample

	err := db.Raw("SELECT id, name FROM sample WHERE id = ?", 1).Scan(&sample).Error
	assert.Nil(t, err)
	assert.Equal(t, "Golang", sample.Name)

	var samples []Sample
	err = db.Raw("SELECT id, name FROM sample").Scan(&samples).Error
	assert.Nil(t, err)
	assert.Len(t, samples, 3)
}

func TestSqlRow(t *testing.T) {
	rows, err := db.Raw("SELECT id, name FROM sample").Rows()
	assert.Nil(t, err)
	defer rows.Close()

	var samples []Sample
	for rows.Next() {
		var id string
		var name string

		err = rows.Scan(&id, &name)
		assert.Nil(t, err)

		samples = append(
			samples, Sample{
				ID:   id,
				Name: name,
			},
		)
	}
	assert.Len(t, samples, 3)
}

func TestScanRow(t *testing.T) {
	rows, err := db.Raw("SELECT id, name FROM sample").Rows()
	assert.Nil(t, err)

	var samples []Sample
	for rows.Next() {
		err = db.ScanRows(rows, &samples)
		assert.Nil(t, err)
	}
	assert.Len(t, samples, 3)
}

func TestCreateUser(t *testing.T) {
	user := User{
		ID:       "1",
		Password: "rahasia",
		Name: Name{
			FirstName:  "Aditya",
			MiddleName: "Jago",
			LastName:   "Prasetyo",
		},
		Information: "ini akan di ignore",
	}

	response := db.Create(&user)
	assert.Nil(t, response.Error)
	assert.Equal(t, int64(1), response.RowsAffected)
}

func TestBatchInsert(t *testing.T) {
	var users []User
	for i := 2; i < 10; i++ {
		users = append(
			users, User{
				ID:       strconv.Itoa(i),
				Password: "rahasia",
				Name: Name{
					FirstName: "User" + strconv.Itoa(i),
				},
			},
		)
	}

	response := db.Create(&users)
	assert.Nil(t, response.Error)
	assert.Equal(t, int64(8), response.RowsAffected)
}

func TestTransactionSuccess(t *testing.T) {
	err := db.Transaction(
		func(tx *gorm.DB) error {
			err := tx.Create(
				&User{
					ID:       "10",
					Password: "rahasia",
					Name: Name{
						FirstName: "User 10",
					},
				},
			).Error
			err = tx.Create(
				&User{
					ID:       "11",
					Password: "rahasia",
					Name: Name{
						FirstName: "User 11",
					},
				},
			).Error
			err = tx.Create(
				&User{
					ID:       "12",
					Password: "rahasia",
					Name: Name{
						FirstName: "User 12",
					},
				},
			).Error
			if err != nil {
				return err
			}
			return nil
		},
	)

	assert.Nil(t, err)
}

func TestTransactionError(t *testing.T) {
	err := db.Transaction(
		func(tx *gorm.DB) error {
			err := tx.Create(
				&User{
					ID:       "14",
					Password: "rahasia",
					Name: Name{
						FirstName: "User 13",
					},
				},
			).Error
			if err != nil {
				return err
			}

			return nil
		},
	)
	err = db.Transaction(
		func(tx *gorm.DB) error {
			err := tx.Create(
				&User{
					ID:       "12",
					Password: "rahasia",
					Name: Name{
						FirstName: "User 12",
					},
				},
			).Error
			if err != nil {
				return err
			}

			return nil
		},
	)

	assert.NotNil(t, err)
}

func TestManualTransactionSuccess(t *testing.T) {
	tx := db.Begin()
	defer tx.Rollback()

	err := tx.Create(
		&User{
			ID:       "15",
			Password: "rahasia",
			Name: Name{
				FirstName: "User 15",
			},
		},
	).Error
	assert.Nil(t, err)

	err = tx.Create(
		&User{
			ID:       "16",
			Password: "rahasia",
			Name: Name{
				FirstName: "User 16",
			},
		},
	).Error
	assert.Nil(t, err)

	if err == nil {
		tx.Commit()
	}
}

func TestManualTransactionError(t *testing.T) {
	tx := db.Begin()
	defer tx.Rollback()

	err := tx.Create(
		&User{
			ID:       "17",
			Password: "rahasia",
			Name: Name{
				FirstName: "User 17",
			},
		},
	).Error
	assert.Nil(t, err)

	err = tx.Create(
		&User{
			ID:       "16",
			Password: "rahasia",
			Name: Name{
				FirstName: "User 16",
			},
		},
	).Error
	assert.NotNil(t, err)

	if err == nil {
		tx.Commit()
	}
}
