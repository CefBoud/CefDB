package parser

import (
	"fmt"
	"testing"

	"github.com/CefBoud/CefDB/record"
	"github.com/stretchr/testify/assert"
)

func TestParser(t *testing.T) {
	p := NewParser()
	queryString := "SelecT ToTo from MATABLE where LoLo = 'EE' AND 123 = Titi"
	expectedReconstructedQuery := "SELECT toto FROM matable WHERE lolo = 'EE' AND 123 = titi"
	qd, err := p.Query(queryString)
	assert.NoError(t, err, "Select parsing failed")
	assert.Equal(t, expectedReconstructedQuery, qd.String())

	insertString := `INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)
					 VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');`
	insertData, err := p.Insert(insertString)
	expectedInsertData := &InsertData{Table: "customers",
		Fields: []string{"customername", "contactname", "address", "city", "postalcode", "country"},
		Values: []interface{}{"'Cardinal'", "'Tom B. Erichsen'", "'Skagen 21'", "'Stavanger'", "'4006'", "'Norway'"}}

	assert.Equal(t, expectedInsertData, insertData)

	updateString := `UPDATE Customers SET ContactName = 'Alfred Schmidt' WHERE CustomerID = 1;`
	updateData, err := p.Modify(updateString)
	expectedUpdateString := "UpdateData{table: <customers>, field: <contactname>, expression: <'Alfred Schmidt'>, predicate: <customerid = 1>}"
	assert.Equal(t, expectedUpdateString, updateData.String())

	deleteString := `DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';`
	deleteData, err := p.Delete(deleteString)
	expectedDeleteString := "DeleteData{table: <customers>, predicate: <customername = 'Alfreds Futterkiste'>}"
	assert.Equal(t, expectedDeleteString, deleteData.String())

	createTableString := `CREATE TABLE Persons (
    PersonID int,
    LastName varchar(255),
    FirstName varchar(255),
    Address varchar(255),
    City varchar(255)
		);`
	createTableData, err := p.CreateTable(createTableString)

	expected := CreateTableData{
		Table: "persons",
		Schema: record.NewSchemaWithFields(map[string]record.FieldInfo{
			"address":   {Type: record.VARCHAR, Length: 255},
			"city":      {Type: record.VARCHAR, Length: 255},
			"firstname": {Type: record.VARCHAR, Length: 255},
			"lastname":  {Type: record.VARCHAR, Length: 255},
			"personid":  {Type: record.INTEGER, Length: 0},
		}),
	}
	fmt.Printf("createTableData %#v %v err %v", createTableData, createTableData.Schema.Fields, err)
	assert.Equal(t, expected.Table, createTableData.Table)
	assert.Equal(t, expected.Schema, createTableData.Schema)
}
