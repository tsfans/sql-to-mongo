# sql-to-mongo

Convert SQL statement to mongo statement.

- Compatible with MySQL: use [tidb sql parser](https://github.com/pingcap/tidb/tree/master/pkg/parser) to parse SQL.
- Convert SQL **SELECT statement** to mongo query statement.
- Precheck before convert: will check SQL semantic, does collection existed, does columns existed and so on.

## Get Started

### Prerequisites

- go version >= 1.21.4

### Example

```shell
  go get github.com/tsfans/sql-to-mongo@v1.0.4
```

```go
  package main

  import (
 "encoding/json"
 "fmt"

 "github.com/tsfans/sql-to-mongo/converter"
 "github.com/tsfans/sql-to-mongo/parser"
)

func main() {
 // define table schema so we can validate columns
 tableSchema := map[string][]*parser.Column{
  "student": {
   {Type: parser.Int, Name: "id"},
   {Type: parser.Datetime, Name: "createdAt"},
   {Type: parser.Datetime, Name: "updatedAt"},
   {Type: parser.String, Name: "stuId"},
   {Type: parser.String, Name: "name"},
   {Type: parser.Int, Name: "age"},
   {Type: parser.String, Name: "gender"},
   {Type: parser.Boolean, Name: "foreign"},
   {Type: parser.Datetime, Name: "entryDate"},
   {Type: parser.String, Name: "grade"},
   {Type: parser.Int, Name: "class"},
  },
 }

 selectSql := "select id, name from student where age > 18"
 // create new parser to parse SQL
 sqlParser := parser.NewMySQLSelectParser(selectSql, tableSchema)
 // parse SQL to AST, and validate sementic
 sql, err := sqlParser.Parse()
 if err != nil {
  panic(err)
 }

 // define view so we can validate mongo query statement
 sourceView := map[string]string{"student": "student"}
 // create new converter to convert SQL to mongo statement
 conv := converter.NewMongoQueryConverter(sql, sourceView)
 var query converter.Query
 // convert SQL to mongo statement
 query, err = conv.Convert()
 if err != nil {
  panic(err)
 }

 mongoQuery := query.(*converter.MongoQuery)

 // StartView use to start mongo aggregate
 fmt.Println(mongoQuery.StartView)

 // SelectFields represent final result fields
 selectFields, _ := json.Marshal(mongoQuery.SelectFields)
 fmt.Println(string(selectFields))

 // Pipeline use to execute mongo aggregate
 // this example will print [{"$match":{"$expr":{"$gt":["$age",18]}}},{"$replaceRoot":{"newRoot":{"id":"$id","name":"$name"}}}]
 // you can use it on mongo: db.student.aggregate([{"$match":{"$expr":{"$gt":["$age",18]}}},{"$replaceRoot":{"newRoot":{"id":"$id","name":"$name"}}}])
 pipeline, _ := json.Marshal(mongoQuery.Pipeline)
 fmt.Println(string(pipeline))
}
```

more usage see [converter_test](https://github.com/tsfans/sql-to-mongo/blob/main/converter/converter_test.go)

## Support SQL syntax

Right now only support **SELECT statement**, The specific content is as follows:
| Syntax    | Comment |
| --------- | --------- |
| SELECT ||
| LEFT JOIN ||
| GROUP BY ||
| ORDER BY ||
| LIMIT ||
| + ||
| - ||
| * ||
| / ||
| SUM ||
| AVG ||
| COUNT ||
| MAX ||
| MIN ||
| YEAR ||
| MONTH ||
| DAY ||
| HOUR ||
| MINUTE ||
| SECOND ||
| DATE_FORMAT ||
| ROUND ||
| IF ||
| CASE WHEN ||
| NOW() ||
| TO_DOUBLE ||
| FLOOR ||
| SUBSTRING_INDEX |SUBSTRING_INDEX(model_file_key, '/', 3), the third arg `3` means array index(0, 1, 2, 3)|

## Future

- Support More SQL Functions.
