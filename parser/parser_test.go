package parser

import (
	"testing"
)

var (
	SELECT  = "select count(1) from student where entryDate >= '2023-11-11' and `foreign`=0 group by grade"
	SELECT2 = "select s1.id,name,class from (select id,name,grade from student) s1 left join (select id,class from student) s2 on s1.id = s2.id"
	SELECT3 = "select " +
		"grade,subject,sum_score,avg_score from " +
		"(select " +
		"grade, " +
		"json_extract(homework_12894917934,'$.subject') as subject, " +
		"sum(json_extract(homework_12894917934,'$.score')) as sum_score, " +
		"avg(json_extract(homework_12894917934,'$.score')) as avg_score " +
		"from student group by grade,json_extract(homework_12894917934,'$.subject')) s1"
	SELECT4 = "select * from student"

	SELECT_LIST = []string{SELECT, SELECT2, SELECT3}

	TableSchema = map[string][]*Column{
		"student": {
			{Type: Int, Name: "id"},
			{Type: Datetime, Name: "createdAt"},
			{Type: Datetime, Name: "updatedAt"},
			{Type: String, Name: "stuId"},
			{Type: String, Name: "name"},
			{Type: Int, Name: "age"},
			{Type: String, Name: "gender"},
			{Type: Boolean, Name: "foreign"},
			{Type: Datetime, Name: "entryDate"},
			{Type: String, Name: "grade"},
			{Type: Int, Name: "class"},
			{Type: Json, Name: "homework_12894917934", Json: map[string]*Column{
				"id":        {Type: Int, Name: "id"},
				"createdAt": {Type: Datetime, Name: "createdAt"},
				"updatedAt": {Type: Datetime, Name: "updatedAt"},
				"subject":   {Type: String, Name: "subject"},
				"score":     {Type: Float, Name: "score"},
			}},
		},
	}
)

func TestParseMySQLSelect(t *testing.T) {
	for idx := range SELECT_LIST {
		sqlParser := NewMySQLSelectParser(SELECT_LIST[idx], TableSchema)
		_, err := sqlParser.Parse()
		if err != nil {
			t.Errorf("parse failed,err=%v", err.Error())
		}
	}
}
