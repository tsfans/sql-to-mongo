package converter

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/tsfans/sql_to_mongo/parser"
)

var (
	TableSchema = map[string][]*parser.Column{
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
		"homework": {
			{Type: parser.Int, Name: "id"},
			{Type: parser.Datetime, Name: "createdAt"},
			{Type: parser.Datetime, Name: "updatedAt"},
			{Type: parser.Int, Name: "stuId"},
			{Type: parser.String, Name: "subject"},
			{Type: parser.Float, Name: "score"},
		},
		"employee": {
			{Type: parser.Int, Name: "id"},
			{Type: parser.Datetime, Name: "createdAt"},
			{Type: parser.Datetime, Name: "updatedAt"},
			{Type: parser.String, Name: "name"},
			{Type: parser.String, Name: "jobnumber"},
			{Type: parser.String, Name: "department"},
			{Type: parser.String, Name: "center"},
			{Type: parser.String, Name: "area"},
		},
		"signin": {
			{Type: parser.Int, Name: "id"},
			{Type: parser.Datetime, Name: "createdAt"},
			{Type: parser.Datetime, Name: "updatedAt"},
			{Type: parser.String, Name: "type"},
			{Type: parser.Int, Name: "employeeId"},
			{Type: parser.Int, Name: "locationId"},
		},
		"checkin": {
			{Type: parser.Int, Name: "id"},
			{Type: parser.Datetime, Name: "createdAt"},
			{Type: parser.Datetime, Name: "updatedAt"},
			{Type: parser.String, Name: "type"},
			{Type: parser.Int, Name: "employeeId"},
			{Type: parser.Int, Name: "locationId"},
		},
		"location": {
			{Type: parser.Int, Name: "id"},
			{Type: parser.Datetime, Name: "createdAt"},
			{Type: parser.Datetime, Name: "updatedAt"},
			{Type: parser.String, Name: "area"},
			{Type: parser.String, Name: "city"},
			{Type: parser.String, Name: "province"},
		},
	}

	Source_View = map[string]string{
		"student":  "student",
		"homework": "homework",
		"employee": "employee",
		"signin":   "signin",
		"checkin":  "checkin",
		"location": "location",
	}
	SELECT  = "select count(1) as stu_cnt from student where entryDate >= '2023-11-11' and (`foreign`= 0 or age > 0) group by grade"
	SELECT2 = "select id,name from student where age > 18"
	SELECT3 = "select " +
		"s.grade as grade," +
		"h.subject as subject," +
		"count(distinct s.stuId) as dis_cnt_stuId," +
		"sum(h.score) as sum_score," +
		"avg(h.score) as avg_score " +
		"from student s left join homework h on s.id = h.stuId " +
		"where s.`foreign` = false and (s.id > 10 or s.age > 18 ) " +
		"group by s.grade,h.subject"
	SELECT4 = "select " +
		"grade," +
		"subject," +
		"dis_cnt_stuId as stuId_dis_cnt," +
		"sum_score as score_sum," +
		"avg_score as score_avg " +
		"from (" +
		"select " +
		"s.grade as grade," +
		"h.subject as subject," +
		"count(distinct s.stuId) as dis_cnt_stuId," +
		"sum(h.score) as sum_score," +
		"avg(h.score) as avg_score " +
		"from student s left join homework h on s.id = h.stuId " +
		// "where s.`foreign` = false and (s.id > 10 or s.age > 18 ) " +
		"group by s.grade,h.subject" +
		") a"
	SELECT5 = "select a.id as a_id,b.name as b_name from student a left join student b on a.id=b.id where a.age > 18"
	SELECT6 = "select b.id as id,b.name as name from (select * from student a where a.age > 18) b"
	SELECT7 = "select count(1) as total from student where entryDate >= '2023-11-11' and `foreign`= 0"
	SELECT8 = "select 'test',123 value,year(createdAt) as createdYear,* from student where entryDate >= '2023-11-11' and `foreign`=0"
	SELECT9 = "select " +
		"e.id as employeeId," +
		"e.name as employeeName," +
		"s.type as signinType," +
		"count(s.id) as signinCount " +
		"from employee e left join signin s on e.id = s.employeeId " +
		"where date_format(s.createdAt,'%Y-%m-%d') = '2023-12-12' " +
		"group by e.id,e.name,s.type"
	SELECT10 = "select * from (select * from student where entryDate >= '2023-11-11' and `foreign`= 0) a"
	SELECT11 = "select * from (select id,name,age from student where entryDate >= '2023-11-11' and `foreign`= 0) a"
	SELECT12 = "select s1.*,s2.* from (select id as s_id,name as s_name,age as s_age from student) s1 left join student s2 on s1.s_id = s2.id"
	SELECT13 = "select year(createdAt) as createdYear,* from student"
	SELECT14 = "select * from employee e left join signin s on e.id = s.employeeId"
	SELECT15 = "select createdYear,id,name from (select year(createdAt) as createdYear, * from employee) a"
	SELECT16 = "select " +
		"a.id as employeeId," +
		"a.name as employeeName," +
		"a.type as signinType," +
		"a.createdYear as entryYear," +
		"c.type as checkinType " +
		"from (select e.id as id,e.name as name,s.type as type,year(e.createdAt) as createdYear from employee e left join signin s on e.id = s.employeeId) a " +
		"left join checkin c on a.id = c.employeeId limit 0,100"
	SELECT17 = "select " +
		"a.id as employeeId," +
		"a.name as employeeName," +
		"a.type as signinType," +
		"a.createdYear as entryYear," +
		"c.type as checkinType " +
		"from checkin c left join " +
		"(select e.id as id,e.name as name,s.type as type,year(e.createdAt) as createdYear " +
		"from employee e left join signin s on s.employeeId = e.id) a " +
		"on a.id = c.employeeId " +
		"limit 0,100"
	SELECT18 = "select " +
		"b.employeeId as employeeId," +
		"b.employeeName as employeeName," +
		"b.signinType as signinType," +
		"b.checkinType as checkinType," +
		"b.entryYear as entryYear," +
		"d.department as department," +
		"d.center as center," +
		"d.area as area from " +
		"employee d " +
		"left join " +
		"(select " +
		"a.id as employeeId," +
		"a.name as employeeName," +
		"a.type as signinType," +
		"c.type as checkinType, " +
		"a.createdYear as entryYear " +
		"from checkin c left join " +
		"(select e.id as id,e.name as name,s.type as type,year(e.createdAt) as createdYear " +
		"from employee e left join signin s on s.employeeId = e.id) a " +
		"on a.id = c.employeeId) b " +
		"on d.id = b.employeeId " +
		"limit 0,100"
	SELECT19 = "select " +
		"e.id as employeeId," +
		"e.name as employeeName," +
		"e.department as employeeDepartment," +
		"e.area as employeeArea," +
		"e.center as employeeCenter," +
		"a.att as att," +
		"a.signinType as signinType," +
		"a.location as signinLocation," +
		"a.signinTime as signinTime " +
		"from employee e left join " +
		"(select " +
		"'签到' as att," +
		"s.createdAt as signinTime," +
		"s.employeeId as employeeId," +
		"s.type as signinType," +
		"l.city as location " +
		"from signin s left join location l on s.locationId = l.id" +
		") a " +
		"on e.id = a.employeeId " +
		"limit 0,100"
	SELECT20 = "select " +
		"e.id as employeeId," +
		"e.name as employeeName," +
		"a.signinType as signinType," +
		"max(a.signinTime) as latestSigninTime " +
		"from employee e left join " +
		"(select " +
		"'签到' as att," +
		"s.createdAt as signinTime," +
		"s.employeeId as employeeId," +
		"s.type as signinType," +
		"l.city as location " +
		"from signin s left join location l on s.locationId = l.id" +
		") a " +
		"on e.id = a.employeeId " +
		"group by e.id,e.name,a.signinType " +
		"limit 0,100"
	SELECT21 = "select id/id as id,name,1.1 as c,id/1.1 as id_1,id*10 as id_2,id+10 as id_3,id-10 as id_4 from employee e"
	SELECT22 = "select avg(id) as id from employee e"
	SELECT23 = "select id,name,date_format(e.createdAt,'%Y') as createdYear from employee e"
	SELECT24 = "select " +
		"name as employeeName," +
		"sum(id) as id_sum," +
		"sum(id+id)as id_plus_sum," +
		"count(distinct id) as id_dis_cnt," +
		"round(sum(id+id)/(count(distinct id)+2*3.5), 1) as id_avg " +
		"from employee " +
		"group by employeeName " +
		"having id_sum >= 0 " +
		"order by id_sum desc "
	SELECT25        = "select distinct name from employee"
	SELECT26        = "select sum(distinct id) as res from employee"
	SELECT27        = "select avg(distinct id) as res from employee"
	SELECT28        = "select max(distinct id) as res from employee"
	SELECT29        = "select min(distinct id) as res from employee"
	SELECT30        = "select sum(distinct id)+1 as res from employee"
	SELECT31        = "select count(distinct name)/2+1/2 as name_cnt from employee"
	SELECT32        = "select sum(id) as id_sum,year(createdAt) as createdYear from employee group by createdYear having id_sum > 0 order by id_sum"
	SELECT_LIST     = []string{SELECT2}
	SELECT_LIST_ALL = []string{
		SELECT,
		SELECT2,
		SELECT3,
		SELECT4,
		SELECT5,
		SELECT6,
		SELECT7,
		SELECT8,
		SELECT9,
		SELECT10,
		SELECT11,
		SELECT12,
		SELECT13,
		SELECT14,
		SELECT15,
		SELECT16,
		SELECT17,
		SELECT18,
		SELECT19,
		SELECT20,
		SELECT21,
		SELECT22,
		SELECT23,
		SELECT24,
		SELECT25,
		SELECT26,
		SELECT27,
		SELECT28,
		SELECT29,
		SELECT30,
		SELECT31,
		SELECT32,
	}
)

func TestMongoQueryConverter(t *testing.T) {
	for _, sql := range SELECT_LIST_ALL {
		sqlParser := parser.NewMySQLSelectParser(sql, TableSchema)
		sql, err := sqlParser.Parse()
		if err != nil {
			t.Errorf("parse failed,err=%v", err.Error())
			return
		}
		conv := NewMongoQueryConverter(sql, Source_View)
		var query Query
		query, err = conv.Convert()
		if err != nil {
			t.Errorf("convert failed,err=%v", err.Error())
			return
		}
		mongoQuery := query.(*MongoQuery)
		fmt.Println(mongoQuery.StartView)
		selectFields, _ := json.Marshal(mongoQuery.SelectFields)
		fmt.Println(string(selectFields))
		pipeline, _ := json.Marshal(mongoQuery.Pipeline)
		fmt.Println(string(pipeline))
	}
}
