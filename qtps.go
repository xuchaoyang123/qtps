package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/mattn/go-oci8"
	"log"
	"os"
	"time"
	"io/ioutil"
)

var (
	//mysql 变量
	QPS1      int
	QPS2      int
	TPS1      int
	TPS2      int
	QPS_Totol int
	TPS_Totol int
	Varlues   string

	//Oracle 变量
	QPS  float32
	TPS  float32
	MBPS float32

	FormatTimes = time.Now().Format("2006-01-02") //定义备份的文件显示的日期格式
	ListQps  = make([]int, 0) //提供收集统计最大QPS
	ListTps  = make([]int, 0) //提供收集统计最大TPS

	//oracle 有小数点
	OListQps  = make([]float32, 0) //提供收集统计最大QPS
	OListTps  = make([]float32, 0) //提供收集统计最大TPS
	OListMbps  = make([]float32, 0) //提供收集统计最大MPS
)

func main() {


	README()


	//获取参数值
	dbtype, host, username, password, port, db, Interval := GetValues()

	//判断当前数据库类型
	if dbtype == "mysql" || dbtype == "MYSQL" {
		 Mysql(username, password, host, port, db, Interval)
	
	} else if dbtype == "oracle" || dbtype == "ORACLE" {
		Oracl1(username, password, host, port, db, Interval )

	}else if dbtype == "sqlserver" || dbtype == "SQLSERVER" ||  dbtype == "MSSQL" || dbtype == "mssql"   {
		
		Sqlserver(username, password, host, port, db, Interval )

	} else {
		fmt.Println("error: Execute - h for query option, please confirm whether the parameter input is correct...")
	}
	

}

func Mysql(username, password, host, port, db string,Interval int)  {

	//将数据转换成数据库url作为返回值
	conn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local", username, password, host, port, db)
	open, err := sql.Open("mysql", conn)
	if err != nil {
		log.Printf("open database error:%v", err)
	}
	defer open.Close()
	if err != nil {
		log.Println(err)
	}

	//创建日志文件
	file, err := os.OpenFile("./"+FormatTimes+"_Mysql_qtps.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("File open failed: ", err)
	}
	defer file.Close()
	file.WriteString("date,QPS,TPS\n")


	//查询这个参数是否开启,如果开启不做操作,没有开启进行自动开启。
	// show variables like '%show_compatibility_56%';
	//set global show_compatibility_56=on;

	fmt.Println("		------------------------------")
	fmt.Println("		| Mysql：QPS,TPS monitor v2.0 |")
	fmt.Println("		------------------------------")
	fmt.Println("")
	fmt.Printf("         %s      | %s |%s |\n", "date", "QPS", "TPS")

	for i := 0; i < Interval; i++ {
		//一秒前的数据
		Qps1, err := open.Query(" select sum(VARIABLE_VALUE) QPS_Totol from  information_schema.GLOBAL_STATUS where VARIABLE_NAME in (?,?,?,?)", "com_select", "com_insert", "com_delete", "com_update")
		Tps1, err := open.Query(" select sum(VARIABLE_VALUE) TPS_Totol from  information_schema.GLOBAL_STATUS where VARIABLE_NAME in (?,?,?,?,?)", "Com_commit", "Com_rollback", "com_insert", "com_delete", "com_update")

		if err != nil {
			log.Fatal(err)
		}

		for Qps1.Next() {
			if err := Qps1.Scan(&QPS_Totol); err != nil {
				log.Fatal(err)
			}
			QPS1 = QPS_Totol
		}

		for Tps1.Next() {
			if err := Tps1.Scan(&TPS_Totol); err != nil {
				log.Fatal(err)
			}
			TPS1 = TPS_Totol

		}

		//停顿1秒
		time.Sleep(time.Second * 1)

		//一秒后的数据
		Qps2, err := open.Query(" select sum(VARIABLE_VALUE) QPS_Totol from  information_schema.GLOBAL_STATUS where VARIABLE_NAME in (?,?,?,?)", "com_select", "com_insert", "com_delete", "com_update")
		Tps2, err := open.Query(" select sum(VARIABLE_VALUE) TPS_Totol from  information_schema.GLOBAL_STATUS where VARIABLE_NAME in (?,?,?,?,?)", "Com_commit", "Com_rollback", "com_insert", "com_delete", "com_update")
		if err != nil {
			log.Fatal(err)
		}

		for Qps2.Next() {
			if err := Qps2.Scan(&QPS_Totol); err != nil {
				log.Fatal(err)
			}
			QPS2 = QPS_Totol
		}

		for Tps2.Next() {
			if err := Tps2.Scan(&TPS_Totol); err != nil {
				log.Fatal(err)
			}
			TPS2 = TPS_Totol
		}
		log.Println("|", QPS2-QPS1-2, "|", TPS2-TPS1, "|")
		
		ListQps = append(ListQps, QPS2-QPS1-2)
		ListTps = append(ListTps, TPS2-TPS1)

		//写入日志
		Nows := time.Now().Format("2006/1/2 15:04:05")
		sprintf := fmt.Sprintf("%v,%v,%v", Nows, QPS2-QPS1-2, TPS2-TPS1)
		file.WriteString(sprintf + "\n")

	}
		//排序算法来排序QPS哪个最大
		for i := 0; i < len(ListQps)-1; i++ {
			for j := i + 1; j < len(ListQps); j++ {
				if ListQps[j] > ListQps[i] {
					ListQps[i], ListQps[j] = ListQps[j], ListQps[i]
				}
			}
		}

			//排序算法来排序TPS哪个最大
			for i := 0; i < len(ListTps)-1; i++ {
				for j := i + 1; j < len(ListTps); j++ {
					if ListTps[j] > ListTps[i] {
						ListTps[i], ListTps[j] = ListTps[j], ListTps[i]
					}
				}
			}

	fmt.Println()
	fmt.Printf(" 	MAX QPS values: %v  MAX TPS values: %v \n",ListQps[0],ListTps[0])

}

func Oracl1(username, password, host, ports, dbs string,Interval int) {
	ORA_conn := fmt.Sprintf("%s/%s@%s:%s/%s", username, password, host, ports, dbs)
	db, err := sql.Open("oci8", ORA_conn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	

	//创建日志文件
	file1, err := os.OpenFile("./"+FormatTimes+"_ORA_qtps.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("文件打开失败: ", err)
	}
	defer file1.Close()
	file1.WriteString("date,QPS,TPS,MBPS\n")

	fmt.Println("		------------------------------------")
	fmt.Println("		| Oracle：QPS,TPS,MBPS monitor v2.0 |")
	fmt.Println("		------------------------------------")
	fmt.Println("")
	fmt.Printf("         %s       | %s |%s |%s |\n", "date", "QPS", "TPS", "MBPS")

	for i := 0; i < Interval; i++ {
		Ora01, err := db.Query("select round((select sum(value)  from gv$sysmetric where metric_name='I/O Requests per Second'),1)as qps,round((select sum(value) from gv$sysmetric where metric_name='User Transaction Per Sec'),1) as tps,round((select sum(value) from gv$sysmetric where metric_name='I/O Megabytes per Second'),1) as mbps from dual")
		if err != nil {
			log.Fatal(err)
		}

		for Ora01.Next() {

			Ora01.Scan(&QPS, &TPS, &MBPS)
		}

		Ora01.Close()
		log.Println("|", QPS, "|", TPS, "|", MBPS, "|")

		//将每次获取的数据存入列表中
		OListQps = append(OListQps, QPS)
		OListTps = append(OListTps, TPS)
		OListMbps= append(OListMbps, MBPS)

		//写入日志
		Nows := time.Now().Format("2006/1/2 15:04:05")
		sprintf := fmt.Sprintf("%v,%v,%v,%v", Nows, QPS, TPS, MBPS)
		file1.WriteString(sprintf + "\n")

		//停顿1秒
		time.Sleep(time.Second * 1)

	}

	//排序算法来排序QPS哪个最大
	for i := 0; i < len(OListQps)-1; i++ {
		for j := i + 1; j < len(OListQps); j++ {
			if OListQps[j] > OListQps[i] {
				OListQps[i], OListQps[j] = OListQps[j], OListQps[i]
			}
		}
	}

		//排序算法来排序TPS哪个最大
		for i := 0; i < len(OListTps)-1; i++ {
			for j := i + 1; j < len(OListTps); j++ {
				if OListTps[j] > OListTps[i] {
					OListTps[i], OListTps[j] = OListTps[j], OListTps[i]
				}
			}
		}

			//排序算法来排序MBPS哪个最大
			for i := 0; i < len(OListMbps)-1; i++ {
				for j := i + 1; j < len(OListMbps); j++ {
					if OListMbps[j] > OListMbps[i] {
						OListMbps[i], OListMbps[j] = OListMbps[j], OListMbps[i]
					}
				}
			}


fmt.Println()
fmt.Printf(" 	MAX QPS values: %v  MAX TPS values: %v   MAX MBPS values: %v \n",OListQps[0],OListTps[0],OListMbps[0])


}


func Sqlserver(username, password, host, port, db string,Interval int)  {

	var QPS3 int
	var QPS4 int
	var TPS3 int
	var TPS4 int


	//连接字符串
	connString := fmt.Sprintf("server=%s;port%d;database=%s;user id=%s;password=%s", host, port, db, username, password)
	//建立连接
	conn, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Open Connection failed:", err.Error())
	}
	defer conn.Close()

	//创建日志文件
	file, err := os.OpenFile("./"+FormatTimes+"_SQL_qtps.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("File open failed: ", err)
	}
	defer file.Close()

	file.WriteString("date,QPS,TPS\n")

	fmt.Println("		----------------------------------")
	fmt.Println("		| Sqlserver：QPS,TPS monitor v2.0 |")
	fmt.Println("		----------------------------------")
	fmt.Println("")
	fmt.Printf("         %s       | %s |%s |\n", "date", "QPS", "TPS")

for i := 0; i < Interval; i++ {
   //通过连接对象执行查询
rows, err := conn.Query(`select (select sum(cntr_value) QPS3 from sys.dm_os_performance_counters where  ltrim(rtrim(instance_name))
not in ('master','model','msdb','tempdb','mssqlsystemresource','_Total')
and rtrim(counter_name) in ('Batch Requests/sec')) QPS3 ,(select sum(cntr_value) TPS3 from sys.dm_os_performance_counters
where  ltrim(rtrim(instance_name)) not in ('master','model','msdb','tempdb','mssqlsystemresource','_Total')
and rtrim(counter_name) in ('Transactions/sec') ) TPS3 `)



   if err != nil {
      log.Fatal("Query failed:", err.Error())
   }
   defer rows.Close()


   for rows.Next() {
	  rows.Scan(&QPS3,&TPS3)
	  
	}
	//fmt.Println("1#####",TPS3,IOPS3)

	

		//停顿1秒
		time.Sleep(time.Second * 1)


	   //通过连接对象执行查询
	   rows1, err := conn.Query(`select (select sum(cntr_value) QPS4 from sys.dm_os_performance_counters where  ltrim(rtrim(instance_name))
	   not in ('master','model','msdb','tempdb','mssqlsystemresource','_Total')
	   and rtrim(counter_name) in ('Batch Requests/sec')) QPS4 ,(select sum(cntr_value) TPS4 from sys.dm_os_performance_counters
	   where  ltrim(rtrim(instance_name)) not in ('master','model','msdb','tempdb','mssqlsystemresource','_Total')
	   and rtrim(counter_name) in ('Transactions/sec') ) TPS4 `)

   if err != nil {
      log.Fatal("Query failed:", err.Error())
   }
   defer rows1.Close()


   for rows1.Next() {
	rows1.Scan(&QPS4,&TPS4)
}
	//fmt.Println("2#####",QPS4,TPS4)


log.Println("|",QPS4-QPS3,"|",TPS4-TPS3,"|" )

ListQps = append(ListQps, QPS4-QPS3)
ListTps = append(ListTps, TPS4-TPS3)

//写入日志
Nows := time.Now().Format("2006/1/2 15:04:05")
sprintf := fmt.Sprintf("%v,%v,%v", Nows,QPS4-QPS3,TPS4-TPS3)
file.WriteString(sprintf + "\n")

}

		//排序算法来排序QPS哪个最大
		for i := 0; i < len(ListQps)-1; i++ {
			for j := i + 1; j < len(ListQps); j++ {
				if ListQps[j] > ListQps[i] {
					ListQps[i], ListQps[j] = ListQps[j], ListQps[i]
				}
			}
		}

			//排序算法来排序TPS哪个最大
			for i := 0; i < len(ListTps)-1; i++ {
				for j := i + 1; j < len(ListTps); j++ {
					if ListTps[j] > ListTps[i] {
						ListTps[i], ListTps[j] = ListTps[j], ListTps[i]
					}
				}
			}

	fmt.Println()
	fmt.Printf(" 	MAX QPS values: %v  MAX TPS values: %v \n",ListQps[0],ListTps[0])

}



//定义人工输入参数
func GetValues() (dbtype, host, username, password, port, db string,Interval int) {

	flag.StringVar(&dbtype, "dbtype", "", "* Database support type (mysql,oracle,sqlserver)")
	flag.StringVar(&host, "host", "127.0.0.1", "* Database address")
	flag.StringVar(&username, "user", "root", "* database username")
	flag.StringVar(&password, "pass", "", "* Database password [nill]")
	flag.StringVar(&port, "port", "3306", "Database port")
	flag.StringVar(&db, "instance", "", "Specify the instance name or database name: (Mysql is db, Oracle is an instance, Sqlserver is an instance)")
	flag.IntVar(&Interval, "interval", 99999999, "Data acquisition times: once every 1 second")

	//解析胡获取参数
	flag.Parse() //解析一下
	return dbtype, host, username, password, port, db,Interval

}

//软件使用介绍
func README(){
	
		dataStr := `
1. Software introduction: 
This tool mainly realizes monitoring the QPS information of the DB to detect the pressure of the database.


2. Instructions:


	example Mysql:

	qtps.exe -dbtype mysql  -host 127.0.0.1 -user monitor -pass monitor -port 3306 -instance mysql -interval 5


	If there is Error 3167: The 'INFORMATION_SCHEMA.GLOBAL_STATUS' feature is disabled; see the documentation for 'show_compatibility_56'

	Solution:
	Execute the command with root authority 【set global show_compatibility_56=on】



	example oracle:

	qtps.exe -dbtype oracle -host  127.0.0.1 -user monitor -pass monitor  -port 1521  -instance ORCL   -interval 5



	example sqlserver:

	qtps.exe -dbtype sqlserver -host  127.0.0.1 -user monitor -pass monitor  -interval 10

        
		   `

	 //字符串转为字节类型
    dataBytes := []byte(dataStr)

    err := ioutil.WriteFile("./README.md", dataBytes, 0666)
    if err != nil {
        fmt.Println("An error has occurred: ", err)
    } 
		

}
