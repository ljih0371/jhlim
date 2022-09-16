namespace mcactmodule
{
    using System;
    using MySql.Data.MySqlClient;
    using System.IO;
    using YamlDotNet.Serialization;
    using System.Collections.Generic;

    public class SQLManager
    {
        // 통합 Test용 DB info
        static string dbname;
        static string tbname1;
        static string tbname2;
        static string ip;
        static string uid;
        static string pwd;
        static string connectionstring;

        // DB연결이 안될 경우 또는 데이터가 이상하여 넣지 못한 데이터를 파일에 모아 두기 위한 파일 위치 경로
        static string ExceptionData = $"/home/data/ExceptionData.txt";
        static string StrDateTime = DateTime.Now.ToString("yyyyMMdd");
        static string ProblematicData = $"/home/data/logfile/Actmodule/ProblematicData_" + StrDateTime + ".txt";
        static string open = "Open";

        // SQL 정보를 가져옴
        public static void SQLConfig()
        {
            var d = new Deserializer();
            var result = d.Deserialize<Dictionary<string, YamlReader.YamlConfig>>(new StreamReader($"/home/data/edge_config.yml"));
            foreach (var a in result.Values)
            {
                dbname = a.server.dbname;
                tbname1 = a.server.tbname.report;
                tbname2 = a.server.tbname.time;
                ip = a.server.ip;
                uid = a.server.uid;
                pwd = a.server.pwd;
            }
            connectionstring = $"Server = {ip}; Database = {dbname}; Uid = {uid}; pwd = {pwd}";
        }

        // Maria DB 테이블 생성
        public static void CreateSQLTable()
        {
            string keyoptions = "DTFULL DATETIME NOT NULL, BC VARCHAR(45) NOT NULL, LID INT, DT DATE, V VARCHAR(30), R INT, PROB FLOAT, ERROR TEXT(21844), ETIME FLOAT, CUTOFF FLOAT, FTUR_NM_SET TEXT(21844), FTUR_VAL_SET TEXT(21844), TRHD_NM_SET TEXT(21844), TRHD_VAL_SET TEXT(21844), ML_R INT, RULE_R INT, TEST_NG_R INT, FTUR_ENRG FLOAT, FTUR_WVFM_STDDEV FLOAT, FTUR_TRGER INT, FTUR_ENRG_TRHD FLOAT, FTUR_WVFM_STDDEV_TRHD FLOAT, FTUR_TRGER_TRHD INT, PRIMARY KEY(BC, DTFULL)";
            //string keyoptions = "DTFULL DATETIME NOT NULL, BC VARCHAR(45) NOT NULL, LID INT, DT DATE, V VARCHAR(30), R INT, PROB FLOAT, ERROR TEXT(21844), ETIME FLOAT, CUTOFF FLOAT, FTUR_NM_SET TEXT(21844), FTUR_VAL_SET TEXT(21844), TRHD_NM_SET TEXT(21844), TRHD_VAL_SET TEXT(21844), PRIMARY KEY(BC, DTFULL)";
            string CreateTableNameInSQLstring = $"CREATE TABLE IF NOT EXISTS {dbname}.{tbname1} ({keyoptions}) ENGINE = InnoDB CHARSET=utf8;";

            string keyoptions2 = "CHTIME VARCHAR(100), MLTIME VARCHAR(100), ACTTIME VARCHAR(100), ACTETIME VARCHAR(100), ETIME_CH VARCHAR(100), ETIME_LOAD VARCHAR(100), ETIME_PREP VARCHAR(100), ETIME VARCHAR(100), BC VARCHAR(45), DTFULL DATETIME";
            string CreateTableNameInSQLstring2 = $"CREATE TABLE IF NOT EXISTS {dbname}.{tbname2} ({keyoptions2}) ENGINE = InnoDB CHARSET=utf8;";
            try
            {
                
                using (MySqlConnection connection = new MySqlConnection(connectionstring))
                {
                    connection.Open();

                    // 분석 데이터 저장 테이블
                    using (MySqlCommand command = new MySqlCommand(CreateTableNameInSQLstring, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    // 시간 데이터 저장 테이블 (테스트용)
                    using (MySqlCommand command = new MySqlCommand(CreateTableNameInSQLstring2, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    connection.Close();
                }
            }
            catch (Exception e)
            {
                string errorMessage = e.ToString();
                ErrorLogManager.LogWrite("Table Create SQL Error\n" + errorMessage);
            }
        }
        
        // 일반적인 데이터 삽입
        public static void InsertSQLTable(string DTFULL, string BC, string LID, string DT, string V, string R, string PROB, string ERROR, string ETIME, string CUTOFF, string FTUR_NM_SET, string FTUR_VAL_SET, string TRHD_NM_SET, string TRHD_VAL_SET, string ML_R, string RULE_R, string TEST_NG_R, string FTUR_ENRG, string FTUR_WVFM_STDDEV, string FTUR_TRGER, string FTUR_ENRG_TRHD, string FTUR_WVFM_STDDEV_TRHD, string FTUR_TRGER_TRHD)
        {
            string InsertRawDataToSQLstring = $"INSERT INTO {dbname}.{tbname1} (DTFULL, BC, LID, DT, V, R, PROB, ERROR, ETIME, CUTOFF, FTUR_NM_SET, FTUR_VAL_SET, TRHD_NM_SET, TRHD_VAL_SET, ML_R, RULE_R, TEST_NG_R, FTUR_ENRG, FTUR_WVFM_STDDEV, FTUR_TRGER, FTUR_ENRG_TRHD, FTUR_WVFM_STDDEV_TRHD, FTUR_TRGER_TRHD) VALUES(\"{DTFULL}\", \"{BC}\", \"{LID}\", \"{DT}\", \"{V}\", \"{R}\", \"{PROB}\", \"{ERROR}\", \"{ETIME}\", \"{CUTOFF}\", \"{FTUR_NM_SET}\", \"{FTUR_VAL_SET}\", \"{TRHD_NM_SET}\", \"{TRHD_VAL_SET}\", \"{ML_R}\", \"{RULE_R}\", \"{TEST_NG_R}\", \"{FTUR_ENRG}\", \"{FTUR_WVFM_STDDEV}\", \"{FTUR_TRGER}\", \"{FTUR_ENRG_TRHD}\", \"{FTUR_WVFM_STDDEV_TRHD}\", \"{FTUR_TRGER_TRHD}\");";
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionstring))
                {
                    try
                    {
                        connection.Open();
                        open = "Open";
                    }
                    catch
                    {
                        open = "Close";
                    }
                    Console.WriteLine("SQL Status : " + open);

                    // DB 연결 여부 확인
                    if (open.Equals("Open"))
                    {

                        // 실시간 데이터 삽입
                        using (MySqlCommand command = new MySqlCommand(InsertRawDataToSQLstring, connection))
                        {
                            command.ExecuteNonQuery();
                        }

                        // 파일 존재 여부 확인
                        if (File.Exists(ExceptionData))
                        {

                            // 파일 한 줄 씩 읽기
                            string[] Unconnected = File.ReadAllLines(ExceptionData);
                            Console.WriteLine("Insert SQL ExceptionData File");
                            foreach (string str in Unconnected)
                            {

                                // 콤마(,)로 구분되어 있는 데이터를 나눠줌
                                string[] result = str.Split(new char[] { ',' });
                                string InsertunconnectedDataToSQLstring = $"INSERT INTO {dbname}.{tbname1} (DTFULL, BC, LID, DT, V, R, PROB, ERROR, ETIME, CUTOFF, FTUR_NM_SET, FTUR_VAL_SET, TRHD_NM_SET, TRHD_VAL_SET, ML_R, RULE_R, TEST_NG_R, FTUR_ENRG, FTUR_WVFM_STDDEV, FTUR_TRGER, FTUR_ENRG_TRHD, FTUR_WVFM_STDDEV_TRHD, FTUR_TRGER_TRHD) VALUES(\"{result[0]}\", \"{result[1]}\", \"{result[2]}\", \"{result[3]}\", \"{result[4]}\", \"{result[5]}\", \"{result[6]}\", \"{result[7]}\", \"{result[8]}\", \"{result[9]}\", \"{result[10]}\", \"{result[11]}\", \"{result[12]}\", \"{result[13]}\", \"{result[14]}\", \"{result[15]}\", \"{result[16]}\", \"{result[17]}\", \"{result[18]}\", \"{result[19]}\", \"{result[20]}\", \"{result[21]}\", \"{result[22]}\");";
                                using (MySqlCommand command = new MySqlCommand(InsertunconnectedDataToSQLstring, connection))
                                {
                                    command.ExecuteNonQuery();
                                }
                            }
                            connection.Close();

                            // 처리가 끝난 데이터 삭제
                            File.Delete(ExceptionData);
                            Console.WriteLine("Delete ExceptionData File");
                        }
                    }

                    // Status가 Close일 경우 파일로 저장
                    else
                    {
                        ErrorLogManager.LogWrite("Not Connected SQL Error\n");
                        string str = DTFULL + ',' + BC + ',' + LID + ',' + DT + ',' + V + ',' + R + ',' + PROB + ',' + ERROR + ',' + ETIME + ',' + CUTOFF + ',' + FTUR_NM_SET + ',' + FTUR_VAL_SET + ',' + TRHD_NM_SET + ',' + TRHD_VAL_SET + ',' + ML_R + ',' + RULE_R + ',' + TEST_NG_R + ',' + FTUR_ENRG + ',' + FTUR_WVFM_STDDEV + ',' + FTUR_TRGER + ',' + FTUR_ENRG_TRHD + ',' + FTUR_WVFM_STDDEV_TRHD + ',' + FTUR_TRGER_TRHD;
                        ErrorLogManager.LogWrite("Create ExceptionData File\n" + str);

                        // 파일 열기
                        using (StreamWriter w = File.AppendText(ExceptionData))
                        {
                            w.WriteLine(str);
                        }
                    }
                }
            }

            // Status가 Open이지만 Insert 실패할 경우 파일로 저장
            catch (Exception e)
            {
                string errorMessage = e.ToString();
                ErrorLogManager.LogWrite("Insert SQL Error\n" + errorMessage);
                string str = DTFULL + ',' + BC + ',' + LID + ',' + DT + ',' + V + ',' + R + ',' + PROB + ',' + ERROR + ',' + ETIME + ',' + CUTOFF + ',' + FTUR_NM_SET + ',' + FTUR_VAL_SET + ',' + TRHD_NM_SET + ',' + TRHD_VAL_SET + ',' + ML_R + ',' + RULE_R + ',' + TEST_NG_R + ',' + FTUR_ENRG + ',' + FTUR_WVFM_STDDEV + ',' + FTUR_TRGER + ',' + FTUR_ENRG_TRHD + ',' + FTUR_WVFM_STDDEV_TRHD + ',' + FTUR_TRGER_TRHD;
                ErrorLogManager.LogWrite("Create ProblematicData File\n" + str);

                // 파일 열기
                using (StreamWriter w = File.AppendText(ProblematicData))
                {
                    w.WriteLine(str);
                }
            }
        }
        
        // testtime Insert
        public static void InsertSQLTimeTable(string CHTIME, string MLTIME, string ACTTIME, string ACTETIME, string ETIME_CH, string ETIME_LOAD, string ETIME_PREP, string ETIME, string BC, string DTFULL)
        {
            string InsertRawDataToSQLstring = $"INSERT INTO {dbname}.{tbname2} (CHTIME, MLTIME, ACTTIME, ACTETIME, ETIME_CH, ETIME_LOAD, ETIME_PREP, ETIME, BC, DTFULL) VALUES (\"{CHTIME}\", \"{MLTIME}\", \"{ACTTIME}\", \"{ACTETIME}\", \"{ETIME_CH}\", \"{ETIME_LOAD}\", \"{ETIME_PREP}\", \"{ETIME}\", \"{BC}\", \"{DTFULL}\");";
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionstring))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(InsertRawDataToSQLstring, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    connection.Close();
                }
            }
            catch (Exception e)
            {
                string errorMessage = e.ToString();
                ErrorLogManager.LogWrite("Insert SQL Test Time Error\n" + errorMessage);
            }
        }
    }
}