namespace mcactmodule
{
    using System;
    using System.IO;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    // using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Newtonsoft.Json;

    public class ConnectionManager
    {
        static string sourceFile = "";
        static string destinationFile = "";
        

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        public static async Task Init()
        {
            // Open a connection to the Edge runtime
            // MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            // ITransportSettings[] settings = { mqttSetting };
            // AmqpTransportSettings amqpSetting = new AmqpTransportSettings(Microsoft.Azure.Devices.Client.TransportType.Amqp_Tcp_Only);
            AmqpTransportSettings amqpSetting = new AmqpTransportSettings(TransportType.Amqp_Tcp_Only);
            ITransportSettings[] settings = { amqpSetting };

            ModuleClient ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Console.WriteLine("\n\nIoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient);
        }

        public static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            // PipeMessage 시작 시간
            DateTime startactdt = DateTime.Now;
            string strstartdt = startactdt.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

            ModuleClient moduleClient = (ModuleClient)userContext;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }
            
            // input1 데이터 받아오기
            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);

            // ML에서 들어온 데이터에 붙은 쓰레기값 제거
            messageString = messageString.Replace("[\"","");
            messageString = messageString.Replace("\"]","");
            messageString = messageString.Replace("\\","");
            Console.WriteLine("\n\nReceive\n" + messageString);

            // JSON To .net 객체 변환
            MessageBody messageBody = null;
            try
            {
                messageBody = JsonConvert.DeserializeObject<MessageBody>(messageString);
            }
            catch(Exception e)
            {
                string errorMessage = e.ToString();
                ErrorLogManager.LogWrite("Deserialize Error\n" + errorMessage);
                ErrorLogManager.LogWrite("Error Receive Message\n" + messageString);
            }
            
            //PLC로 결과 전송하는 코드
            string stempMessage = messageBody.r;
            if(stempMessage != "0" && stempMessage != "1") 
            {
                stempMessage = "2";
            }
            PLCManager.PLCMessage(stempMessage, messageBody.bc); //결과값 전달
            PLCManager.MethodThread(); //전송

            DateTime dt1 = DateTime.Now;
            TimeSpan ts1 = dt1 - startactdt;
            Console.WriteLine("메시지 정리 시간 : " + ts1);
            
            if (!string.IsNullOrEmpty(messageString))
            {

                // 데이터를 Insert하기 전 DateTime 형식으로 변환
                string[] result = null;
                string dtfull = null;
                string dt = null;
                try
                {
                    result = messageBody.dtfull.Split(new char[] { '-' });
                    dtfull = result[0] + "-" +  result[1] + "-" + result[2] + " " + result[3] + ":" + result[4] + ":" + result[5];
                    dt = result[0] + "-" +  result[1] + "-" + result[2];
                }
                // dtfull 값 오류일 경우 현재 시간을 넣어 처리한다.
                catch(Exception e)
                {
                    string errorMessage = e.ToString();
                    ErrorLogManager.LogWrite("DTFULL Error\n" + errorMessage);
                    dtfull = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
                    dt = DateTime.Now.ToString("yyyy-MM-dd");
                }

                string bc = "Null Value";
                if(messageBody.bc != "")
                {
                    bc = messageBody.bc;
                }

                try
                {
                    // 바코드, 라인정보, 날짜시간, 날짜, ML 버전, 결과, 양불 확률값, error 메시지, 판정 소요시간, 양불 판정 기준값
                    SQLManager.InsertSQLTable(dtfull, bc, messageBody.lid, dt, messageBody.v, messageBody.r, messageBody.prob, messageBody.error, messageBody.etime, messageBody.cutoff, messageBody.FTUR_NM_SET, messageBody.FTUR_VAL_SET, messageBody.TRHD_NM_SET, messageBody.TRHD_VAL_SET, messageBody.ML_R, messageBody.RULE_R, messageBody.TEST_NG_R, messageBody.FTUR_ENRG, messageBody.FTUR_WVFM_STDDEV, messageBody.FTUR_TRGER, messageBody.FTUR_ENRG_TRHD, messageBody.FTUR_WVFM_STDDEV_TRHD, messageBody.FTUR_TRGER_TRHD);
                }
                catch(Exception e)
                {
                    string errorMessage = e.ToString();
                    ErrorLogManager.LogWrite("Insert Command Error\n" + errorMessage);
                }
                DateTime dt2 = DateTime.Now;
                TimeSpan ts2 = dt2 - startactdt;
                Console.WriteLine("report 처리 시간 : " + ts2);

                //성은씨가 추가한 코드
                //File을 Temp 디렉토리로 이동
                try
                {
                    destinationFile = "";

                    // 들어온 path를 sourceFile에 저장
                    sourceFile = messageBody.path;

                    // 경로 부분 추출
                    string [] arr = sourceFile.Split("/");
                    string sourcePath = "/" + arr[1] + "/" + arr[2] + "/" + arr[3] + "/" + arr[4];
                    arr[arr.Length-2] = "Raw";

                    // 옮겨줄 경로를 만들어줌
                    foreach(string str in arr)
                    {
                        destinationFile += str + "/";
                    }
                    destinationFile = destinationFile.Substring(0,destinationFile.Length-1);
                    string destinationPath = "/" + arr[1] + "/" + arr[2] + "/" + arr[3] + "/" + arr[4];
                    string fileName = arr[5];

                    // 현재 파일 이름 나누기
                    string [] nowfile = fileName.Split("_");

                    // 현재 파일 이름의 시간 나누기
                    string [] ntime = nowfile[2].Split("-");

                    // 현재 파일 시간 저장
                    int nowtime = Int32.Parse(ntime[3] + ntime[4] + ntime[5].TrimEnd('5', 'h', '.'));

                    // 현재 들어온 데이터 이전 시간 파일 옮기기        
                    DirectoryInfo di = new System.IO.DirectoryInfo(sourcePath);

                    // RawData 폴더에 있는 파일을 모두 검색
                    foreach (System.IO.FileInfo File in di.GetFiles())
                    {

                        // 파일 이름 가져오기
                        string FileNameOnly = File.Name.Substring(0, File.Name.Length - 3);
                        Console.WriteLine("검색되는 파일명(Foreach FileNameOnly) : " + FileNameOnly);

                        // 파일 이름 나누기
                        string [] prearr = FileNameOnly.Split("_");

                        // 파일 이름의 시간 나누기
                        string [] ptime = prearr[2].Split("-");

                        // 파일 시간 저장
                        int pretime = Int32.Parse(ptime[3] + ptime[4] + ptime[5]);

                        // 검색한 파일의 경로와 옮겨줄 경로 저장
                        string PresourceFile = sourcePath + "/" + File.Name;
                        string PredestinationFile = destinationPath + "/" + File.Name;

                        // 현재 파일 시간과 방금 추출한 파일 시간을 비교하여 현재 파일 시간이 더 크면 실행
                        if(nowtime>pretime)
                        {
                            Console.WriteLine("nowTime : " + nowtime);
                            Console.WriteLine("preTime : " + pretime);
                            try
                            {
                                System.IO.File.Move(PresourceFile, PredestinationFile);
                                Console.WriteLine("이전 파일 PresourceFile : " + PresourceFile); //조준연 임시
                                Console.WriteLine("이전 파일 PredestinationFile : " + PredestinationFile);//조준연 임시
                            }

                            // 이전데이터에 대한 중복처리
                            catch(Exception e)
                            {
                                string errorMessage = e.ToString();
                                Console.WriteLine("PRE File Move Error\n" + errorMessage);
                                if(System.IO.File.Exists(PredestinationFile))
                                {
                                    bool Same_File_Check = true;
                                    int AddSameFileNum = 1;
                                    while(Same_File_Check)
                                    {
                                        try
                                        {

                                            // 중복판정된 파일 이름을 가져와 (n)을 추가하여 줍니다.
                                            // 반복하여 중복이 없을때까지 실행합니다.
                                            string SameFile = PredestinationFile.Substring(0,PredestinationFile.Length-3) + $"({AddSameFileNum}).h5";
                                            Console.WriteLine(AddSameFileNum);
                                            System.IO.File.Move(PresourceFile, SameFile);
                                            Console.WriteLine("Same File Move : " + SameFile);

                                            // 파일 이동이 완료되면 반복문을 빠져 나옵니다.
                                            Same_File_Check = false;
                                        }
                                        catch (Exception)
                                        {
                                            AddSameFileNum = AddSameFileNum + 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // 현재 들어온 데이터 파일 옮기기
                    try
                    {

                        // 파일을 확인
                        //if(File.Exists(sourceFile) && IsAccessAble(sourceFile))
                        if(File.Exists(sourceFile))
                        {
                            File.Move(sourceFile, destinationFile);
                            Console.WriteLine("현재 파일 sourceFile : " + sourceFile); //조준연 임시
                            Console.WriteLine("현재 파일 destinationFile : " + destinationFile);//조준연 임시
                        }
                    }
                    catch (Exception e)
                    {
                        string errorMessage = e.ToString();
                        ErrorLogManager.LogWrite("File Move Error\n" + errorMessage);
                        
                        // 파일을 옮길 폴더에 동일 파일이 존재하면 (N) 파일 이름에 추가하여 옮김
                        if(File.Exists(destinationFile))
                        {
                            bool Same_File_Check = true;
                            int AddSameFileNum = 1;
                            while(Same_File_Check)
                            {
                                try
                                {
                                    string SameFile = destinationFile.Substring(0,destinationFile.Length-3) + $"({AddSameFileNum}).h5";
                                    Console.WriteLine(AddSameFileNum);
                                    File.Move(sourceFile, SameFile);
                                    Same_File_Check = false;
                                }
                                catch (Exception)
                                {
                                    AddSameFileNum = AddSameFileNum + 1;
                                }
                            }
                        }
                    }
                }

                // 잘못된 파일 이름이 들어왔을 경우 (BC 또는 DTFULL 값의 오류)
                catch (Exception e)
                {
                    string errorMessage = e.ToString();
                    ErrorLogManager.LogWrite("SelectPath Command Error\n" + errorMessage);
                    string [] arr = sourceFile.Split("/");

                    // 잘못된 파일 이름 출력
                    ErrorLogManager.LogWrite("Path error file name : " + arr[5]);

                    // 파일 확인
                    if(File.Exists(sourceFile))
                    {

                        // 파일 이동
                        try
                        {
                            File.Move(sourceFile, destinationFile);
                            ErrorLogManager.LogWrite("error file move");
                        }
                        
                        // 중복 검사
                        catch(Exception ee)
                        {
                            errorMessage = ee.ToString();
                            ErrorLogManager.LogWrite("File Move Error\n" + errorMessage);
                            if(File.Exists(destinationFile))
                            {
                                bool Same_File_Check = true;
                                int AddSameFileNum = 1;
                                while(Same_File_Check)
                                {
                                    try
                                    {
                                        string SameFile = destinationFile.Substring(0,destinationFile.Length-3) + $"({AddSameFileNum}).h5";
                                        Console.WriteLine(AddSameFileNum);
                                        File.Move(sourceFile, SameFile);
                                        Same_File_Check = false;
                                    }
                                    catch (Exception)
                                    {
                                        AddSameFileNum = AddSameFileNum + 1;
                                    }
                                }
                            }
                        }
                    }
                }
                DateTime dt3 = DateTime.Now;
                TimeSpan ts3 = dt3 - startactdt;
                Console.WriteLine("파일 옮기는 시간 : " + ts3);

                // 결과 값 내보내기
                // .net 객체 to JSON 변환
                messageString = "task end";
                messageBytes = Encoding.UTF8.GetBytes(messageString);
                var pipeMessage = new Message(messageBytes);
                pipeMessage.ContentEncoding = "utf-8";
                pipeMessage.ContentType = "application/json";
                await moduleClient.SendEventAsync(pipeMessage);
                DateTime dt4 = DateTime.Now;
                TimeSpan ts4 = dt4 - startactdt;
                Console.WriteLine("메시지 처리 시간 : " + ts4);

                // 통합 모듈의 러닝시간 확인을 위한 마지막 시간
                DateTime endactdt = DateTime.Now;
                string strenddt = endactdt.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
                
                // TestTime 저장
                try
                {
                    SQLManager.InsertSQLTimeTable(messageBody.chtime, messageBody.mltime, strstartdt, strenddt, messageBody.etime_ch, messageBody.etime_load, messageBody.etime_prep, messageBody.etime, messageBody.bc, dtfull);
                }
                catch(Exception e)
                {
                    string errorMessage = e.ToString();
                    ErrorLogManager.LogWrite("Test Time Insert Command Error\n" + errorMessage);
                }
                DateTime dt5 = DateTime.Now;
                TimeSpan ts5 = dt5 - startactdt;
                Console.WriteLine("testtime 처리 시간 : " + ts5);
            }
            return MessageResponse.Completed;
        }

        private static void MethodThread()
        {
            throw new NotImplementedException();
        }

        private static void PLCMessage(string stempMessage, string bc)
        {
            throw new NotImplementedException();
        }

        public static bool IsAccessAble(String path)
        {
            FileStream fs = null;
            try
            {
                fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                return false;
            }
            finally
            {
                if (fs != null)
                {
                    fs.Close();
                }
            }
            return true;
        }
    }
}