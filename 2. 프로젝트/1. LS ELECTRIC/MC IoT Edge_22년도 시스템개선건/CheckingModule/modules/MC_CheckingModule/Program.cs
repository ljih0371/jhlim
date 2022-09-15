namespace MC_CheckingModule
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Newtonsoft.Json;
    using System.Linq;
    using System.Timers;
    using System.Diagnostics;
    using System.Collections.Generic;

    ///log 추가 Test 버전 

    class Program
    {
        private static DateTime maxFTime;
        private static string lastpath = "";
        private static int file_cnt = 0;
        private static int send_cnt = 0;
        private static System.Timers.Timer aTimer;
        private static object lockObject = new Object();

        //CountLines에서 사용하는 변수
        private const char CR = '\r';
        private const char LF = '\n';
        private const char NULL = (char)0;

        private static string slash;

        static void Main(string[] args)
        {
            Console.WriteLine(DateTime.Now);
            ModuleClient moduleclient = null;
            DirectoryManager dirmanager = null;
            //string OS = "testOnWindow";
            string OS = "productionOnLinux";

            //while문에 사용되는 flag
            bool flag = true;

            string logdate = DateTime.Now.ToString("yyyyMMdd");
            string dir_path = "";
            string tmp_path = "";
            string logpath = "";
            string logdir_path = "";
            //string [] files;
            //int c_file = 0;

            if (OS == "productionOnLinux") // IotEdge 모듈로 배포 할 때 사용(container 배포시)
            {
                dirmanager = new DirectoryManager(OS);
                slash = "/";
                DirectoryManager.IsNewDay();
                dir_path = DirectoryManager.dirpath;
                tmp_path = DirectoryManager.tmppath;
                Console.WriteLine("path: " + dir_path);
                logdir_path = "/home/data/logfile/CheckingModule/";
                logpath = logdir_path + $"CheckingModule_Log_{logdate}.txt";
                LogManager.logpath = logpath;
                moduleclient = ConnectionManager.Init().Result;
            }
            else if (OS == "testOnWindow") // 로컬PC에서 테스트 진행 시 
            {
                dirmanager = new DirectoryManager(OS);
                slash = "\\";
                DirectoryManager.IsNewDay();
                dir_path = DirectoryManager.dirpath;
                tmp_path = DirectoryManager.tmppath;
                Console.WriteLine(DirectoryManager.dirpath);
                logdir_path = "c:\\dd\\logfile\\CheckingModule\\";
                logpath = logdir_path + $"CheckingModule_Log_{logdate}.txt";
                LogManager.logpath = logpath;
            }

            //디렉토리가 없으면 생성
            if (!Directory.Exists(dir_path))
            {
                Directory.CreateDirectory(dir_path);
                Console.WriteLine(dir_path + " : Create Directory");
                lastpath = "";
            }

            if (!Directory.Exists(tmp_path))
            {
                Directory.CreateDirectory(tmp_path);
                Console.WriteLine(tmp_path + " : Create Directory");
                lastpath = "";
            }

            //Logfile 디렉토리에 존재하는 이전 로그파일 삭제
            if (Directory.Exists(logdir_path))
            {
                if(!File.Exists(logpath))
                {
                    // Log File 생성
                    FileStream stream = File.Create(logpath);
                    stream.Close();
                }
                
                string[] logfiles = Directory.GetFiles(logdir_path);
                foreach (string lf in logfiles)
                {
                    if (lf != logpath)
                    {
                        File.Delete(lf);
                    }
                }
            }

            //Today의 0시 0분 0초로 초기화
            maxFTime = DateTime.Today;
            Console.WriteLine("M. SetTimer Start......");
            SetTimer(flag, moduleclient);
            Console.WriteLine("M. SetTimer End........");


            // Wait until the app unloads or is cancelled
            Console.WriteLine("M. Wait until the app unloads or is cancelled-----start");
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            ConnectionManager.WhenCancelled(cts.Token).Wait();
            Console.WriteLine("M. Wait until the app unloads or is cancelled-----end");
        }

        /// <summary>
        /// 이름 : 디렉토리 검사
        ///  - '19.9.30 : 디렉토리 검사 시간 조정(0.1 -> 0.2)
        ///  - 1초마다 디렉토리 검사 시간 조정 10.14
        /// </summary>
        private static void SetTimer(bool flag, ModuleClient moduleclient)
        {
            // 디렉토리검사시간에 대한 최적의 시간을 조율중...
            // 0.1 -> 0.2(200)초마다 디렉토리 검사 시간 조정 9.30 
            // 0.2 -> 0.5초마다 디렉토리 검사 시간 조정 10.2 
            // 0.5 -> 1초마다 디렉토리 검사 시간 조정 10.14 
            aTimer = new System.Timers.Timer(1000);
            aTimer.Elapsed += (obj, e) => OnTimedEvent(obj, e, moduleclient);
            aTimer.AutoReset = true;
            aTimer.Enabled = true;

            //flag가 true인 동안 타이머 이벤트 반복
            while (flag != false) 
            {
            //22.06 소스추가 3
            //Sleep을 추가하여 점유율 안정성 확보
                Thread.Sleep(10);
            }

            if (flag == false)
            {
                aTimer.Stop();
                aTimer.Dispose();
            }
        }
        /// <summary>
        /// 타이머 이벤트
        /// </summary>
        private static void OnTimedEvent(object obj, ElapsedEventArgs e, ModuleClient moduleclient)
        {
            string path = DirectoryManager.dirpath;
            string tmppath = DirectoryManager.tmppath;
            DateTime startTime = DateTime.Now;

            //22.06 추가 코드 1-2
            if (aTimer.Interval > 100)
            {
                aTimer.Interval = 100;

            }

            //Date 변경 여부 확인
            //변경되었으면 새로운 일자의 디렉토리와 로그파일 생성 및 이전 로그파일 삭제
            if (DirectoryManager.IsNewDay())
            {
                path = DirectoryManager.dirpath;
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                    Console.WriteLine(path + " : Create Directory");
                }
                tmppath = DirectoryManager.tmppath;
                if (!Directory.Exists(tmppath))
                {
                    Directory.CreateDirectory(tmppath);
                    Console.WriteLine(tmppath + " : Create Directory");
                }
                file_cnt = 0;
                send_cnt = 0;
                lastpath = "";
                string logdir_path = "/home/data/logfile/CheckingModule/";
                string logdate = DateTime.Now.ToString("yyyyMMdd");
                string new_logpath = logdir_path + $"CheckingModule_Log_{logdate}.txt";
                LogManager.logpath = new_logpath;

                if (Directory.Exists(logdir_path))
                {
                    
                    if(!File.Exists(new_logpath))
                    {
                        // Log File 생성
                        FileStream stream = File.Create(new_logpath);
                        stream.Close();
                    }
                    string[] logfiles = Directory.GetFiles(logdir_path);
                    foreach (string lf in logfiles)
                    {
                        if (lf != new_logpath)
                        {
                            File.Delete(lf);
                        }
                    }
                }

                Console.WriteLine("New Working Day : " + DirectoryManager.directoryDay.ToString());
            }

            String[] files;
            files = Directory.GetFiles(path);
            int cnt_file = files.Length;

            Console.WriteLine("디렉토리 파일 개수[" + DateTime.Now.ToString("yyyy-MM-dd  HH:mm:ss.ffffff") + "] : " + cnt_file.ToString());
            if (cnt_file > 0)
            {
                // 10.14 새로운 매소드 호출
                //FindNew2(startTime, moduleclient);
                // 11.05 MS sample code 로 다시 정리함
                FindNew3(startTime, moduleclient);
            }
            return;
        }

        #region FindNew(미사용) 
        /// <summary> 
        /// FindNew(미사용) --> FindNew2 매소드를 신규 생성
        /// </summary>
        private static void FindNew(DateTime startTime, ModuleClient moduleclient)
        {
            aTimer.Enabled = false;
            Console.WriteLine("FindNew 실행 " + DateTime.Now.ToString("yyyy-MM-dd  HH:mm:ss.ffffff"));
            lock (lockObject)
            {


                Stopwatch sw = Stopwatch.StartNew();
                string path = DirectoryManager.dirpath;
                string tmppath = DirectoryManager.tmppath;
                DateTime min = DateTime.Now.AddDays(1);
                string newfile = "";

                var directory = new DirectoryInfo(path);
                FileInfo[] files;
                files = directory.GetFiles();

                // 새 파일 찾기
                foreach (FileInfo f in files)
                {
                    Console.WriteLine("foreach Start...............");
                    // Checking Module은 DataFile 을 직접 읽고 쓰지 않기 때문에 체크 불필요 - 19.10.2
                    //if(IsAccessAble(f.FullName) && f.LastWriteTime > maxFTime && f.LastWriteTime < min && f.Length > 0)
                    //Console.WriteLine("파일 저장 시간은 : "+ f.LastWriteTime.ToString()); //파일명이 아니고, 파일 생성된 시간임
                    //if(f.LastWriteTime > maxFTime && f.LastWriteTime < min && f.Length > 0) 
                    //Console.WriteLine("Find New File Size  :  "+ f.Length);
                    if (f.Length > 0) // 10.10 : f.Length = File Size 가 0byte 보다 크면 진행
                    {
                        min = f.LastWriteTime;
                        newfile = f.FullName;

                        Console.WriteLine("Length File Size Check...............");
                    }
                    Console.WriteLine("foreach End...............");
                }


                var swe1 = sw.Elapsed;


                //if(newfile == "" || String.Equals(newfile,lastpath))  // 10.14 : 파일위치가 빈값으로 확인 될때만 return 처리
                if (newfile.ToString() == "")
                {
                    //Console.WriteLine("newfile Check... : "+ newfile.ToString());
                    //Console.WriteLine("lastpath Check.. : "+ lastpath.ToString());
                    aTimer.Enabled = true;
                    return;
                }

                /*
                 * 2019년 하반기 JYTEK 확산 적용 - 불필요 코드 주석 처리
                 * HDF5로 파일형태 변경으로 인해 File Open 시 에러 발생 
                
                
                //파일의 라인 수 확인
                int flcnt = 0;
                FileStream fs = null;
                try
                {
                    fs = File.Open(newfile, FileMode.Open);
                    flcnt = CountFileLines(fs);                            
                }
                catch (IOException ex)
                {
                    Console.WriteLine($"[ERROR] - Program / File.Open - IOException : {ex.Message}");
                    Console.WriteLine($"\t{ex.ToString()}");
                }
                catch (System.Exception ex)
                {
                    Console.WriteLine($"[ERROR] - Program / File.Open - Unexpected Exception : {ex.Message}");
                    Console.WriteLine($"\t{ex.ToString()}");
                }
                finally
                {
                    if(fs != null)
                    {
                        fs.Close();
                    }
                }

                //flcnt 가 76800보다 적으면 Finenew 메서드 종료, Timer 재시작
                if(flcnt < 76800)
                {
                    aTimer.Enabled = true;
                    return;
                }
                */

                /*
                 * 설명: 처리된 파일인지 확인하여 IotEdge Hub 에 메시지 전송 
                 *       (chmodule -> Hub -> mlmodule)
                 * 변경사항 : 
                 *    - 10.2 : File 접근 가능 여부 확인 로직 주석 IsAccessAble(newfile) 
                 *             Checking Module은 DataFile 을 직접 읽고 쓰지 않기 때문에 체크 불필요
                 */
                //if(IsAccessAble(newfile) && !File.Exists(tmppath + newfile))
                if (!File.Exists(tmppath + newfile))
                {
                    Console.WriteLine("Found New File & Send Message");
                    //lastpath = newfile;
                    var swe2 = sw.Elapsed;
                    ++file_cnt;
                    SendMessage(newfile, startTime, moduleclient);
                    // lastpath = newfile;
                    // maxFTime = new FileInfo(lastpath).LastWriteTime;

                    var swe3 = sw.Elapsed;
                    DateTime endTime = DateTime.Now;
                    Console.WriteLine("----------------------------------------------------------");
                    Console.WriteLine("S1.elapsed time: " + (endTime - startTime));
                    Console.WriteLine("S2.check file line & lock & message : " + (swe2 - swe1));
                    Console.WriteLine("S3.Send Message : " + (swe3 - swe2));
                    Console.WriteLine("----------------------------------------------------------");
                }
                else
                {
                    Console.WriteLine("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                    //Console.WriteLine("Flcnt :: "+ flcnt + " :: IsAccessAble :: " + IsAccessAble(newfile) + " :: !File.Exists :: " + !File.Exists(tmppath + newfile) + " :: newfile!=lastpath :: " + !String.Equals(newfile,lastpath));
                    Console.WriteLine("File Not Exists :: " + !File.Exists(tmppath + newfile) + " :: newfile!=lastpath :: " + !String.Equals(newfile, lastpath));
                    Console.WriteLine("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                }
                aTimer.Enabled = true;
            }
        }

        /// <summary>
        /// 새로운 파일 찾기 
        ///    1. 파일 접근 가능 여부 확인, 
        ///    2. 보낸 이력에 따른 validation 처리
        /// </summary>
        private static void FindNew2(DateTime startTime, ModuleClient moduleclient)
        {
            aTimer.Enabled = false;
            //Console.WriteLine("===============================================FindNew2-start");
            Console.WriteLine("FindNew2 실행(10.17) :::: " + DateTime.Now.ToString("yyyy-MM-dd  HH:mm:ss.ffffff"));


            lock (lockObject)
            {
                Stopwatch sw = Stopwatch.StartNew();
                string path = DirectoryManager.dirpath;
                string tmppath = DirectoryManager.tmppath;
                DateTime min = DateTime.Now.AddDays(1);
                string newfile = "";
                string destFile = "";

                var dir = new DirectoryInfo(path);
                dir.Refresh();
                //Console.WriteLine($"####[{dir.GetFiles().Length}] GetFiles Count :::::::::::::: ");
                List<FileInfo> fileList = new List<FileInfo>();

                fileList = dir.GetFiles().OrderBy(f => f.LastWriteTime).ToList();

                // 로그 파일 read
                //Console.WriteLine("LogManager.logpath ::: "+ LogManager.logpath);
                StreamReader logfile = new StreamReader(LogManager.logpath);

                // 새 파일 찾기
                foreach (FileInfo f in fileList)
                {
                    /*************************************
                     * try ~ catch 주석 처리 10.16
                    try
                    {
                    **************************************/

                        var swe1 = sw.Elapsed;
                        // 1. validation Check (파일명, 파일사이즈, 직전의 파일명이 같은지 확인)
                        if (f.Length > 0 && f.FullName != "")
                        {
                            min = f.LastWriteTime;
                            newfile = f.FullName;
                            destFile = tmppath + slash + f.Name;

                            //ML로 전송한 메세지를 로그파일에서 확인
                            bool existSendlog = false;
                            //bool existDestFile = false;
                            //existDestFile = File.Exists(destFile); // A. 위에 설명 고려사항이 아닌것 같음 ..주석 처리해도됨
                            string logline = "";
                            // Log파일을 마지막 줄 가져오기

                            while ((logline = logfile.ReadLine()) != null)
                            {
                                if (logline.Substring(0, 5) == "send:")
                                {
                                    existSendlog = logline.Contains(f.Name); // B.
                                    if (existSendlog)
                                    {
                                        Console.WriteLine("existSendlog ::: " + existSendlog.ToString());
                                        break;
                                    }
                                }
                            }
                            logfile.Close();

                            /*  A. 목적지폴더에 전송할 파일이 이미 존재하는 경우
                                B. 로그파일을 검색해서 보낸 이력이 있는 경우
                                ===================================================
                                A	    B	    내용
                                ---------------------------------------------------
                                TRUE	TRUE	skip 처리
                                TRUE	FALSE	전송하면 actmodule에서 중복 파일 복사 처리가 이루어 짐(정상처리)
                                FALSE	TRUE	누군가가 일부로 지우지 않는 경우 발생안함(테스트 중에는 많이 발생할수 있음) 
                                                예외처리 : skip 처리
                                FALSE	FALSE	(정상처리) ML메시지 보내기
                                ---------------------------------------------------
                                -> 분석 결과 목적지폴더에 대한 중복처리는 고려사항이 아님
                                -> 로그파일 검색 이력 결과에 따른 ML전송 여부 결정
                             */
                            
                            if (!existSendlog)
                            {
                                Console.WriteLine("Found New File & Send Message-----------------------------");

                                //lastpath = newfile;
                                var swe2 = sw.Elapsed;
                                ++file_cnt;
                                SendMessage(newfile, startTime, moduleclient);
                                lastpath = newfile;
                                // maxFTime = new FileInfo(lastpath).LastWriteTime;

                                var swe3 = sw.Elapsed;
                                DateTime endTime = DateTime.Now;
                                Console.WriteLine("**********************************************************");
                                Console.WriteLine("S1.elapsed time: " + (endTime - startTime));
                                Console.WriteLine("S2.check file line & lock & message : " + (swe2 - swe1));
                                Console.WriteLine("S3.Send Message : " + (swe3 - swe2));
                                Console.WriteLine("**********************************************************");
                                //continue;
                                break;
                            }
                            else
                            {
                                Console.WriteLine("**********************************************************");
                                Console.WriteLine("Filename exists in Sendlog : " + f.Name);
                                Console.WriteLine("**********************************************************");
                                //continue;
                            }
                            Console.WriteLine("if existSendlog end==========================================");
                            continue;
                        }
                        else
                        {
                            //aTimer.Enabled = true;
                            Console.WriteLine("File not found or File read error....................");
                            break;
                        }
                        //Console.WriteLine("-------------------------------------------------send end");
                        //Thread.Sleep(1000); // 10.15 1초 쉬었다 가기
                    /**************************************************
                     * try ~ catch 주석 처리 10.16
                    }
                    catch (System.IO.FileNotFoundException e)
                    {
                        // then just continue.
                        Console.WriteLine(e.Message);
                        continue;
                    }
                    Console.WriteLine("----------------------------------------------foreach out");
                    *************************************************/
                }
                Console.WriteLine("lockObject end===============================================");
            }
            aTimer.Enabled = true;
            Console.WriteLine("-------------------------------------------------FindNew2-end");
            
        }

        #endregion
    

        private static void FindNew3(DateTime startTime, ModuleClient moduleclient)
        {
			
			aTimer.Enabled = false;
            Console.WriteLine("FindNew3 실행 :::: " + DateTime.Now.ToString("yyyy-MM-dd  HH:mm:ss.ffffff"));

			Stopwatch sw = Stopwatch.StartNew();
			string path = DirectoryManager.dirpath;
			string tmppath = DirectoryManager.tmppath;
			DateTime min = DateTime.Now.AddDays(1);
			string newfile = "";
			string destFile = "";
			
			var dir = new DirectoryInfo(path);
            //dir.Refresh();

			string[] fileList = null;
			try
			{
				//fileList = System.IO.Directory.GetFiles(dir);
                fileList = System.IO.Directory.GetFiles(path);
			}
			catch (UnauthorizedAccessException e)
			{
				Console.WriteLine(e.Message);
				//continue;
                return;
			}
			catch (System.IO.DirectoryNotFoundException e)
			{
				Console.WriteLine(e.Message);
				return;
			}

			// Perform the required action on each file here.
			// Modify this block to perform your required task.
			foreach (string file in fileList)
			{
				try
				{
					// Perform whatever action is required in your scenario.
					System.IO.FileInfo fi = new System.IO.FileInfo(file);

                    //22.06 소스추가 2-1
                    bool fileWriteDone = IsFileWriteDone(fi.FullName);
                    if (fileWriteDone)
                    {

                        Console.WriteLine("{0}: {1}, {2}", fi.Name, fi.Length, fi.CreationTime);
                        
                        var swe1 = sw.Elapsed;

                        min = fi.LastWriteTime;
                        newfile = fi.FullName;
                        destFile = tmppath + slash + fi.Name;

                        //ML로 전송한 메세지를 로그파일에서 확인
                        bool existSendlog = false;

                        string logline = "";
                        // 로그 파일 read
                        StreamReader logfile = new StreamReader(LogManager.logpath);
                        // Log파일 이력 검색
                        try
                        {
                            while (logfile.Peek() > -1)
                            {
                                logline = logfile.ReadLine();
                                
                                if (logline.Substring(0, 5) == "send:")
                                {
                                    
                                    existSendlog = logline.Contains(fi.Name); 
                                    if (existSendlog)
                                    {
                                        Console.WriteLine("existSendlog ::: " + existSendlog.ToString());
                                        break;
                                    }
                                }
                            }
                        }
                        //22.06 소스추가 2-2
                        catch (Exception e)
                        {
                            // Logfile 탐색중 Exception 발생하였을때 existSending = true 처리 필요
                            Console.WriteLine(e.Message);
                            existSendlog = true;
                        }
                        finally
                        {
                            logfile.Close();
                        }
                    

                        // 보낸이력이 있는지 확인결과에 따른 처리
                        // 보낸이력이 없으면 메시지 전송
                        // 보낸이력이 있으면 메시지 전송 안함
                        
                        if (!existSendlog)
                        {
                            Console.WriteLine("Found New File & Send Message-----------------------------");

                            //lastpath = newfile;
                            var swe2 = sw.Elapsed;
                            ++file_cnt;
                            SendMessage(newfile, startTime, moduleclient);
                            lastpath = newfile;
                            // maxFTime = new FileInfo(lastpath).LastWriteTime;

                            var swe3 = sw.Elapsed;
                            DateTime endTime = DateTime.Now;
                            Console.WriteLine("**********************************************************");
                            Console.WriteLine("S1.elapsed time: " + (endTime - startTime));
                            //Console.WriteLine("S2.check file line & lock & message : " + (swe2 - swe1));
                            Console.WriteLine("S2.Send Message : " + (swe3 - swe2));
                            Console.WriteLine("**********************************************************");
                            //continue;

                            //22.06 코드 추가 1-1
                            // SendMessage후 ML,Action 모듈의 Process 시간 확보를 위해 Timer 주기 연장
                            aTimer.Interval = 1000;

                            break;
                        }
                        else
                        {
                            Console.WriteLine("**********************************************************");
                            Console.WriteLine("Filename exists in Sendlog : " + fi.Name);
                            Console.WriteLine("**********************************************************");
                        }
                        //Console.WriteLine("if existSendlog end==========================================");
                    }
				}
				catch (System.IO.FileNotFoundException e)
				{
					// If file was deleted by a separate application
					//  or thread since the call to TraverseTree()
					// then just continue.
					Console.WriteLine(e.Message);
					continue;
				}



			}


			aTimer.Enabled = true;
		}

        #region (HDF5 파일형태로 변경후 사용안함)

        /// <summary> 
        /// CSV 파일 사용시에는 비정상적인 file Line 으로 count 하는 매소드 
        /// (HDF5 파일형태로 변경후 사용안함)
        /// </summary>
        private static int CountFileLines(Stream stream)
        {
            //Ensure.NotNull(stream, nameof(stream));

            var lineCount = 0;

            var byteBuffer = new byte[1024 * 1024];
            const int BytesAtTheTime = 4;
            var detectedEOL = NULL;
            var currentChar = NULL;

            int bytesRead;
            while ((bytesRead = stream.Read(byteBuffer, 0, byteBuffer.Length)) > 0)
            {
                var i = 0;
                for (; i <= bytesRead - BytesAtTheTime; i += BytesAtTheTime)
                {
                    currentChar = (char)byteBuffer[i];

                    if (detectedEOL != NULL)
                    {
                        if (currentChar == detectedEOL) { lineCount++; }

                        currentChar = (char)byteBuffer[i + 1];
                        if (currentChar == detectedEOL) { lineCount++; }

                        currentChar = (char)byteBuffer[i + 2];
                        if (currentChar == detectedEOL) { lineCount++; }

                        currentChar = (char)byteBuffer[i + 3];
                        if (currentChar == detectedEOL) { lineCount++; }
                    }
                    else
                    {
                        if (currentChar == LF || currentChar == CR)
                        {
                            detectedEOL = currentChar;
                            lineCount++;
                        }
                        i -= BytesAtTheTime - 1;
                    }
                }

                for (; i < bytesRead; i++)
                {
                    currentChar = (char)byteBuffer[i];

                    if (detectedEOL != NULL)
                    {
                        if (currentChar == detectedEOL) { lineCount++; }
                    }
                    else
                    {
                        if (currentChar == LF || currentChar == CR)
                        {
                            detectedEOL = currentChar;
                            lineCount++;
                        }
                    }
                }
            }

            if (currentChar != LF && currentChar != CR && currentChar != NULL)
            {
                lineCount++;
            }
            return lineCount;
        }

        /// <summary> 
        /// CSV 파일에 접근 가능 여부를 확인하기 위한 매소드 (HDF5 파일형태로 변경후 사용안함)
        /// </summary>
        private static bool IsAccessAble(string path)
        {
            FileStream fs = null;
            try
            {
                fs = new FileStream(path, FileMode.Open,
                                    FileAccess.ReadWrite, FileShare.None);
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

        /// <summary> 
        /// 여러건의 파일를 한번에 ML에 메시지 보내는 매소드(현재사용안함)
        /// </summary>
        private static void SendDirList(DirectoryInfo dir, ModuleClient moduleclient)
        {
            DateTime startTime;
            //var directory = new DirectoryInfo(dir_path);
            List<FileInfo> fileList = new List<FileInfo>();
            dir.Refresh();
            Console.WriteLine($"####[{dir.GetFiles().Length}] GetFiles Count :::::::::::::: ");
            if (dir.GetFiles().Length > 0)
            {
                fileList = dir.GetFiles().OrderBy(f => f.LastWriteTime).ToList();


                foreach (FileInfo fi in fileList)
                {
                    //if(maxFTime < fi.LastWriteTime && lastpath != fi.FullName)
                    if (lastpath != fi.FullName)
                    {
                        //Console.WriteLine(fi.Name);
                        startTime = DateTime.Now;
                        file_cnt++;
                        SendMessage(fi.FullName, startTime, moduleclient);
                        lastpath = fi.FullName;
                        maxFTime = fi.LastWriteTime;
                    }
                }
            }
        }
        #endregion


        /// <summary> 
        /// Message를 EdgeHub로 전송. 1초 초과시 TimeOut
        /// </summary>
        /// <returns>
        /// 파일경로, 체킹시작시간, 모듈
        /// </returns>
        private static void SendMessage(string file, DateTime startTime, ModuleClient moduleclient)
        {

            var messageBody = AssignTempMessageBody(file, startTime);
            var messageString = JsonConvert.SerializeObject(messageBody);
            try
            {
                ConnectionManager.SendData(moduleclient, messageString).Wait(1000);


                lastpath = file;
                maxFTime = new FileInfo(lastpath).LastWriteTime;

                send_cnt++;
                Console.WriteLine($"####[{send_cnt}] SentMessage :: " + messageString);
                LogManager.LogWrite(messageString, LogManager.MessageStatus.Send);
            }
            catch (AggregateException ex)
            {
                Console.WriteLine($"[ERROR] - Program.SendMessage() - AggregateException : {ex.Message}");
                Console.WriteLine($"\t{ex.ToString()}");
                //throw;
            }
            catch (ObjectDisposedException ex)
            {
                Console.WriteLine($"[ERROR] - Program.SendMessage() - ObjectDisposedException : {ex.Message}");
                Console.WriteLine($"\t{ex.ToString()}");
                //throw;
            }
            catch (System.Exception ex)
            {
                Console.WriteLine($"[ERROR] - Program.SendMessage() - Unexpected Exception : {ex.Message}");
                Console.WriteLine($"\t{ex.ToString()}");
            }
        }

        /// <summary> 
        /// This is MessageBody 
        /// </summary>
        private static MessageBody AssignTempMessageBody(string path, DateTime chtime)
        {
            var messageBody = new MessageBody
            {
                Path = path,
                ChTime = chtime
            };
            return messageBody;
        }

        //22.06 소스추가 2-1 
        
        private static bool IsFileWriteDone(string path)
        {
            FileStream fs = null;
            try
            {
                fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.None);
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
