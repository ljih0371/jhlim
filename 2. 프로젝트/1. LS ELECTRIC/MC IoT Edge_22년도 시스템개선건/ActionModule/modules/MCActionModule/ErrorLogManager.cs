namespace mcactmodule
{
    using System.IO;
    using System;

    public class ErrorLogManager
    {
        // log 디렉토리 생성
        public static void LogDirectory()
        {
            string ErrorLogPATH = $"/home/data/logfile/Actmodule";

            if(!Directory.Exists(ErrorLogPATH))
            {
                Directory.CreateDirectory(ErrorLogPATH);
            }

            DeleteFiles(ErrorLogPATH, "*.txt", 60);
        }

        public static void DeleteFiles(string sDirPath, string sTargetFile, int nLen)
        {
            DirectoryInfo di = new DirectoryInfo(sDirPath);
                //if (!di.Exists) Directory.CreateDirectory(sDirPath);

                FileInfo[] files = string.IsNullOrEmpty(sTargetFile) ? di.GetFiles() : di.GetFiles(sTargetFile);
                DateTime currentDate = DateTime.Now;

                if (files.Length > nLen)
                    {
                        for (int i = 0; i < files.Length - nLen; i++)
                        {
                            files[i].Delete();
                        }
                    }
        }

        
        public static void LogWrite(string message)
        {
            string StrDateTime = DateTime.Now.ToString("yyyyMMdd");
            string ErrorLogFilePATH = $"/home/data/logfile/Actmodule/Actmodule_ErrorLog_" + StrDateTime + ".txt";
            using (StreamWriter w = File.AppendText(ErrorLogFilePATH))
            {
                LogWriteLine(message, w, "# Error Log : ");
            }
        }
        private static void LogWriteLine(string logMessage, TextWriter txtWriter, string logType)
        {
            Console.WriteLine(logMessage);
            txtWriter.Write(logType);
            txtWriter.WriteLine("{0} {1}\n\r", DateTime.Now.ToLongTimeString(), DateTime.Now.ToLongDateString());
            txtWriter.WriteLine("{0}\n\r", logMessage);
            txtWriter.WriteLine("-------------------------------\n\r");
        }
    }
}