namespace MC_CheckingModule
{
    using System.IO;
    using System.Reflection;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Diagnostics;
    using System.Globalization;

    public class LogManager
    {
        public static string logpath {get; set;}

        public enum MessageStatus
        {
            Send,
            Usual,
            Error
        }


        // 로그 파일 만들기 및 열기
        public static void LogWrite(string Message, MessageStatus messageStatus)
        {
            
            string StrDateTime = DateTime.Now.ToString("yyyyMMdd");
            string FilePATH = logpath;
            
            //로그파일이 저장될 디렉토리가 없는 경우 디렉토리 생성.
            FileInfo fi = new FileInfo(FilePATH);

            if(!Directory.Exists(fi.DirectoryName))
            {
                Directory.CreateDirectory(fi.DirectoryName);
            }

            //파일 열기
            try
            {
                using (StreamWriter w = File.AppendText(FilePATH))
                {
                    WriteOnConsole(Message, messageStatus, w);
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("\n Stream Writer Error \n" + e);
            }
        }

        // 메시지 상태에 따라 다른 추가 메시지 삽입
        private static string WriteOnConsole(string message, MessageStatus messageStatus, TextWriter w)
        {
            string messageToWrite = message;
            switch (messageStatus)
            {
                case MessageStatus.Send:
                    //WriteMessage(message);
                    SendMessageLog(messageToWrite,w, "# SendMessage Log : ");
                    return messageToWrite;

                case MessageStatus.Usual:
                    WriteMessage(message);
                    Log(messageToWrite, w, "# Usual Log : ");
                    return messageToWrite;

                case MessageStatus.Error:
                    WriteErrorMessage("Warning :" + message);
                    Log(messageToWrite, w, "# Error Log : ");
                    return messageToWrite;

                default:
                    return messageToWrite;
            }
        }

        // 메시지를 파일에 쓰기
        private static void Log(string logMessage, TextWriter txtWriter, string logType)
        {
            txtWriter.WriteLine("-----------------------------------");
            txtWriter.Write(logType);
            txtWriter.WriteLine("{0}", DateTime.Now.ToString());
            txtWriter.WriteLine(logMessage);
        }

        private static void SendMessageLog(string logMessage, TextWriter txtWriter, string logType)
        {
            txtWriter.WriteLine("-------------------------------------");
            txtWriter.Write(logType);
            txtWriter.WriteLine("{0}", DateTime.Now.ToString());
            txtWriter.WriteLine("send: {0}", logMessage);
        }
        
        // 메시지 상태가 Error면 빨간 글씨
        private static void WriteErrorMessage(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(message);
            Console.ResetColor();
        }
        
        //메시지 상태가 일반이면 그대로
        private static void WriteMessage(string message)
        {
            Console.WriteLine(message);
        }
    }
    
}