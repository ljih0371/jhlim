namespace mcactmodule
{
    using System;
    using System.Runtime.Loader;
    using System.Threading;

    class Program
    {
        static void Main(string[] args)
        {
            // SQL Config 파일 불러오기
            SQLManager.SQLConfig();

            // 디렉토리가 없으면 생성
            ErrorLogManager.LogDirectory();

            // 테이블이 없으면 생성
            SQLManager.CreateSQLTable();

            //PLC연결
            PLCManager.PLCConfig();
            PLCManager.PLCConnect();
            
            // Action 동작
            ConnectionManager.Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            ConnectionManager.WhenCancelled(cts.Token).Wait();
        }
    }
}
