namespace MC_CheckingModule
{
    using System;
    using System.IO;
    using System.Globalization;
    public class DirectoryManager
    {
        public static DateTime directoryDay {get;set;}
        public static string dirpath {get; set;}
        public static string tmppath {get; set;}
        //public static string linepath {get;set;}
        public static string rootpath {get;set;}
        public static string slash {get;set;}

        public DirectoryManager(string OS)
        {
            directoryDay = DateTime.Now;
            if(OS == "testOnWindow")
            {
                rootpath = "c:\\dd\\";
                slash = "\\";
            }
            else if(OS == "productionOnLinux")
            {
                rootpath = "/home/data/";
                slash = "/";
            }
     
        }

        // workingday 변경 여부 체크
        public static bool IsNewDay()
        {
            string thisDay = DateTime.Now.ToString("yyyy-MM-dd");
            string directoryday = directoryDay.ToString("yyyy-MM-dd");
            if(directoryday != thisDay)
            {
                directoryDay = DateTime.Now;
                setDirPath();
                return true;
            }
            else if(directoryday == thisDay)
            {
                setDirPath();
                return false;
            }
            else
            {
                setDirPath();
                return false;
            }
        }
        
        /*
        // Line 디렉토리 탐색
        public static void checkLine()
        {
            string [] all_line = {"MC*22b*", "MR*18a*", "MC*40a*", "MC*65a*","MC*100a*","MC*","MR*"};

            string [] dirs;
            for (int i = 0; i < all_line.Length; i++)
            {
                dirs = Directory.GetDirectories(rootpath, all_line[i], SearchOption.TopDirectoryOnly);

                if(dirs.Length == 1 && Directory.GetDirectories(dirs[0],"Switch",SearchOption.TopDirectoryOnly).Length != 0)
                {
                    linepath = dirs[0];
                    break; 
                }
                else if(dirs.Length > 1)
                {
                   // Switch 폴더가 있는 디렉토리를 찾기
                    foreach (string dir in dirs) 
                    {
                        if(Directory.GetDirectories(dir,"Switch",SearchOption.TopDirectoryOnly).Length != 0 && Directory.Exists(Directory.GetDirectories(dir,"Switch",SearchOption.TopDirectoryOnly)[0]))
                        {
                            linepath = dir;
                            break;
                        }
                    }
                }
            }

            if(!Directory.Exists(linepath))
            {
                LogManager.LogWrite($"{rootpath}에서 Line 디렉토리를 찾을 수 없습니다.", LogManager.MessageStatus.Error);
                throw new System.ArgumentException("Line 디렉토리를 찾을 수 없습니다. : 'MC*22b*', 'MR*18a*', 'MC*40a*', 'MC*65a*', 'MC*100a*', 'MC*', 'MR*' 필요!","linepath");
            }
            // linepath = "/home/data";
        }
        */

        // 디렉토리 path 설정
        public static void setDirPath()
        {
            dirpath = rootpath + directoryDay.ToString("yyyy-MM-dd") + slash + "RawData";
            tmppath = rootpath + directoryDay.ToString("yyyy-MM-dd") + slash + "Raw";
            //dirpath = linepath + slash + "Switch" + slash +"AI Data" + slash + directoryDay.ToString("yyyy-MM-dd");
        }

    }
}