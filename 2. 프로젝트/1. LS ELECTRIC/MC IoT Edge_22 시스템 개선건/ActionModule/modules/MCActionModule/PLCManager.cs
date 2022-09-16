namespace mcactmodule
{
    using System;
    using System.Threading;
    using System.Net.Sockets;
    using System.IO;
    using System.Collections.Generic;
    using LSIS;
    using LSIS.NetworkLib;
    using YamlDotNet.Serialization;

    //static으로 클래스 객체와 변수를 선언하지 않으면 빌드 시 에러 발생

    public class PLCManager
    {
        enum BIT { PC_READY, MEASURE, OK , NG}

        enum SIGNAL { OFF, ON };
        
        //static string m_sIP; //PLC IP주소
        static string m_sIP; //PLC IP주소
        static string[] m_sXAddress = { "%MX2000", "%MX2001" ,"%MX2002", "%MX2003" }; //0,1,결과 표현

        static LSIS_FEnet CXGK; //PLC 통신용 객체 변수
        static Socket Csocket; //소켓 객체 변수

        static bool m_btemp = true;
        static int result = 1;
        static string barcode = "";

        // SQL 정보를 가져옴
        public static void PLCConfig()
        {
            var d = new Deserializer();
            var result = d.Deserialize<Dictionary<string, YamlReader.YamlConfig>>(new StreamReader($"/home/data/edge_config.yml"));
            foreach (var a in result.Values)
            {
                m_sIP = a.line.plc; //PLC IP정보 습득
            }
        }

        //PLC 소켓통신 연결
        public static void PLCConnect()
        {
            Exception NET_EXCEPTION;
            Csocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            
            CXGK = new LSIS_FEnet(LSIS_FEnet.PLC.XGT);
            
            bool btemp = true;
            LSIS_FEnet.EECode nEECode;
            while(btemp)
            {
                nEECode = CXGK.OpenEthernetPort(Csocket, m_sIP, out NET_EXCEPTION);

                if(nEECode != LSIS_FEnet.EECode.NONE)
                {
                    Console.WriteLine(NET_EXCEPTION.Message);
                }
                else { btemp = false; }
            }            
            Console.WriteLine("PLC 소켓 연결 성공");
        }

        //ML 결과 받기
        public static void PLCMessage(string mlresult, string mlbarcode)
        {
            result = int.Parse(mlresult);
            barcode = mlbarcode;
        }

        //PLC 상태 체크 및 연결 끊김 시 소켓 해제 후 재연결
        public static void MethodThread()
        {
            while (m_btemp)
            {
                try
                {
                    if (Convert.ToBoolean(CXGK.ReadSingleData(Csocket, LSIS_FEnet.DATA_TYPE.BIT, "%FX0000").DATA[0]))
                    {
                        //Console.WriteLine("PLC %FX0000 true");

                        if(result == 0) //양품인 경우
                        {
                            Console.WriteLine("Send result : OK(->PLC)");
                            CXGK.WriteSingleData(Csocket, LSIS_FEnet.DATA_TYPE.BIT, m_sXAddress[(int)BIT.OK], SIGNAL.ON);
                        }
                        else if(result == 1)//불량인 경우
                        {
                            Console.WriteLine("Send result : NG(->PLC)");
                            CXGK.WriteSingleData(Csocket, LSIS_FEnet.DATA_TYPE.BIT, m_sXAddress[(int)BIT.NG], SIGNAL.ON);
                        }
                        else if(result == 2) //ML ERR인 경우
                        {
                            ErrorLogManager.LogWrite("ML RESULT ERR");
                            CXGK.WriteSingleData(Csocket, LSIS_FEnet.DATA_TYPE.BIT, m_sXAddress[(int)BIT.NG], SIGNAL.ON);
                        }
                    }
                    else
                    {
                        ErrorLogManager.LogWrite("PLC %FX0000 false");
                        if(!CXGK.IsConnectedTCP(Csocket))
                        {
                            CXGK.CloseTCP(Csocket);
                            ErrorLogManager.LogWrite("Socket 종료");

                            Exception CDummyException;
                            Csocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                            CXGK.OpenTCP(Csocket, m_sIP, out CDummyException);
                            ErrorLogManager.LogWrite("Socket 재연결");
                        }

                        Thread.Sleep(500);
                        continue;
                    }
                    //결과 전송에 성공한 경우 스레드 종료
                    m_btemp = false;
                }
                catch(Exception e)
                {
                    ErrorLogManager.LogWrite(e.ToString());
                }
                
            }
            
            //다음 제품을 위한 bool 변수 True
            m_btemp = true;
        }
    }
}