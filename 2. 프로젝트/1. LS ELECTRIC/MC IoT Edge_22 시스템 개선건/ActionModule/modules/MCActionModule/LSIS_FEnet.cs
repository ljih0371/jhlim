#define XGT_EFMT
#define NET3

using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Collections;
using System.IO;

namespace LSIS
{
    namespace NetworkLib
    {
        class LSIS_FEnet//v1.1.2(�����Ͻ�:2018.07.05)
        {
            #region[ENUMŸ��]
            public enum PLC { GLOFA, XGT, XGB }
            readonly string[] COMPANY_ID = new string[] { "LGIS-GLOFA", "LSIS-XGT", "LSIS-XGT" };
            // ASCII CODE
            protected enum ASCII : byte
            {
                NULL = 0x00,
                CLIENT = 0x11,//PLC -> PC
                SERVER = 0x33,//PC -> PLC
            }

            //XGKŸ��(EFMT ��⿡���� ��ȿ)
            protected enum CPU : byte
            {
                XGK = 0xA0,
                XGI = 0xA4,
                XGB = 0xA8,
            }

            protected enum PORT
            {
                TCP = 2004,
                UDP = 2005,
                MODBUS_TCP = 502,
            }

            //���ɾ�
            protected enum COMMAND : byte
            {
                READ_REQ = 0x54,
                READ_RCV = 0x55,
                WRITE_REQ = 0x58,
                WRITE_RCV = 0x59,
            }

            //������ ��������
            public enum DATA_TYPE : byte
            {
                BIT = 0x00,
                BYTE = 0x01,
                WORD = 0x02,
                DWORD = 0x03,
                LWORD = 0x04,
                CONTI = 0x14,//�����б⸸ ����
            }

            protected enum APP_HEADER
            {
                COMPANY_ID = 0,//EUTB:LGIS-GLOFA, EFMT:LSIS-XGT
                PLC_INFO = 20,
                CPU_INFO = 24,//EUTB������ ���࿵��
                SOURCE = 26,
                INVOKE_ID = 28,
                LENGTH = 32,//Application Instruction Format(���ɾ�~������) ����[byte����]
                MODULE_POS = 36,//EUTB������ ���࿵��
                BCC = 38,
            }

            protected enum APP_INSTRUCTION
            {
                COMMAND = 40,
                DATA_TYPE = 44,
                ERROR = 52,//ERROR ���¿� ���� �����ڵ� Ȥ�� ���������� ��
                BLOCK = 56,
                ERR_CODE = 56,
                DATA_SIZE = 60,
                DATA = 64,
            }

            public enum EECode
            {
                NONE,
                NOT_OPEN_TCP_PORT,
                WRONG_IP,
            }
            #endregion

            private byte[] TX;
            private string m_sIP;
            private const int RECV_BUFFER_SIZE = 2048;//�����б�/���⿡�� �ִ� 1400����Ʈ�� �ٷ�� 2048�� ����
            private const int HEAD_SIZE = 20;
            
            protected PLC m_nPLCType;
            public static string ERR_LOG_FOLDER
            { get; private set; }

            public string IP
            { private set; get; }

            public LSIS_FEnet(PLC nPLC)
            {
                m_nPLCType = nPLC;
                //ERR_LOG_FOLDER = Application.StartupPath + @"\ErrLog";
                ERR_LOG_FOLDER = "dummy";
            }

            internal bool IsConnectedTCP()
            {
                throw new NotImplementedException();
            }

            #region[�����Ʈ����/�ݱ�]
            /// <summary>
            /// TCP��Ʈ����
            /// </summary>
            /// <param name="sIP">IP</param>
            /// <param name="strErr">����</param>
            /// <returns>��������</returns>
            public bool OpenTCP(Socket cSocket, string sIP, out Exception TCP_EXCEPTION)
            {
                bool bConnected = false; m_sIP = sIP;
                TCP_EXCEPTION = null;

                try { CreateTCP(cSocket, m_sIP); }
                catch (Exception e)
                { TCP_EXCEPTION = e; }

                if (cSocket.Connected) bConnected = true;
                return bConnected;
            }

            private void CreateTCP(Socket cSocket, string sIP)
            {
                IPAddress CHost = IPAddress.Parse(sIP);
                IPEndPoint CIPEP = new IPEndPoint(CHost/*PLC*/, (int)PORT.TCP);
                
                //190515 �ɼ� ��� �����۵��Ͽ� �ּ� ó����(�翬��ÿ� ������ ������ CLOSE �ϰ� NEW�� ���� �Ҵ� �ϹǷ� �ʿ� ����)
                //cSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.ReuseAddress, true);

                cSocket.Connect(CIPEP);

            }

            /// <summary>
            /// TCP��Ʈ�ݱ�
            /// </summary>
            /// <returns>��������</returns>
            public bool CloseTCP(Socket cSocket)
            {
                bool bConnected = false;

                try { cSocket.Shutdown(SocketShutdown.Both); }
                catch { }
                try { cSocket.Close(); }
                catch { }

                if (!cSocket.Connected) bConnected = true;

                return bConnected;
            }

            /// <summary>
            /// TCP��Ʈ����Ȯ��
            /// </summary>
            /// <returns>���ӿ���</returns>
            public bool IsConnectedTCP(Socket cSocket)
            {
                bool bConnected = false;
                if (cSocket.Connected) bConnected = true;
                return bConnected;
            }
            #endregion

            #region[���� ������ �м�]
            /// <summary>
            /// ���� ������ �м�(HEAD)
            /// </summary>
            /// <param name="sRcvFrame">����������</param>
            /// <returns>�м��� ���ڿ� �迭</returns>
            public string[] RcvHeaderAnalysis(string sRcvFrame)
            {
                string[] sHeaderData = new string[Enum.GetNames(typeof(APP_HEADER)).Length];

                sHeaderData[0] = HexToAscii(sRcvFrame.Substring((int)APP_HEADER.COMPANY_ID, 20));
                sHeaderData[1] = sRcvFrame.Substring((int)APP_HEADER.PLC_INFO, 4);//PLC�� ���� �ڼ��� ������ STATUS �������� ó��
                if (sHeaderData[0] == "LSIS-XGT\0\0") sHeaderData[2] = Enum.GetName(typeof(CPU), Convert.ToInt32(sRcvFrame.Substring((int)APP_HEADER.CPU_INFO, 2), 16));
                sHeaderData[3] = sRcvFrame.Substring((int)APP_HEADER.SOURCE, 2) == "11" ? "CLIENT" : "SERVER";
                sHeaderData[4] = HexToDec(SwapFrame(sRcvFrame.Substring((int)APP_HEADER.INVOKE_ID, 4), 2));
                sHeaderData[5] = HexToDec(SwapFrame(sRcvFrame.Substring((int)APP_HEADER.LENGTH, 4), 2));
                if (sHeaderData[0] == "LSIS-XGT\0\0") sHeaderData[6] = sRcvFrame.Substring((int)APP_HEADER.MODULE_POS, 2);
                sHeaderData[7] = sRcvFrame.Substring((int)APP_HEADER.BCC, 2);

                return sHeaderData;
            }

            /// <summary>
            /// ���� ������ �м�(INSTRUCTION)
            /// </summary>
            /// <param name="sRcvFrame">����������</param>
            /// <returns>�м��� ���ڿ� �迭</returns>
            public string[] RcvInstructionAnalysis(string sRcvFrame)
            {
                ArrayList al = new ArrayList();

                string sCmdType = Enum.GetName(typeof(COMMAND), Convert.ToInt32(sRcvFrame.Substring((int)APP_INSTRUCTION.COMMAND, 2), 16));
                string sDataType = Enum.GetName(typeof(DATA_TYPE), Convert.ToInt32(sRcvFrame.Substring((int)APP_INSTRUCTION.DATA_TYPE, 2), 16));

                al.Add(sCmdType);
                al.Add(sDataType);
                al.Add(sRcvFrame.Substring((int)APP_INSTRUCTION.ERROR, 4));

                if (sRcvFrame.Substring((int)APP_INSTRUCTION.ERROR, 4) != "0000") al.Add(sRcvFrame.Substring((int)APP_INSTRUCTION.ERR_CODE, 2));
                else
                {
                    if (sCmdType == "READ_RCV")
                    {
                        int nIndexCtrl = (int)APP_INSTRUCTION.DATA_SIZE;
                        al.Add(sRcvFrame.Substring((int)APP_INSTRUCTION.BLOCK, 4));
                        switch (sDataType)
                        {
                            case "CONTI":
                                int nDataLen = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring(nIndexCtrl, 4), 2)));
                                al.Add(sRcvFrame.Substring(nIndexCtrl, 4));
                                nIndexCtrl += 4;//APP_INSTRUCTION.DATA
                                //for (int i = nData.Length - 1; i > -1; i--)
                                for (int i = 0; i < nDataLen; i++)
                                {
                                    System.Diagnostics.Trace.WriteLine("�����Ͱ�(HEX):" + sRcvFrame.Substring(nIndexCtrl, 2));//�����
                                    al.Add(sRcvFrame.Substring(nIndexCtrl, 2));
                                    nIndexCtrl += 2;
                                }
                                break;
                            default:
                                //���ŵ� ������ �� BIT(BYTE)�� ��� ������(����) ��Ʈ(����Ʈ)�� ��ȿ
                                int nDataSize;// = int.Parse(HexToDec(swapFrame(rcvFrame.Substring((int)APP_INSTRUCTION.DATA_SIZE, 4),2)));
                                int nBlockCnt = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring((int)APP_INSTRUCTION.BLOCK, 4), 2)));

                                for (int i = 0; i < nBlockCnt; i++)
                                {
                                    nDataSize = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring(nIndexCtrl, 4), 2)));

                                    al.Add(sRcvFrame.Substring(nIndexCtrl, 4));//���ŵ� HEX ������ �״�� �Ľ�
                                    al.Add(sRcvFrame.Substring(nIndexCtrl + 4, nDataSize * 2));//���ŵ� HEX ������ �״�� �Ľ�

                                    nIndexCtrl += (4 + nDataSize * 2);//������ũ�����(4)+������ũ��(x)*2
                                }
                                break;
                        }
                    }
                }
                return (string[])al.ToArray(typeof(string));
            }

            /// <summary>
            /// ���� ������ ������ �м�
            /// </summary>
            /// <param name="sRcvFrame">����������</param>
            /// <param name="sAscii">���� ����̽��� ��ȯ�� ��(���ڿ�)</param>
            /// <param name="nData">���� ����̽��� ��ȯ�� ��(�չ迭)</param>
            protected bool RcvValueAnalysis(string sRcvFrame, ref string sAscii, ref long[] nData)
            {
                int nCmdType = Convert.ToInt32(sRcvFrame.Substring((int)APP_INSTRUCTION.COMMAND, 2), 16);
                int nDataType = Convert.ToInt32(sRcvFrame.Substring((int)APP_INSTRUCTION.DATA_TYPE, 2), 16);
                int nAppIstFormLen = Convert.ToInt32(sRcvFrame.Substring((int)APP_HEADER.LENGTH, 2), 16);//Application Instruction Format ����
                int nIndexCtrl = (int)APP_INSTRUCTION.DATA_SIZE;

                if (sRcvFrame.Substring((int)APP_INSTRUCTION.ERROR, 4) != "0000") { nData[0] = Convert.ToInt64(sRcvFrame.Substring((int)APP_INSTRUCTION.ERR_CODE, 2), 16); return false; }
                else
                {
                    switch (nDataType)
                    {
                        case (int)DATA_TYPE.CONTI:
                            nIndexCtrl += 4;//APP_INSTRUCTION.DATA

                            //for (int i = nData.Length - 1; i > -1; i--)//�ԷµǴ� ������ �ݴ�� ǥ��(�Է½���(�����ּ� ������)�� �����ʿ��� ��������)
                            for (int i = 0; i < nData.Length; i++)//�ԷµǴ� ������� ǥ��(�Է½���(�����ּ� ������)�� ���ʿ��� ����������)
                            {
                                //System.Diagnostics.Trace.WriteLine("�����Ͱ�(HEX):" + rcvFrame.Substring(indexCtrl, 2));//�����
                                nData[i] = Convert.ToInt32(sRcvFrame.Substring(nIndexCtrl, 2), 16/*2, 8, 10, 16���� �� ����*/);//PLC���� ������ ���޵ɶ�(MOV)
                                sAscii += HexToAscii(sRcvFrame.Substring(nIndexCtrl, 2));//PLC���� ���ڷ� ���޵ɶ�($MOV)
                                nIndexCtrl += 2;
                            }
                            break;
                        default:
                            if (nCmdType != (int)COMMAND.WRITE_RCV)
                            {
                                //���ŵ� ������ �� BIT(BYTE)�� ��� ������(����) ��Ʈ(����Ʈ)�� ��ȿ
                                int nDataSize;// = int.Parse(HexToDec(swapFrame(rcvFrame.Substring((int)APP_INSTRUCTION.DATA_SIZE, 4),2)));
                                int nBlockCnt = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring((int)APP_INSTRUCTION.BLOCK, 4), 2)));

                                for (int i = 0; i < nBlockCnt; i++)
                                {
                                    nDataSize = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring(nIndexCtrl, 4), 2)));
                                    if (nDataSize < 2)//��Ʈ Ȥ�� ����Ʈ
                                        nData[i] = Convert.ToInt32(sRcvFrame.Substring(nIndexCtrl + 4, nDataSize * 2), 16);//PLC���� ������ ���޵ɶ�(MOV)
                                    else //���� �̻�(����Ʈ���� ���� �� �� ���)
                                    {
                                        nData[i] = long.Parse(HexToDec(SwapFrame(sRcvFrame.Substring(nIndexCtrl + 4, nDataSize * 2), 2)));//PLC���� ������ ���޵ɶ�(MOV)
                                        sAscii += HexToAscii(sRcvFrame.Substring(nIndexCtrl + 4, nDataSize * 2));//PLC���� ���ڷ� ���޵ɶ�($MOV)
                                    }

                                    nIndexCtrl += (4 + nDataSize * 2);//������ũ�����(4)+������ũ��(x)*2
                                }
                            }
                            break;
                    }
                }
                return true;
            }

            protected bool RcvValueAnalysisXX(string sRcvFrame, ref string sAscii, ref long[] nData)
            {
                int nCmdType = Convert.ToInt32(sRcvFrame.Substring((int)APP_INSTRUCTION.COMMAND, 2), 16);
                int nDataType = Convert.ToInt32(sRcvFrame.Substring((int)APP_INSTRUCTION.DATA_TYPE, 2), 16);
                int nAppIstFormLen = Convert.ToInt32(sRcvFrame.Substring((int)APP_HEADER.LENGTH, 2), 16);//Application Instruction Format ����
                int nIndexCtrl = (int)APP_INSTRUCTION.DATA_SIZE;

                if (sRcvFrame.Substring((int)APP_INSTRUCTION.ERROR, 4) != "0000") { nData[0] = Convert.ToInt64(sRcvFrame.Substring((int)APP_INSTRUCTION.ERR_CODE, 2), 16); return false; }
                else
                {
                    switch (nDataType)
                    {
                        case (int)DATA_TYPE.CONTI:
                            nIndexCtrl += 4;//APP_INSTRUCTION.DATA

                            //for (int i = nData.Length - 1; i > -1; i--)//�ԷµǴ� ������ �ݴ�� ǥ��(�Է½���(�����ּ� ������)�� �����ʿ��� ��������)
                            for (int i = 0; i < nData.Length; i++)//�ԷµǴ� ������� ǥ��(�Է½���(�����ּ� ������)�� ���ʿ��� ����������)
                            {
                                //System.Diagnostics.Trace.WriteLine("�����Ͱ�(HEX):" + rcvFrame.Substring(indexCtrl, 2));//�����
                                nData[i] = Convert.ToInt32(sRcvFrame.Substring(nIndexCtrl, 2), 16/*2, 8, 10, 16���� �� ����*/);//PLC���� ������ ���޵ɶ�(MOV)
                                sAscii += HexToAscii(sRcvFrame.Substring(nIndexCtrl, 2));//PLC���� ���ڷ� ���޵ɶ�($MOV)
                                nIndexCtrl += 2;
                            }
                            break;
                        default:
                            if (nCmdType != (int)COMMAND.WRITE_RCV)
                            {
                                //���ŵ� ������ �� BIT(BYTE)�� ��� ������(����) ��Ʈ(����Ʈ)�� ��ȿ
                                int nDataSize;// = int.Parse(HexToDec(swapFrame(rcvFrame.Substring((int)APP_INSTRUCTION.DATA_SIZE, 4),2)));
                                int nBlockCnt = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring((int)APP_INSTRUCTION.BLOCK, 4), 2)));

                                for (int i = 0; i < nBlockCnt; i++)
                                {
                                    nDataSize = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring(nIndexCtrl, 4), 2)));
                                    if (nDataSize < 2)//��Ʈ Ȥ�� ����Ʈ
                                        nData[i] = Convert.ToInt32(sRcvFrame.Substring(nIndexCtrl + 4, nDataSize * 2), 16);//PLC���� ������ ���޵ɶ�(MOV)
                                    else //���� �̻�(����Ʈ���� ���� �� �� ���)
                                    {
                                        try
                                        {
                                            nData[i] = long.Parse(HexToDec(SwapFrame(sRcvFrame.Substring(nIndexCtrl + 4, nDataSize * 2), 2)));//PLC���� ������ ���޵ɶ�(MOV)
                                        }
                                        catch (Exception e) { string s = e.Message; }
                                        sAscii += HexToAscii(sRcvFrame.Substring(nIndexCtrl + 4, nDataSize * 2));//PLC���� ���ڷ� ���޵ɶ�($MOV)
                                    }

                                    nIndexCtrl += (4 + nDataSize * 2);//������ũ�����(4)+������ũ��(x)*2
                                }
                            }
                            break;
                    }
                }
                return true;
            }
            #endregion

            #region[�����ͺ�ȯ�Լ�]
            /// <summary>
            /// ���ڿ� ����
            /// </summary>
            /// <param name="sChar">�������ڿ�</param>
            /// <returns>���ҹ��ڿ�</returns>
            public string SwapChar(string sChar)
            {
                string sz = "", temp;
                temp = sChar.Trim('\0');//��('\0')���� ����
                if (sChar.Length > 1)
                {
                    //�ι��� ���Կ��� �Ǵ��Ͽ� ���ڱ�ȯ
                    if (temp.Length == 2) sz = sChar.Substring(1, 1) + sChar.Substring(0, 1);
                    else if (temp.Length == 1) sz = sChar.Substring(1, 1);
                }
                else { sz = sChar.Substring(0, 1); }
                //return strChar.Substring(1, 1) + strChar.Substring(0, 1);
                return sz;
            }

            /// <summary>
            /// HEX���ڿ��� ASCII���ڿ��� ��ȯ
            /// </summary>
            /// <param name="sHex">HEX���ڿ��� ASCII������ ��ȯ</param>
            /// <returns>ASCII���ڿ�</returns>
            string HexToAscii(string sHex)
            {
                string sResult = "";

                foreach (Match m in (new Regex(@"[0-9A-F]{2,2}", RegexOptions.IgnoreCase)).Matches(sHex))
                    sResult += ((char)Convert.ToInt32(m.Value, 16)).ToString();

                return sResult;
            }

            /// <summary>
            /// �Էµ� Hex���ڸ� ������ ������ ����
            /// </summary>
            /// <param name="sHex">Hex�� ���ڿ�</param>
            /// <param name="nUnit">���Ҵ���</param>
            /// <returns>���ҹ��ڿ�</returns>
            string SwapFrame(string sHex, int nUnit)
            {
                int nPos = 0;
                int nBlock = sHex.Length / nUnit + 1;
                string s = "";
                for (int i = 1; i < nBlock; i++)
                {
                    nPos = sHex.Length - nUnit * i;
                    s += sHex.Substring(nPos, nUnit);
                }
                if (string.IsNullOrEmpty(s)) s = sHex;
                return s;
            }

            /// <summary>
            /// Hex�� ���ڸ� �Է¹޾� Dec���� ���ڷ� ��ȯ
            /// </summary>
            /// <param name="sHex">Hex�� ���ڿ�</param>
            /// <returns>Dec�� ���ڿ�</returns>
            string HexToDec(string sHex)
            {
                return Convert.ToInt64(sHex, 16).ToString();
            }
            #endregion

            #region[��Ű����Լ�]
            /// <summary>
            /// �������
            /// </summary>
            /// <param name="nInvokeID">�����ΰ� ������ ���� ������ ���� �䱸�� �������� ����</param>
            /// <param name="nDataSize">����� ������ ���ɿ䱸������ ũ��</param>
            protected void MakeHeader(int nInvokeID, int nDataSize)
            {
                TX = new byte[HEAD_SIZE + nDataSize]; // ����(����) ������ ���� �������� �����迭 ����

                int i = 0;
                byte[] nBytes = Encoding.Default.GetBytes(COMPANY_ID[(int)m_nPLCType]);

                //XGT(XGK/XGI)/XGB�� ��� 'LSIS-XGT' 8byte��
                //'LGIS-GLOFA'�� 10byte�� �����ϱ� ���� 2byte�� ���࿵���� �ִ� ������ �Ǿ� ����
                foreach (byte sz in nBytes)
                { TX[i++] = sz; }

                //���� �������� ���� �ε���(10,11,12,18)�� ��� PLC�κ��� ���ŵ� ����� ��ȿ�� ���� ����
                //[10~11]:PLC Info
                //[12]:CPU Info

                TX[13] = (byte)ASCII.SERVER;//Source of Frame
                byte[] z = BitConverter.GetBytes(nInvokeID);
                TX[14] = z[0];//Invoke ID
                TX[15] = z[1];//Invoke ID
                TX[16] = Convert.ToByte(nDataSize);//Length(Application Instruction�� Byte Size)
                //[18]:FEnet Position
                TX[19] = ByteCheckSum();//BCC(Application Header�� Byte Sum)
            }

            /// <summary>
            /// BCC����
            /// </summary>
            /// <returns>BCC������</returns>
            protected byte ByteCheckSum()
            {
                int nCheckSum = 0;

                foreach (byte c in TX)
                {
                    nCheckSum += c;
                    if (nCheckSum > 255) nCheckSum -= 256;
                }

                return Convert.ToByte(nCheckSum);
            }

            /// <summary>
            /// ���ŵ���������
            /// </summary>
            /// <param name="recData">���ŵ�����(HEX)</param>
            /// <returns>��������</returns>
            protected bool ReceiveData(Socket cSocket, ref string sHex, ref byte[] nRcvFrame)
            {
                bool bReceived = false;
                int nTime = 0, nRecv = 0;

                do
                {
                    try
                    { nRecv = cSocket.Receive(nRcvFrame, nRcvFrame.Length, SocketFlags.None); }
                    catch { break; }

                    if (nRecv > 0)
                    {
                        for (int i = 0; i < nRecv; i++)
                            sHex += string.Format("{0:X2}", nRcvFrame[i]);
                        //string s = Encoding.GetEncoding("euc-kr").GetString(receive, 0, iRecv);//PLC���� ���ڿ��� ���۵ɰ��
                        bReceived = true;
                    }
                    nTime++;
                } while (nTime < 10000 && nRecv == 0);

                return bReceived;
            }
            #endregion

            #region[PLC�б�]
            /* %MX2000 �б�(������ Ÿ�� ���� ���� ǥ��:%MX3200)
             * 1.�䱸
             * CompanyID(10) PLCInfo(2) CPUInfo(1) SourceFrame(1) InvokeID(2) Length(2) FEnet Position(1) BCC(1) - Application Header
             * 4C5349532D5847540000 0000 00 33 0100 1100 00 A0 - Application Header
             * 5400 0000 0000 0100 0700 254D5833323030 - Application Instruction:
             * 2.����(ACK)
             * 4C5349532D5847540000 0301 A0 11 0100 0D00 03 21 - Application Header
             * 5500 0000 0000 0000 0100 0100 00 - Application Instruction
             * 3.����(NAK)
             * 4C5349532D5847540000 0301 A0 11 0100 0A00 00 1B - Application Header
             * 5500 0000 0000 FFFF 1200 - Application Instruction
             */
            /// <summary>
            /// �����б�
            /// </summary>
            /// <param name="nInvokeID">INVOKE ID</param>
            /// <param name="nDataType">������ DATA_TYPE(CONTI ����)</param>
            /// <param name="nBlockCnt">�ִ�16�������� h00~h10����(1����=��������+����)</param>
            /// <param name="sAddress">PLC�ּ�(�ּ� �ϳ��� �ִ�16���ڱ��� ���Ǹ� �� ������ ����̽� ������ Ÿ���� �ݵ�� �����ؾ� ��)(%MX100,%MX200)</param>
            /// <param name="sReqFrame">����������</param>
            /// <param name="sRcvFrame">����������</param>
            /// <param name="sRcvASCII">���� ����̽��� ��ȯ�� ��(���ڿ�)</param>
            /// <param name="nData">���� ����̽��� ��ȯ�� ��(�չ迭)</param>
            /// <returns>��������</returns>
            public string ReadSprtPLC(Socket cSocket, int nInvokeID, DATA_TYPE nDataType, string[] sAddress, 
                out string sReqFrame, out string sRcvFrame, out string sRcvASCII, out long[] nData)
            {
                sRcvASCII = sRcvFrame = sReqFrame = "";
                int nBlockCnt = sAddress.Length;
                nData = new long[nBlockCnt];

                //������ ������ Ÿ�԰� ����(����)�� ��ġ�ϴ� Ȯ��
                if (nBlockCnt > 16) return "���ϼ��� 16���� ŭ";
                DATA_TYPE nAddressType = (DATA_TYPE)0xFF;

                switch (sAddress[0].Substring(2, 1).ToUpper())
                {
                    case "X": nAddressType = DATA_TYPE.BIT; break;
                    case "B": nAddressType = DATA_TYPE.BYTE; break;
                    case "W": nAddressType = DATA_TYPE.WORD; break;
                    case "D": nAddressType = DATA_TYPE.DWORD; break;
                    case "L": nAddressType = DATA_TYPE.LWORD; break;
                }
                if (nAddressType != nDataType) return "������ Ÿ�԰� ���� Ÿ�� ����ġ";

                int nAddrByte = 0;
                int nReqByte = 0; //����� ������ ���ɿ䱸������ ũ��[����:byte]
                for (int i = 0; i < nBlockCnt; i++)//�����б� �ּ�ũ�� ���
                    nAddrByte = nAddrByte + 2 + sAddress[i].Length;//nTemp = nTemp + ��������ũ��(2) + �������� [byte]
                nReqByte = 8 + nAddrByte;// ũ��(8) : ���ɾ�(2) + ������Ÿ��(2) + ���࿵��(2) + ��������(2) [byte]

                MakeHeader(nInvokeID, nReqByte);
                MakeReadOrder(nDataType, nBlockCnt, sAddress);

                foreach (byte h in TX)
                    sReqFrame += string.Format("{0:X2}", h);
                
                try {cSocket.Send(TX); }
                catch { }

                byte[] nRcvByte = new byte[RECV_BUFFER_SIZE];
                if (ReceiveData(cSocket, ref sRcvFrame, ref nRcvByte))
                {
                    if (sRcvFrame.Substring(52, 4) != "0000")
                    { return ErrCodeToErrMsg(sRcvFrame.Substring(56, 2)); }
                    
                    string s = SwapFrame(sRcvFrame.Substring((int)APP_INSTRUCTION.BLOCK, 4), 2);
                    int blockCnt = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring((int)APP_INSTRUCTION.BLOCK, 4), 2)));
                    if (nData.Length == blockCnt) RcvValueAnalysis(sRcvFrame, ref sRcvASCII, ref nData);
                }

                return null;
            }

            /// <summary>
            /// �����б�
            /// </summary>
            /// <param name="nInvokeID">INVOKE ID</param>
            /// <param name="sAddress">PLC�ּ�((�ּ� �ϳ��� �ִ�16���ڱ��� ���)�� �ݵ�� Byte(%MB100)(%MX200 = %MB40 = %MW20 = %MD10 = %ML5)</param>
            /// <param name="nByteSize">����ũ��(�ִ�1400byte)</param>
            /// <param name="sReqFrame">����������</param>
            /// <param name="sRcvFrame">����������</param>
            /// <param name="sRcvASCII">���� ����̽��� ��ȯ�� ��(���ڿ�)</param>
            /// <param name="nData">���� ����̽��� ��ȯ�� ��(�չ迭)</param>
            /// <returns>��������</returns>
            public string ReadContiPLC(Socket cSocket, int nInvokeID, string sAddress, int nByteSize,
                out string sReqFrame, out string sRcvFrame, out string sRcvASCII, out long[] nData)
            {
                sRcvASCII = sRcvFrame = sReqFrame = "";
                nData = new long[nByteSize];
                if (nByteSize > 1400) return "����Ʈ���� 1400���� ŭ";

                int nReqByte = 10 + sAddress.Length + 2; // ũ��(10+x+2) : ���ɾ�(2) + ������Ÿ��(2) + ���࿵��(2) + ��������(2) + ��������(2) + ������(x) + ������ ũ��(2) [byte]
                MakeHeader(nInvokeID, nReqByte);
                MakeReadOrder(sAddress, nByteSize);

                foreach (byte h in TX)
                    sReqFrame += string.Format("{0:X2}", h);

                try { cSocket.Send(TX); }
                catch { }

                byte[] nRcvByte = new byte[RECV_BUFFER_SIZE];
                if (ReceiveData(cSocket, ref sRcvFrame, ref nRcvByte))
                {
                    if (sRcvFrame.Substring(52, 4) != "0000")
                    { return ErrCodeToErrMsg(sRcvFrame.Substring(56, 2)); }
                        
                    int dataCnt = int.Parse(HexToDec(SwapFrame(sRcvFrame.Substring((int)APP_INSTRUCTION.DATA_SIZE, 4), 2)));
                    if (nData.Length == dataCnt) RcvValueAnalysis(sRcvFrame, ref sRcvASCII, ref nData);
                    //else "�������� �䱸ũ��� ����ũ�Ⱑ �ٸ��ϴ�.";
                }

                return null;
            }
            #endregion

            #region[���ɾ�Ÿ�Կ� ���� �б� �������� ����]
            /// <summary>
            /// �����б� ���ɿ䱸 ������ ����
            /// </summary>
            /// <param name="nDataType">������ DATA_TYPE</param>
            /// <param name="nBlockCnt">���ϰ���</param>
            /// <param name="sAddresses">PLC�ּ�</param>
            protected void MakeReadOrder(DATA_TYPE nDataType, int nBlockCnt, string[] sAddresses)
            {
                string[] sUAdd = (string[])sAddresses.Clone();
                TX[20] = Convert.ToByte(COMMAND.READ_REQ);
                TX[22] = Convert.ToByte(nDataType);
                TX[26] = Convert.ToByte(nBlockCnt);

                //EFMT ��⿡�� ������ Ÿ���� BIT�̸鼭 Ư�� �÷��װ� �ƴ� ��� �ּҺ��� �ʿ�
                //��:%MX172C -> 172*16(WORD)+12(BIT)=2764 �� %MX2764�ּҷ� ��ȯ(�ڼ��� ������ v1.6 8-11������ ����)
                if (m_nPLCType == PLC.XGT && nDataType == DATA_TYPE.BIT)
                {
                    string sMem = "", sAddNo = "", sBit = "";
                    int nWord = 0;

                    for (int k = 0; k < sUAdd.Length; k++)
                    {
                        if (sUAdd[k].Contains("%FX")) continue;//Ư�� �÷��� ��Ʈ

                        //�ּҹ��ڿ����� �޸𸮿����� �ּҺи�
                        sMem = sUAdd[k].Substring(0, 3);
                        sAddNo = sUAdd[k].Replace(sMem, "");
                        //��ȯ �ּ� ������ ���� ����� ��Ʈ ������ �и�
                        nWord = int.Parse(sAddNo.Substring(0, sAddNo.Length - 1));
                        sBit = sAddNo.Substring(sAddNo.Length - 1, 1).ToUpper();
                        //������ ����Ȯ��(0-9,A-F)
                        byte[] b = new ASCIIEncoding().GetBytes(sBit.ToCharArray());
                        b[0] = (b[0] >= 65/*'A'*/) ? b[0] -= 55 : Convert.ToByte(sBit);
                        //��ȯ�ּ� ���� �� ���Ŀ� �´� �ּ��������� ����
                        sUAdd[k] = sMem + (nWord * 16 + b[0]).ToString();
                    }
                }
                

                int i = 28;
                for (int j = 0; j < nBlockCnt; j++)
                {
                    TX[i++] = Convert.ToByte(sUAdd[j].Length);//�����̸�����
                    TX[i++] = Convert.ToByte(ASCII.NULL);
                    byte[] bs = Encoding.Default.GetBytes(sUAdd[j]);//����

                    for (int k = 0; k < sUAdd[j].Length; k++)
                    {
                        TX[i + k] = bs[k];
                    }
                    i += sUAdd[j].Length;
                }
            }

            /// <summary>
            /// �����б� ���ɿ䱸 ������ ����
            /// </summary>
            /// <param name="sAddress">PLC�ּ�</param>
            /// <param name="nByteSize">����ũ��</param>
            protected void MakeReadOrder(string sAddress, int nByteSize)
            {
                TX[20] = Convert.ToByte(COMMAND.READ_REQ);
                TX[22] = Convert.ToByte(DATA_TYPE.CONTI);
                TX[26] = 0x01;//���ϼ�(����)

                TX[28] = Convert.ToByte(sAddress.Length);//�ּ�(����)����

                byte[] nBytes = Encoding.Default.GetBytes(sAddress);//�ּ�(����)

                int i = 30;
                foreach (byte sz in nBytes)
                {
                    TX[i++] = sz;
                }
                //TX[i++] = Convert.ToByte(ByteSize);//readContiPLC() �Լ��� nReqByte���� ������ 2byte
                byte[] z = BitConverter.GetBytes(nByteSize);
                TX[i++] = z[0];
                TX[i++] = z[1];
            }
            #endregion

            #region[PLC����]
            /// <summary>
            /// ��������
            /// </summary>
            /// <param name="nDataType">������ DATA_TYPE(CONTI ����)</param>
            /// <param name="nBlockCnt">�ִ�16�������� h00~h10����(1����=��������+����)</param>
            /// <param name="sAddress">PLC�ּ�(�ּ� �ϳ��� �ִ�16���ڱ��� ���) ��)%MX100</param>
            /// <param name="oValue">��</param>
            /// <param name="sReqFrame">����������</param>
            /// <param name="sRcvFrame">����������</param>
            /// <returns>��������</returns>
            public string WriteSprtPLC(Socket cSocket, int nInvokeID, DATA_TYPE nDataType, string[] sAddress, object[] oValue,
                out string sReqFrame, out string sRcvFrame)
            {
                sRcvFrame = sReqFrame = "";
                int nBlockCnt = sAddress.Length;
                if (nBlockCnt > 16) return "���ϼ��� 16���� ŭ";
                if (sAddress.Length != oValue.Length) return "������ ������ �������� ������ �ٸ�";

                int nTemp = 0;
                int nReqByte = 8; // ũ��(8) : ���ɾ�(2) + ������Ÿ��(2) + ���࿵��(2) + ��������(2) [byte]
                //������ Ÿ�Կ� ���� ������ ũ�� �ڵ�����
                int nDataSize = 0;
                if (nDataType == DATA_TYPE.BIT || nDataType == DATA_TYPE.BYTE) nDataSize = 1;
                else if (nDataType == DATA_TYPE.WORD) nDataSize = 2;
                else if (nDataType == DATA_TYPE.DWORD) nDataSize = 4;
                else if (nDataType == DATA_TYPE.LWORD) nDataSize = 8;

                for (int i = 0; i < nBlockCnt; i++)
                    nTemp = nTemp + 2 + sAddress[i].Length + 2 + nDataSize; // ������ ����(2) + ������(x) + ������ ����(2) + ������(2)[byte]
                nReqByte = nReqByte + nTemp;

                MakeHeader(nInvokeID, nReqByte);
                MakeWriteOrder(nDataType, nBlockCnt, sAddress, nDataSize, oValue);

                foreach (byte h in TX)
                    sReqFrame += string.Format("{0:X2}", h);

                try { cSocket.Send(TX); }
                catch { }

                byte[] nRcvByte = new byte[RECV_BUFFER_SIZE];
                if (ReceiveData(cSocket, ref sRcvFrame, ref nRcvByte))
                {
                    if (sRcvFrame.Substring(52, 4) != "0000")
                    { return ErrCodeToErrMsg(sRcvFrame.Substring(56, 2)); }
                }

                return null;
            }

            /// <summary>
            /// ���Ӿ���
            /// </summary>
            /// <param name="sAddress">PLC�ּ�((�ּ� �ϳ��� �ִ�16���ڱ��� ���)�� �ݵ�� Byte ��)%MB100</param>
            /// <param name="nValue">Byteũ�� �迭����(�ִ�1400Byte)</param>
            /// <param name="sReqFrame">����������</param>
            /// <param name="sRcvFrame">����������</param>
            /// <returns>��������</returns>
            public string WriteContiPLC(Socket cSocket, int nInvokeID, string sAddress, byte[] nValue, 
                out string sReqFrame, out string sRcvFrame)
            {
                sRcvFrame = sReqFrame = "";
                if (nValue.Length > 1400) return "�������� ���� �ʰ�(1400byte)";

                int nReqByte = 10 + sAddress.Length + 2 + nValue.Length; // ũ��(10+x+2) : ���ɾ�(2) + ������Ÿ��(2) + ���࿵��(2) + ��������(2) + ��������(2) + ������(x) + ������ ũ��(2) [byte]
                MakeHeader(nInvokeID, nReqByte);
                MakeWriteOrder(sAddress, nValue);

                foreach (byte h in TX)
                    sReqFrame += string.Format("{0:X2}", h);

                try { cSocket.Send(TX); }
                catch { }

                byte[] nRcvByte = new byte[RECV_BUFFER_SIZE];
                if (ReceiveData(cSocket, ref sRcvFrame, ref nRcvByte))
                {
                    if (sRcvFrame.Substring(52, 4) != "0000")
                    { return ErrCodeToErrMsg(sRcvFrame.Substring(56, 2)); }
                }

                return null;
            }
            #endregion

            #region[���ɾ�Ÿ�Կ� ���� ���� �������� ����]
            /// <summary>
            /// �������� �䱸������ ����
            /// </summary>
            /// <param name="nDataType">������ DATA_TYPE</param>
            /// <param name="nBlockCnt">���ϰ���</param>
            /// <param name="sAddresses">PLC�ּ�</param>
            /// <param name="nDataSize">������ũ��</param>
            /// <param name="oValue">��</param>
            protected void MakeWriteOrder(DATA_TYPE nDataType, int nBlockCnt, string[] sAddresses, int nDataSize, object[] oValue)
            {
                byte nValue = 0;
                string[] sUAdd = (string[])sAddresses.Clone();
                TX[20] = Convert.ToByte(COMMAND.WRITE_REQ);
                TX[22] = Convert.ToByte(nDataType);
                TX[26] = Convert.ToByte(nBlockCnt);

#if XGT_EFMT
                //XGB_EMTA ��⿡�� ������ ���� ���ʿ�(�ϱ��ڵ� �ּ�ó��)
                //XGT_EFMT ��⿡�� ������ Ÿ���� BIT�� ��� �ּҺ��� �ʿ�
                //��:%MX172C -> 172*16(WORD)+12(BIT)=2764 �� %MX2764�ּҷ� ��ȯ
                if (m_nPLCType == PLC.XGT && nDataType == DATA_TYPE.BIT)
                {
                    string sMem = "", sAddNo = "", sBit = "";
                    int nWord = 0;

                    for (int k = 0; k < sUAdd.Length; k++)
                    {
                        if (sUAdd[k].Contains("%FX")) continue;//Ư�� �÷��� ��Ʈ

                        //�ּҹ��ڿ����� �޸𸮿����� �ּҺи�
                        sMem = sUAdd[k].Substring(0, 3);
                        sAddNo = sUAdd[k].Replace(sMem, "");
                        //��ȯ �ּ� ������ ���� ����� ��Ʈ ������ �и�
                        nWord = int.Parse(sAddNo.Substring(0, sAddNo.Length - 1));
                        sBit = sAddNo.Substring(sAddNo.Length - 1, 1).ToUpper();
                        //������ ����Ȯ��(0-9,A-F)
                        byte[] b = new ASCIIEncoding().GetBytes(sBit.ToCharArray());
                        b[0] = (b[0] >= 65/*'A'*/) ? b[0] -= 55 : Convert.ToByte(sBit);
                        //��ȯ�ּ� ���� �� ���Ŀ� �´� �ּ��������� ����
                        sUAdd[k] = sMem + (nWord * 16 + b[0]).ToString();
                    }
                }
                
#endif

                // ������ ����(2) + ������(x) + ... + ������ ����(2) + ������(2) + ... [byte] -> �˰����ϴ� ��� ���� ���� �� �� ������ �ش��ϴ� ���� ���������� ����
                int j, i = 28;

                for (j = 0; j < nBlockCnt; j++)
                {
                    TX[i++] = Convert.ToByte(sUAdd[j].Length); // ��������
                    TX[i++] = Convert.ToByte(ASCII.NULL);

                    byte[] bs = Encoding.Default.GetBytes(sUAdd[j]); // ����(�ּ�)
                    for (int k = 0; k < sUAdd[j].Length; k++)
                    {
                        TX[i++] = bs[k];
                    }
                }
                for (j = 0; j < nBlockCnt; j++)
                {
                    // ������ ũ��
                    TX[i++] = Convert.ToByte(nDataSize);
                    if (nDataSize < 256) TX[i++] = Convert.ToByte(ASCII.NULL);

                    if (nDataType > DATA_TYPE.BYTE)//256�̻��� ��(����Ʈ ������ �ʰ��� ���)
                    {
                        TX[i++] = Convert.ToByte(Convert.ToInt32(oValue[j]) & 0x00FF);//����������
                        TX[i++] = Convert.ToByte(Convert.ToInt32(oValue[j]) >> 8);// ����������
                    }
                    else//255������ ��(���޵� ���� ��Ʈ�� ����Ʈ ����)
                    {
                        nValue = Convert.ToByte(oValue[j]);
                        TX[i++] = nValue;
                    }
                }
            }

            /// <summary>
            /// ���Ӿ��� �䱸������ ����
            /// </summary>
            /// <param name="sAddress">PLC�ּ�</param>
            /// <param name="nValue">��</param>
            protected void MakeWriteOrder(string sAddress, byte[] nValue)
            {
                TX[20] = Convert.ToByte(COMMAND.WRITE_REQ);
                TX[22] = Convert.ToByte(DATA_TYPE.CONTI);
                TX[26] = 0x01;//���ϼ�(����)

                // ������ ����(2) + ������(x) + ������ ����(2) + ������ [byte]
                TX[28] = Convert.ToByte(sAddress.Length);//����������

                byte[] nBytes = Encoding.Default.GetBytes(sAddress);//������

                int i = 30;
                foreach (byte sz in nBytes)
                {
                    TX[i++] = sz;
                }

                TX[i++] = Convert.ToByte(nValue.Length);//�����Ͱ���
                if (nValue.Length < 256) TX[i++] = Convert.ToByte(ASCII.NULL);

                foreach (byte b in nValue) TX[i++] = b;//������
            }
            #endregion

            #region[�񵿱� �����б�]
            /*
            byte[] m_recvBuf = new byte[RECV_BUFFER_SIZE];
            public static Queue<string> ASYNC_RCV_DATA=new Queue<string>();

            /// <summary>
            /// �񵿱� �����б�
            /// </summary>
            /// <param name="sCompanyID">"LGIS-GLOFA" Ȥ�� "LSIS-XGT"</param>
            /// <param name="nDataType">������ DATA_TYPE(CONTI ����)</param>
            /// <param name="nBlockCnt">�ִ�16�������� h00~h10����(1����=��������+����)</param>
            /// <param name="sAddress">PLC�ּ�(�ּ� �ϳ��� �ִ�16���ڱ��� ���) ��)%MX100</param>
            /// <param name="sReqFrame">����������</param>
            public void AsyncReadSprtPLC(Socket cSocket, int nInvokeID, DATA_TYPE nDataType, string[] sAddress, 
                out string sReqFrame)
            {
                sReqFrame = "";
                int nAddrByte = 0, nReqByte = 0;
                int nBlockCnt = sAddress.Length;

                for (int i = 0; i < nBlockCnt; i++)
                    nAddrByte = nAddrByte + 2 + sAddress[i].Length;
                nReqByte = 8 + nAddrByte;// ũ��(8) : ���ɾ�(2) + ������Ÿ��(2) + ���࿵��(2) + ��������(2) [byte]

                MakeHeader(nInvokeID, nReqByte);
                MakeReadOrder(nDataType, nBlockCnt, sAddress);

                foreach (byte h in TX)
                    sReqFrame += string.Format("{0:X2}", h);

                cSocket.BeginSend(TX, 0, TX.Length, SocketFlags.None, new AsyncCallback(SendCallBack), TX);
            }

            /// <summary>
            /// �񵿱� �����б�
            /// </summary>
            /// <param name="sCompanyID">"LGIS-GLOFA" Ȥ�� "LSIS-XGT"</param>
            /// <param name="nInvokeID">INVOKE ID</param>
            /// <param name="sAddress">PLC�ּ�((�ּ� �ϳ��� �ִ�16���ڱ��� ���)�� �ݵ�� Byte(%MB100)(%MX200 = %MB40 = %MW20 = %MD10 = %ML5)</param>
            /// <param name="nByteSize">����ũ��(�ִ�1400byte)</param>
            /// <param name="sReqFrame">����������</param>
            public void AsyncReadContiPLC(Socket cSocket, int nInvokeID, string sAddress, int nByteSize, out string sReqFrame)
            {
                sReqFrame = "";
                int nReqByte = 10 + sAddress.Length + 2; // ũ��(10+x+2) : ���ɾ�(2) + ������Ÿ��(2) + ���࿵��(2) + ��������(2) + ��������(2) + ������(x) + ������ ũ��(2) [byte]
                MakeHeader(nInvokeID, nReqByte);
                MakeReadOrder(sAddress, nByteSize);

                foreach (byte h in TX)
                    sReqFrame += string.Format("{0:X2}", h);

                cSocket.BeginSend(TX, 0, TX.Length, SocketFlags.None, new AsyncCallback(SendCallBack), TX);
            }

            /// <summary>
            /// �񵿱� �۽ſϷ� �� �񵿱� ���Ŵ��
            /// </summary>
            /// <param name="ar"></param>
            private void SendCallBack(Socket cSocket, IAsyncResult ar)
            {
                if (IsConnectedTCP(cSocket))
                {
                    cSocket.EndSend(ar);
                    AsyncReceiveData(cSocket);
                }
            }

            /// <summary>
            /// �񵿱� ���Ž���
            /// </summary>
            private void AsyncReceiveData(Socket cSocket)
            {
                m_recvBuf = new byte[RECV_BUFFER_SIZE];
                cSocket.BeginReceive(m_recvBuf,
                    0, RECV_BUFFER_SIZE,
                    SocketFlags.None,
                    new System.AsyncCallback(CallBack_Recv), m_recvBuf);
            }

            /// <summary>
            /// �񵿱� ���ſϷ�
            /// </summary>
            /// <param name="ar"></param>
            private void CallBack_Recv(Socket cSocket, System.IAsyncResult ar)
            {
                int nLen;
                string sRecv = "";

                try
                {
                    nLen = cSocket.EndReceive(ar);
                    if (nLen > 0)
                    {
                        for (int i = 0; i < nLen; i++)
                            sRecv += string.Format("{0:X2}", m_recvBuf[i]);

                        ASYNC_RCV_DATA.Enqueue(sRecv);
                    }

                    //�� �ڵ� ���Խ� ���ܹ߻�("��⿭�� �Ǵ� ���۰� �����Ͽ� ���Ͽ��� �ش� �۾��� �������� ���߽��ϴ�.")
                    //try
                    //{
                    //    sock.BeginReceive(m_recvBuf,
                    //        0, RECV_BUFFER_SIZE,
                    //        System.Net.Sockets.SocketFlags.None,
                    //        new System.AsyncCallback(CallBack_Recv), m_recvBuf);
                    //}
                    //catch { }
                }
                catch (System.Exception e)
                {
                    cSocket.Close();
                    return;
                }
            }*/
            #endregion

            #region[��ſ����ڵ�]
            string ErrCodeToErrMsg(string sHexErrCode)//v2.0
            {
                string sMsg = "";
                int nErrCode = 0;

                foreach (Match m in (new Regex(@"[0-9A-F]{2,2}", RegexOptions.IgnoreCase)).Matches(sHexErrCode))
                    nErrCode = Convert.ToInt32(m.Value, 16);

                switch (nErrCode)
                {
                    case 1: sMsg = "���� �б�/���� ��û�� ���� ���� 16���� ŭ"; break;
                    case 2: sMsg = "X, B, W, D, L�� �ƴ� ������ Ÿ�� ����"; break;
                    case 3: sMsg = "���� ���� �ʴ� ����̽��� �䱸"; break;
                    case 4: sMsg = "�� ����̽��� �����ϴ� ������ �ʰ�"; break;
                    case 5: sMsg = "���� ���� ũ��(1400bytes) �ʰ�"; break;
                    case 6: sMsg = "���Ϻ� �� ũ��(1400bytes) �ʰ�"; break;
                    case 117: sMsg = "���� ���񽺿��� ������ ����� ���� �κ��� �߸��� ���"; break;//Company Name
                    case 118: sMsg = "���� ���񽺿��� ������ ����� Length�� �߸��� ���"; break;
                    case 119: sMsg = "���� ���񽺿��� ������ ����� Checksum�� �߸��� ���"; break;
                    case 120: sMsg = "���� ���񽺿��� ���ɾ �߸��� ���"; break;
                    //������ ��������
                    case 18: sMsg = "������ Ÿ�԰� ���� Ÿ�� ����ġ"; break;
                    
                    default: sMsg = "�� �� ���� ����"; break;
                }

                return sMsg;
            }
            string ErrCodeToErrMsg_v1(string sHexErrCode)
            {
                string sMsg = "";
                int nErrCode = 0;

                foreach (Match m in (new Regex(@"[0-9A-F]{2,2}", RegexOptions.IgnoreCase)).Matches(sHexErrCode))
                    nErrCode = Convert.ToInt32(m.Value, 16);

                switch (nErrCode)
                {
                    //��� ��� ����
                    case 1: sMsg = "��ũ�� ������ ����(��/���� �Ұ�)"; break;
                    case 3: sMsg = "��� ä�� ������ �����ϰ��� �ϴ� ��Ǻ����� �ĺ��ڰ� �������� ����"; break;
                    case 4: sMsg = "������ Ÿ���� ����ġ"; break;
                    case 5: sMsg = "Ÿ�����κ��� ������ ����"; break;
                    case 6: sMsg = "��뱹�� ��� ���ɾ �غ� ���°� �ƴ�"; break;
                    case 7: sMsg = "����Ʈ ���� ����̽� ���°� ���ϴ� ���°� �ƴ�"; break;
                    case 8: sMsg = "����ڰ� ���ϴ� ����� �׼����� �Ұ���"; break;
                    case 9: sMsg = "��뱹�� ��� ���ɾ �ʹ� ���� ���ſ� ���� ó�� �Ұ�"; break;
                    case 10: sMsg = "Ÿ�Ӿƿ�(TimeOut) ����"; break;
                    case 11: sMsg = "Structure ����"; break;
                    case 12: sMsg = "Abort"; break;
                    case 13: sMsg = "Reject"; break;
                    case 14: sMsg = "���ä�� ���� ����(Connect/Disconnect)"; break;
                    case 15: sMsg = "���� ��� �� Ŀ�ؼ� ���� ����"; break;
                    case 33: sMsg = "���� �ĺ��ڸ� ã�� �� ����"; break;
                    case 34: sMsg = "��巹�� ����"; break;
                    case 50: sMsg = "���� ����"; break;
                    case 113: sMsg = "Object Access Unsupported"; break;
                    case 187: sMsg = "������ �ڵ� �̿��� ���� �ڵ�� ����(Ÿ���� ��� �ڵ尪)"; break;
                    //CPU ����
                    case 16: sMsg = "��ǻ�� ��Ÿ���� ��ġ�� �߸� �����Ǿ��� ���"; break;
                    case 17: sMsg = "SLOT NO�� ������ ��� ����� �ʱ�ȭ ����"; break;
                    case 18: sMsg = "�Է� �Ķ���� ���� ����"; break;
                    case 19: sMsg = "���� ���� ����"; break;
                    case 20: sMsg = "��뱹���� �߸��� ���� ����"; break;
                    case 21: sMsg = "Time Out"; break;//��ǻ�� ��� ���κ��� ������ �������� ������ ���
                    case 80: sMsg = "Disconnection Error"; break;//���� ���� ����
                    case 82: sMsg = "Not Received Frame"; break;//������ �������� ���ŵ��� ����
                    case 84: sMsg = "Data Count Error"; break;//��Ǻ����� �Է¿� ���� ������ ������ �����ӿ� ������ ������ ������ ���� �ʰų� ����
                    case 86: sMsg = "No Match Name"; break;//��Ǻ����� �Է¿� ����� ������ �̸��� ������ ����Ʈ�� ����
                    case 87: sMsg = "Not Connected"; break;//ä���� �ξ����� �ʾ���
                    case 89: sMsg = "1m TCP Send Error"; break;//��� ���� ����
                    case 90: sMsg = "1m UDP Send Error"; break;//��� ���� ����
                    case 91: sMsg = "Socket Error"; break;
                    case 92: sMsg = "Channel Disconneted"; break;//ä�� ������
                    case 93: sMsg = "�⺻ �Ķ���� �� �������� �������� �ʾ���"; break;
                    case 94: sMsg = "ä�� ���� ����"; break;
                    case 96: sMsg = "�̹� ä�� ������ ����"; break;
                    case 97: sMsg = "Method Input Error"; break;//��Ǻ����� �Է¿� ���� Method�� �ٸ��� ����
                    case 101: sMsg = "ä�� ��ȣ ���� ����"; break;
                    case 102: sMsg = "��뱹 ���� ����(�缳��)"; break;
                    case 103: sMsg = "Ŀ�ؼ� ���"; break;
                    case 104: sMsg = "������ IP�� ���� ��뱹�� ��Ʈ��ũ�� �������� ����"; break;
                    case 105: sMsg = "��뱹�� PASSIVE ��Ʈ�� �������� ����"; break;
                    case 106: sMsg = "��� �ð��� ���� ä�� ����"; break;
                    case 107: sMsg = "��Ǻ��� ä�� ���� ���� �ʰ�"; break;//��Ǻ��� ä�� ���� ���� = 16 - ���� ���� ����
                    case 108: sMsg = "�ִ� �۽� ���� �ʰ�"; break;//�ƽ�Ű ������ ���� = ���� ������ * 2 -> �ƽ�Ű ������ ���� 1400 ����Ʈ �ʰ� �Ұ�)
                    case 117: sMsg = "���� ���񽺿��� ������ ����� ���� �κ��� �߸��� ���"; break;//Company Name
                    case 118: sMsg = "���� ���񽺿��� ������ ����� Length�� �߸��� ���"; break;
                    case 119: sMsg = "���� ���񽺿��� ������ ����� Checksum�� �߸��� ���"; break;
                    case 120: sMsg = "���� ���񽺿��� ���ɾ �߸��� ���"; break;
                    case 121: sMsg = "���� ���񽺿��� �㰡���� ���� ������ Domain/PI ���񽺸� �䱸�� ���"; break;//UDP������ Domain/PI�� ����� �� ����, �̹� TCP�� �̿��Ͽ� Domain/PI�� ����ϰ� �ִ� ��� �ٸ� ������ Domain/PI ���񽺸� �䱸�� ��쿡 �����߻�

                    default: sMsg = "�� �� ���� ����"; break;
                }

                return sMsg;
            }
            #endregion




            //XGKECore ���̺귯������ �Űܿ� �Ϻ�

            #region[TCP��Ʈ ����/�ݱ�]
            public bool IsValidIP(string sIPv4)
            {
                string[] sIP = sIPv4.Split('.');
                if (sIP.Length != 4) return false;

                int nRange;
                foreach (string s in sIP)
                {
                    if (!int.TryParse(s, out nRange)) return false;
                    if (nRange < 0 || nRange > 255) return false;
                }

                return true;
            }

            public EECode OpenEthernetPort(Socket cSocket, string sIPv4, out Exception NET_EXCEPTION)
            {
                NET_EXCEPTION = null;
                if (IsValidIP(sIPv4))
                {
                    if (!IsConnectedTCP(cSocket))
                    {
                        if (!OpenTCP(cSocket, sIPv4, out NET_EXCEPTION))
                        { return EECode.NOT_OPEN_TCP_PORT; }//LAN ���ӻ���(Cable/IP) Ȯ��
                    }
                }
                else { return EECode.WRONG_IP; }
                IP = sIPv4;

                return EECode.NONE;
            }

            public void CloseEthernetPort(Socket cSocket)
            {
                if (IsConnectedTCP(cSocket)) CloseTCP(cSocket);
            }
            #endregion

            #region[�����б�]
            public TRCV_DATAFORMAT ReadSingleData(Socket cSocket, LSIS_FEnet.DATA_TYPE nUnit, params string[] sAddr/*16������*/)
            {
                //EFMT(XGT)��⿡�� BIT���� ������ �� ��� �ּҺ��� �ʿ�(EUTB(GLOFA)��⿡���� �Էµ� �ּҷ� BIT���� �б� ���� - �ڼ��� ������ ��ǰ �Ŵ��� ����)
                TRCV_DATAFORMAT rcvData = new TRCV_DATAFORMAT();
#if CHECK_MEMORY
            if (!ChkMemoryAddr(ref sAddr)) return rcvData;
#endif

                rcvData.ERROR_MSG = ReadSprtPLC(cSocket, 1, nUnit, sAddr,
                    out rcvData.REQ_FRAME, out rcvData.RCV_FRAME, out rcvData.RCV_ASCII, out rcvData.DATA);

                return rcvData;
            }

            public TRCV_DATAFORMAT ReadMultiData(Socket cSocket, /*LSIS_FEnet.DATA_TYPE.BYTE ����*/string sAddr, int nLen)
            {
                //�����б��� ��� �ּҸ� �ݵ�� BYTE ������ �����ؾ� ��
                TRCV_DATAFORMAT rcvData = new TRCV_DATAFORMAT();
#if CHECK_MEMORY
            if (!ChkMemoryAddr(ref sAddr)) return rcvData;
#endif

                rcvData.ERROR_MSG = ReadContiPLC(cSocket, 2, sAddr, nLen,
                    out rcvData.REQ_FRAME, out rcvData.RCV_FRAME, out rcvData.RCV_ASCII, out rcvData.DATA);

                return rcvData;
            }
            #endregion

            #region[���⾲��]
            public TRCV_DATAFORMAT WriteSingleData(Socket cSocket, LSIS_FEnet.DATA_TYPE nUnit, string sAddr, object nData)
            {
                //EFMT(XGT)��⿡�� BIT���� ������ �� ��� �ּҺ��� �ʿ�(EUTB(GLOFA)��⿡���� �Էµ� �ּҷ� BIT���� �б� ���� - �ڼ��� ������ ��ǰ �Ŵ��� ����)
                TRCV_DATAFORMAT rcvData = new TRCV_DATAFORMAT();
#if CHECK_MEMORY
            if (!ChkMemoryAddr(ref sAddr)) return rcvData;
#endif

                rcvData.ERROR_MSG = WriteSprtPLC(cSocket, 3, nUnit, new string[] { sAddr }, new object[] { nData },
                    out rcvData.REQ_FRAME, out rcvData.RCV_FRAME);

                return rcvData;
            }

            public TRCV_DATAFORMAT WriteSingleData(Socket cSocket, LSIS_FEnet.DATA_TYPE nUnit, string[] sAddr/*16������*/, object[] nData)
            {
                //EFMT(XGT)��⿡�� BIT���� ������ �� ��� �ּҺ��� �ʿ�
                //(EUTB(GLOFA)��⿡���� �Էµ� �ּҷ� BIT���� �б� ���� - �ڼ��� ������ ��ǰ �Ŵ��� ����)
                TRCV_DATAFORMAT rcvData = new TRCV_DATAFORMAT();
#if CHECK_MEMORY
            if (!ChkMemoryAddr(ref sAddr)) return rcvData;
#endif

                rcvData.ERROR_MSG = WriteSprtPLC(cSocket, 4, nUnit, sAddr, nData,
                    out rcvData.REQ_FRAME, out rcvData.RCV_FRAME);

                return rcvData;
            }

            //NET3��뿹:XGK.WriteMultiData("%DB50", (byte[])XGK.SndToDevice<byte>("4567", 4));
            //NET4��뿹:XGK.WriteMultiData("%DB50", XGK.SndToDevice<byte>("4567", 4));
            public TRCV_DATAFORMAT WriteMultiData(Socket cSocket, /*LSIS_FEnet.DATA_TYPE.BYTE ����*/string sAddr, params byte[] nData)
            {
                TRCV_DATAFORMAT rcvData = new TRCV_DATAFORMAT();
#if CHECK_MEMORY
            if (!ChkMemoryAddr(ref sAddr)) return rcvData;
#endif

                rcvData.ERROR_MSG = WriteContiPLC(cSocket, 5, sAddr, nData,
                    out rcvData.REQ_FRAME, out rcvData.RCV_FRAME);

                return rcvData;
            }
            #endregion

            #region[�񵿱��б�]
            
            //public void ReadAsyncSingleData(Socket cSocket, int nInvokeID, LSIS_FEnet.DATA_TYPE nUnit, params string[] sAddr)
            //{
            //    //�񵿱� ��ſ��� ���� �������� LSIS_FEnet Ŭ�������� ASYNC_RCV_DATA ���������� ���� ��
            //    string sReqData;

            //    AsyncReadSprtPLC(cSocket, nInvokeID, nUnit, sAddr, out sReqData);
            //}

            //public void ReadAsyncMultiData(Socket cSocket, /*LSIS_FEnet.DATA_TYPE.BYTE ����*/int nInvokeID, string sAddr, int nLen)
            //{
            //    //�񵿱� ��ſ��� ���� �������� LSIS_FEnet Ŭ�������� ASYNC_RCV_DATA ���������� ���� ��
            //    string sReqData;

            //    AsyncReadContiPLC(cSocket, nInvokeID, sAddr, nLen, out sReqData);
            //}
            #endregion

            #region[��Ű����Լ�]
            /// <summary>
            /// ��ŵ����� �ڷᱸ��
            /// </summary>
            public struct TRCV_DATAFORMAT
            {
                public string REQ_FRAME;
                public string RCV_FRAME;
                public string RCV_ASCII;
                public long[] DATA;
                public string ERROR_MSG;
            }

            //�Էµ� ���� ���ڷ� �����ö�
#if CONVERTER
        public object ConvertArray<T>(long[] nRcvData)
        {
            T type = default(T);
            if (type is bool)
            { return Array.ConvertAll(nRcvData, new Converter<long, bool>(NumericToBool)); }
            
            return Array.ConvertAll(nRcvData, new Converter<long, string>(NumericToString));
        }
        string NumericToString(long nVal)
        { return nVal.ToString(); }
        bool NumericToBool(long nVal)
        { return (nVal >= 1) ? true : false; }
        
#else//delegate
            public string[] ConvertStringArray(long[] nRcvData)
            {
                return Array.ConvertAll(nRcvData, delegate(long nVal) { return nVal.ToString(); });
            }
            public bool[] ConvertBoolArray(long[] nBitData)
            {
                return Array.ConvertAll(nBitData, delegate(long nVal) { return (nVal >= 1) ? true : false; });
            }
#endif
            bool ChkMemoryAddr(ref string[] sAddr)
            {
                for (int i = 0; i < sAddr.Length; i++)
                {
                    if (!ChkMemoryAddr(ref sAddr[i]))
                    { return false; }
                }

                return true;
            }

            bool ChkMemoryAddr(ref string sAddr)
            {
                if (sAddr.Length < 4) return false;
                if (sAddr.Length < 5)
                { sAddr = string.Format("{0}0{1}", sAddr.Substring(0, 3), sAddr.Substring(3, 1)); }

                return true;
            }

            public long[] AnalysisFrame(string sRcv, int nLen, ref int nID)
            {
                string sRcvASC = "";
                long[] nRcvData = new long[nLen];
                string[] sHead = RcvHeaderAnalysis(sRcv);
                nID = int.Parse(sHead[4]);
                if (nID == 10)
                {
                    if (!RcvValueAnalysis(sRcv, ref sRcvASC, ref nRcvData)) nRcvData = null;
                }
                return nRcvData;
            }

            

            /// <summary>
            /// �����α׸� ���Ϸ� ����
            /// </summary>
            /// <param name="sMsg"></param>
            public static void ReportErrLog(string sMsg)
            {
                try
                {
                    DirectoryInfo di = new DirectoryInfo(ERR_LOG_FOLDER);
                    if (!di.Exists) di.Create();
                    using (StreamWriter sw = new StreamWriter(string.Format(@"{0}\ErrorLog_{1}.txt", ERR_LOG_FOLDER, DateTime.Today.ToString("yyyyMMdd")), true))
                    { sw.WriteLine(string.Format("[{0}]{1}", DateTime.Now.ToString(), sMsg)); }
                }
                catch { }
            }

            /// <summary>
            /// 1Unit�� Bit������ ��ȯ
            /// </summary>
            /// <param name="strData">���ڿ�</param>
            /// <param name="nBitSize">��ȯ��Ʈũ��</param>
            /// <returns>bool�迭</returns>
            public bool[] UnitToBit(string sWord, int nUnit/*8,16,32*/)//������ Unit������ �����͸� 1Bit������ ��ȯ
            {
                bool[] nBit = new bool[nUnit];
                uint nData = 0;
                if (!string.IsNullOrEmpty(sWord))
                {
                    for (int i = 0; i < nUnit; i++)
                    {
                        //PLC���� ���� �����ʹ� 16�����̳� PC������ 10������ �ν��Ͽ� 16������ 10������ ��ȯ �� ��Ʈ�����Ͽ� ���̳ʸ� �� ����
                        //data = (uint)Convert.ToInt32(strWord, WORD) & (uint)(0x01 << (byte)i);
                        nData = uint.Parse(sWord) & (uint)(0x01 << (byte)i);
                        if (nData > 0) nBit[i] = true;
                        else nBit[i] = false;
                    }
                }
                //else bit = m_bCcrRevM[nIndex];//����̻����� strWord���� null�� ��� ���� �� ����

                return nBit;
            }

            /// <summary>
            /// PLC �޸𸮿��� �޾ƿ� ���ڵ����͸� ���ڿ��� ��ȯ
            /// </summary>
            /// <param name="nVal">���ڵ�����</param>
            /// <returns>���ڿ�</returns>
            public string[] RcvFromDevice(int[] nVal)//TRCV_DATAFORMAT�� RCV_ASCII�� ����(TRCV_DATAFORMAT�� RCV_ASCII��� ����)
            {
                StringBuilder sb = new StringBuilder();
                //ASCIIEncoding ascii = new ASCIIEncoding();

                for (int j = 0; j < nVal.Length; j++)
                {
                    int nIndex = 0;
                    string sDecToHex = Convert.ToString(nVal[j], 16).PadLeft(4, '0');//10���� ���ڸ� 10���� ������ ��ȯ �� 16���� ���ڷ� ��ȯ
                    string[] sAscii = new string[sDecToHex.Length / 2];
                    for (int i = 0; i < sDecToHex.Length; i += 2)
                    {
                        sAscii[nIndex] = ASCIIEncoding.ASCII.GetString(new byte[] { Convert.ToByte(sDecToHex.Substring(i, 2), 16) });//�ƽ�Ű �ڵ庯ȯ
                        nIndex++;
                    }
                    string sSwing = sAscii[1] + sAscii[0];//���ڿ���ȯ
                    sb.Append(sSwing);
                }
                string s = sb.ToString();
                string[] nData = s.Split(new char[] { '\0' }, StringSplitOptions.RemoveEmptyEntries);

                return nData;
            }

            //TO ASCII
#if NET4
        public dynamic SndToDevice<T>(string sData, int nDataLen = -1/*-1�ϰ�� ���ڿ� ���缭 �ڵ����� ��������*/)//PLC�� �� ���ڸ� �ƽ�Ű �ڵ� ������ ��ȯ
        {
            T type = default(T);
            if (nDataLen == -1) nDataLen = sData.Length;
            else//��������� ���˺���
            {
                if (nLimit % 2 != 0) nLimit++;
                if (sData.Length < nLimit)//�Էµ� �������ڵ尡 ������ �ڸ����� ���� ��� �տ������� '0'�� ä�� ������ �ڸ��� ����
                {
                    do { sData = sData + "0"; } while (sData.Length != nLimit);
                }
                else if (sData.Length > nLimit)
                { sData = sData.Substring(0, nLimit); }
            }

            char[] sz = sData.ToCharArray();//���ڿ��� ���ڹ迭�� ��ȯ
            byte[] nASC = ASCIIEncoding.ASCII.GetBytes(sz);//�� ���ڸ� �ƽ�Ű �ڵ� �� �迭�� ��ȯ
            if (type is byte) return nASC;//ASCII

            int[] nNum = new int[sz.Length / 2];
            try
            {
                int j = 0;
                for (int i = 0; i < nASC.Length; )
                {
                    //��������Ʈ & ��������Ʈ �����Ͽ� ���� ������ ����
                    nNum[j++] = (nASC[i + 1] << 8) | nASC[i]; i += 2;//����
                }
            }
            catch { }
            
            return nNum;//����
        }
#endif

#if NET3
        public object SndToDevice<T>(string sData, int nDataLen = -1/*-1�ϰ�� ���ڿ� ���缭 �ڵ����� ��������*/)//PLC�� �� ���ڸ� �ƽ�Ű �ڵ� ������ ��ȯ
        {
            T type = default(T);
            if (nDataLen == -1) nDataLen = sData.Length;
            else//��������� ���˺���
            {
                if (nDataLen % 2 != 0) nDataLen++; //¦���� ���� ��
                if (sData.Length < nDataLen)//�Էµ� �������ڵ尡 ������ �ڸ����� ���� ��� �տ������� '0'�� ä�� ������ �ڸ��� ����
                {
                    do { sData = sData + "0"; } while (sData.Length != nDataLen);
                }
                else if (sData.Length > nDataLen)
                { sData = sData.Substring(0, nDataLen); }
            }

            byte[] nASC = ASCIIEncoding.ASCII.GetBytes(sData);//�� ���ڸ� �ƽ�Ű �ڵ� �� �迭�� ��ȯ
            if (type is byte) return nASC;//ASCII

            char[] sz = sData.ToCharArray();//���ڿ��� ���ڹ迭�� ��ȯ
            int[] nNum = new int[sz.Length / 2];
            try
            {
                int j = 0;
                for (int i = 0; i < nASC.Length; )
                {
                    //��������Ʈ & ��������Ʈ �����Ͽ� ���� ������ ����
                    nNum[j++] = (nASC[i + 1] << 8) | nASC[i]; i += 2;//����
                }
            }
            catch { }

            return nNum;//����
        }
#endif
            #endregion
        }
    }
}