using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using OPCAutomation;

namespace OPCClient
{
    public partial class FormMain : Form
    {
        //OPC Server 변수
        private OPCAutomation.OPCServer _OPCServer = null;
        
        public FormMain()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            //변수 초기화
            this.Initialize();
        }

        /// <summary>
        /// 변수 초기화
        /// </summary>
        private void Initialize()
        {
            _OPCServer = new OPCServer();
        }
        
        /// <summary>
        /// OPC서버와 연결
        /// </summary>
        /// <param name="Server">OPC Server</param>
        /// <returns></returns>
        private bool ConnectOPCServer(OPCAutomation.OPCServer Server, string ProgID)
        {
            try
            {
                Server.Connect(ProgID);
                return true;
            }
            catch
            {
                return false;
            }

        }

        private void btnConnect_Click(object sender, EventArgs e)
        {
            string ProgID = this.txtProgID.Text;

            if (string.IsNullOrEmpty(ProgID))
            {
                return;
            }

            //OPC서버 연결
            if (this.ConnectOPCServer(_OPCServer, ProgID))
            {
                this.lblNotConnected.Text = "Connected.";                
            }
            else
            {
                this.lblNotConnected.Text = "Not Connected.";
            }

        }

        private void btnDisconnect_Click(object sender, EventArgs e)
        {
            try
            {
                _OPCServer.Disconnect();
                this.lblNotConnected.Text = "Not Connected.";                
            }
            catch
            {
            }
        }
    }
}
