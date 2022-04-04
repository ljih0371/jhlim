namespace OPCClient
{
    partial class FormMain
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.lblProgID = new System.Windows.Forms.Label();
            this.txtProgID = new System.Windows.Forms.TextBox();
            this.btnConnect = new System.Windows.Forms.Button();
            this.btnDisconnect = new System.Windows.Forms.Button();
            this.lblNotConnected = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // lblProgID
            // 
            this.lblProgID.AutoSize = true;
            this.lblProgID.Location = new System.Drawing.Point(15, 15);
            this.lblProgID.Name = "lblProgID";
            this.lblProgID.Size = new System.Drawing.Size(49, 13);
            this.lblProgID.TabIndex = 0;
            this.lblProgID.Text = "ProgID : ";
            // 
            // txtProgID
            // 
            this.txtProgID.Location = new System.Drawing.Point(70, 12);
            this.txtProgID.Name = "txtProgID";
            this.txtProgID.Size = new System.Drawing.Size(148, 20);
            this.txtProgID.TabIndex = 1;
            // 
            // btnConnect
            // 
            this.btnConnect.Location = new System.Drawing.Point(224, 11);
            this.btnConnect.Name = "btnConnect";
            this.btnConnect.Size = new System.Drawing.Size(81, 20);
            this.btnConnect.TabIndex = 2;
            this.btnConnect.Text = "Connect";
            this.btnConnect.UseVisualStyleBackColor = true;
            this.btnConnect.Click += new System.EventHandler(this.btnConnect_Click);
            // 
            // btnDisconnect
            // 
            this.btnDisconnect.Location = new System.Drawing.Point(311, 11);
            this.btnDisconnect.Name = "btnDisconnect";
            this.btnDisconnect.Size = new System.Drawing.Size(81, 20);
            this.btnDisconnect.TabIndex = 3;
            this.btnDisconnect.Text = "Disconnect";
            this.btnDisconnect.UseVisualStyleBackColor = true;
            this.btnDisconnect.Click += new System.EventHandler(this.btnDisconnect_Click);
            // 
            // lblNotConnected
            // 
            this.lblNotConnected.AutoSize = true;
            this.lblNotConnected.Location = new System.Drawing.Point(431, 15);
            this.lblNotConnected.Name = "lblNotConnected";
            this.lblNotConnected.Size = new System.Drawing.Size(82, 13);
            this.lblNotConnected.TabIndex = 4;
            this.lblNotConnected.Text = "Not Connected.";
            // 
            // FormMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(687, 423);
            this.Controls.Add(this.lblNotConnected);
            this.Controls.Add(this.btnDisconnect);
            this.Controls.Add(this.btnConnect);
            this.Controls.Add(this.txtProgID);
            this.Controls.Add(this.lblProgID);
            this.Name = "FormMain";
            this.Text = "OPC Client";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label lblProgID;
        private System.Windows.Forms.TextBox txtProgID;
        private System.Windows.Forms.Button btnConnect;
        private System.Windows.Forms.Button btnDisconnect;
        private System.Windows.Forms.Label lblNotConnected;
    }
}

