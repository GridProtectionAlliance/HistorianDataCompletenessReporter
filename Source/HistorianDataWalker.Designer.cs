namespace HistorianDataWalker
{
    partial class HistorianDataWalker
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(HistorianDataWalker));
            this.groupBoxSourceHistorian = new System.Windows.Forms.GroupBox();
            this.labelSourceHistorianInstanceName = new System.Windows.Forms.Label();
            this.textBoxHistorianInstanceName = new System.Windows.Forms.TextBox();
            this.labelSourceHistorianHostAddress = new System.Windows.Forms.Label();
            this.textBoxHistorianHostAddress = new System.Windows.Forms.TextBox();
            this.maskedTextBoxHistorianDataPort = new System.Windows.Forms.MaskedTextBox();
            this.maskedTextBoxHistorianMetadataPort = new System.Windows.Forms.MaskedTextBox();
            this.labelSourceHistorianDataPort = new System.Windows.Forms.Label();
            this.labelSourceHistorianMetaDataPort = new System.Windows.Forms.Label();
            this.progressBar = new System.Windows.Forms.ProgressBar();
            this.groupBoxMessages = new System.Windows.Forms.GroupBox();
            this.textBoxMessageOutput = new System.Windows.Forms.TextBox();
            this.buttonGo = new System.Windows.Forms.Button();
            this.groupBoxOptions = new System.Windows.Forms.GroupBox();
            this.checkBoxUseUTCTime = new System.Windows.Forms.CheckBox();
            this.textBox1 = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.buttonBrowse = new System.Windows.Forms.Button();
            this.textBoxDestination = new System.Windows.Forms.TextBox();
            this.labelDestination = new System.Windows.Forms.Label();
            this.labelPointList = new System.Windows.Forms.Label();
            this.textBoxPointList = new System.Windows.Forms.TextBox();
            this.checkBoxEnableLogging = new System.Windows.Forms.CheckBox();
            this.maskedTextBoxMessageInterval = new System.Windows.Forms.MaskedTextBox();
            this.labelMessageInterval = new System.Windows.Forms.Label();
            this.labelSeconds = new System.Windows.Forms.Label();
            this.labelPerSec = new System.Windows.Forms.Label();
            this.maskedTextBoxMetadataTimeout = new System.Windows.Forms.MaskedTextBox();
            this.labelFrameRate = new System.Windows.Forms.Label();
            this.labelMetaDataTimeout = new System.Windows.Forms.Label();
            this.maskedTextBoxFrameRate = new System.Windows.Forms.MaskedTextBox();
            this.labelEndTime = new System.Windows.Forms.Label();
            this.dateTimePickerEndTime = new System.Windows.Forms.DateTimePicker();
            this.labelStartTime = new System.Windows.Forms.Label();
            this.dateTimePickerSourceTime = new System.Windows.Forms.DateTimePicker();
            this.groupBoxSourceHistorian.SuspendLayout();
            this.groupBoxMessages.SuspendLayout();
            this.groupBoxOptions.SuspendLayout();
            this.SuspendLayout();
            // 
            // groupBoxSourceHistorian
            // 
            this.groupBoxSourceHistorian.Controls.Add(this.labelSourceHistorianInstanceName);
            this.groupBoxSourceHistorian.Controls.Add(this.textBoxHistorianInstanceName);
            this.groupBoxSourceHistorian.Controls.Add(this.labelSourceHistorianHostAddress);
            this.groupBoxSourceHistorian.Controls.Add(this.textBoxHistorianHostAddress);
            this.groupBoxSourceHistorian.Controls.Add(this.maskedTextBoxHistorianDataPort);
            this.groupBoxSourceHistorian.Controls.Add(this.maskedTextBoxHistorianMetadataPort);
            this.groupBoxSourceHistorian.Controls.Add(this.labelSourceHistorianDataPort);
            this.groupBoxSourceHistorian.Controls.Add(this.labelSourceHistorianMetaDataPort);
            this.groupBoxSourceHistorian.Location = new System.Drawing.Point(8, 12);
            this.groupBoxSourceHistorian.Margin = new System.Windows.Forms.Padding(2);
            this.groupBoxSourceHistorian.Name = "groupBoxSourceHistorian";
            this.groupBoxSourceHistorian.Padding = new System.Windows.Forms.Padding(2);
            this.groupBoxSourceHistorian.Size = new System.Drawing.Size(194, 180);
            this.groupBoxSourceHistorian.TabIndex = 0;
            this.groupBoxSourceHistorian.TabStop = false;
            this.groupBoxSourceHistorian.Text = "&Source Historian";
            // 
            // labelSourceHistorianInstanceName
            // 
            this.labelSourceHistorianInstanceName.AutoSize = true;
            this.labelSourceHistorianInstanceName.Location = new System.Drawing.Point(7, 107);
            this.labelSourceHistorianInstanceName.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelSourceHistorianInstanceName.Name = "labelSourceHistorianInstanceName";
            this.labelSourceHistorianInstanceName.Size = new System.Drawing.Size(82, 13);
            this.labelSourceHistorianInstanceName.TabIndex = 6;
            this.labelSourceHistorianInstanceName.Text = "Instance Name:";
            this.labelSourceHistorianInstanceName.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // textBoxHistorianInstanceName
            // 
            this.textBoxHistorianInstanceName.Location = new System.Drawing.Point(93, 104);
            this.textBoxHistorianInstanceName.Margin = new System.Windows.Forms.Padding(2);
            this.textBoxHistorianInstanceName.Name = "textBoxHistorianInstanceName";
            this.textBoxHistorianInstanceName.Size = new System.Drawing.Size(41, 20);
            this.textBoxHistorianInstanceName.TabIndex = 7;
            this.textBoxHistorianInstanceName.Text = "PPA";
            this.textBoxHistorianInstanceName.TextChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // labelSourceHistorianHostAddress
            // 
            this.labelSourceHistorianHostAddress.AutoSize = true;
            this.labelSourceHistorianHostAddress.Location = new System.Drawing.Point(16, 23);
            this.labelSourceHistorianHostAddress.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelSourceHistorianHostAddress.Name = "labelSourceHistorianHostAddress";
            this.labelSourceHistorianHostAddress.Size = new System.Drawing.Size(73, 13);
            this.labelSourceHistorianHostAddress.TabIndex = 0;
            this.labelSourceHistorianHostAddress.Text = "Host Address:";
            this.labelSourceHistorianHostAddress.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // textBoxHistorianHostAddress
            // 
            this.textBoxHistorianHostAddress.Location = new System.Drawing.Point(93, 20);
            this.textBoxHistorianHostAddress.Margin = new System.Windows.Forms.Padding(2);
            this.textBoxHistorianHostAddress.Name = "textBoxHistorianHostAddress";
            this.textBoxHistorianHostAddress.Size = new System.Drawing.Size(88, 20);
            this.textBoxHistorianHostAddress.TabIndex = 1;
            this.textBoxHistorianHostAddress.Text = "localhost";
            this.textBoxHistorianHostAddress.TextChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // maskedTextBoxHistorianDataPort
            // 
            this.maskedTextBoxHistorianDataPort.Location = new System.Drawing.Point(93, 48);
            this.maskedTextBoxHistorianDataPort.Margin = new System.Windows.Forms.Padding(2);
            this.maskedTextBoxHistorianDataPort.Mask = "00000";
            this.maskedTextBoxHistorianDataPort.Name = "maskedTextBoxHistorianDataPort";
            this.maskedTextBoxHistorianDataPort.Size = new System.Drawing.Size(41, 20);
            this.maskedTextBoxHistorianDataPort.TabIndex = 3;
            this.maskedTextBoxHistorianDataPort.Text = "38402";
            this.maskedTextBoxHistorianDataPort.ValidatingType = typeof(int);
            this.maskedTextBoxHistorianDataPort.TextChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // maskedTextBoxHistorianMetadataPort
            // 
            this.maskedTextBoxHistorianMetadataPort.Location = new System.Drawing.Point(93, 76);
            this.maskedTextBoxHistorianMetadataPort.Margin = new System.Windows.Forms.Padding(2);
            this.maskedTextBoxHistorianMetadataPort.Mask = "00000";
            this.maskedTextBoxHistorianMetadataPort.Name = "maskedTextBoxHistorianMetadataPort";
            this.maskedTextBoxHistorianMetadataPort.Size = new System.Drawing.Size(41, 20);
            this.maskedTextBoxHistorianMetadataPort.TabIndex = 5;
            this.maskedTextBoxHistorianMetadataPort.Text = "6175";
            this.maskedTextBoxHistorianMetadataPort.ValidatingType = typeof(int);
            this.maskedTextBoxHistorianMetadataPort.TextChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // labelSourceHistorianDataPort
            // 
            this.labelSourceHistorianDataPort.AutoSize = true;
            this.labelSourceHistorianDataPort.Location = new System.Drawing.Point(34, 51);
            this.labelSourceHistorianDataPort.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelSourceHistorianDataPort.Name = "labelSourceHistorianDataPort";
            this.labelSourceHistorianDataPort.Size = new System.Drawing.Size(55, 13);
            this.labelSourceHistorianDataPort.TabIndex = 2;
            this.labelSourceHistorianDataPort.Text = "Data Port:";
            this.labelSourceHistorianDataPort.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // labelSourceHistorianMetaDataPort
            // 
            this.labelSourceHistorianMetaDataPort.AutoSize = true;
            this.labelSourceHistorianMetaDataPort.Location = new System.Drawing.Point(9, 79);
            this.labelSourceHistorianMetaDataPort.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelSourceHistorianMetaDataPort.Name = "labelSourceHistorianMetaDataPort";
            this.labelSourceHistorianMetaDataPort.Size = new System.Drawing.Size(80, 13);
            this.labelSourceHistorianMetaDataPort.TabIndex = 4;
            this.labelSourceHistorianMetaDataPort.Text = "Meta-data Port:";
            this.labelSourceHistorianMetaDataPort.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // progressBar
            // 
            this.progressBar.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.progressBar.Location = new System.Drawing.Point(8, 526);
            this.progressBar.Margin = new System.Windows.Forms.Padding(2);
            this.progressBar.Name = "progressBar";
            this.progressBar.Size = new System.Drawing.Size(598, 27);
            this.progressBar.TabIndex = 3;
            // 
            // groupBoxMessages
            // 
            this.groupBoxMessages.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom) 
            | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.groupBoxMessages.Controls.Add(this.textBoxMessageOutput);
            this.groupBoxMessages.Location = new System.Drawing.Point(8, 231);
            this.groupBoxMessages.Margin = new System.Windows.Forms.Padding(2);
            this.groupBoxMessages.Name = "groupBoxMessages";
            this.groupBoxMessages.Padding = new System.Windows.Forms.Padding(2);
            this.groupBoxMessages.Size = new System.Drawing.Size(645, 291);
            this.groupBoxMessages.TabIndex = 2;
            this.groupBoxMessages.TabStop = false;
            this.groupBoxMessages.Text = "Messages";
            // 
            // textBoxMessageOutput
            // 
            this.textBoxMessageOutput.BackColor = System.Drawing.SystemColors.WindowText;
            this.textBoxMessageOutput.Dock = System.Windows.Forms.DockStyle.Fill;
            this.textBoxMessageOutput.Font = new System.Drawing.Font("Lucida Console", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.textBoxMessageOutput.ForeColor = System.Drawing.SystemColors.Window;
            this.textBoxMessageOutput.Location = new System.Drawing.Point(2, 15);
            this.textBoxMessageOutput.Margin = new System.Windows.Forms.Padding(2);
            this.textBoxMessageOutput.Multiline = true;
            this.textBoxMessageOutput.Name = "textBoxMessageOutput";
            this.textBoxMessageOutput.ReadOnly = true;
            this.textBoxMessageOutput.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.textBoxMessageOutput.Size = new System.Drawing.Size(641, 274);
            this.textBoxMessageOutput.TabIndex = 0;
            this.textBoxMessageOutput.TabStop = false;
            // 
            // buttonGo
            // 
            this.buttonGo.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.buttonGo.Font = new System.Drawing.Font("Tahoma", 8.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.buttonGo.Location = new System.Drawing.Point(603, 526);
            this.buttonGo.Margin = new System.Windows.Forms.Padding(2);
            this.buttonGo.Name = "buttonGo";
            this.buttonGo.Size = new System.Drawing.Size(50, 27);
            this.buttonGo.TabIndex = 4;
            this.buttonGo.Text = "&Go!";
            this.buttonGo.UseVisualStyleBackColor = true;
            this.buttonGo.Click += new System.EventHandler(this.buttonGo_Click);
            // 
            // groupBoxOptions
            // 
            this.groupBoxOptions.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.groupBoxOptions.Controls.Add(this.checkBoxUseUTCTime);
            this.groupBoxOptions.Controls.Add(this.textBox1);
            this.groupBoxOptions.Controls.Add(this.label1);
            this.groupBoxOptions.Controls.Add(this.buttonBrowse);
            this.groupBoxOptions.Controls.Add(this.textBoxDestination);
            this.groupBoxOptions.Controls.Add(this.labelDestination);
            this.groupBoxOptions.Controls.Add(this.labelPointList);
            this.groupBoxOptions.Controls.Add(this.textBoxPointList);
            this.groupBoxOptions.Controls.Add(this.checkBoxEnableLogging);
            this.groupBoxOptions.Controls.Add(this.maskedTextBoxMessageInterval);
            this.groupBoxOptions.Controls.Add(this.labelMessageInterval);
            this.groupBoxOptions.Controls.Add(this.labelSeconds);
            this.groupBoxOptions.Controls.Add(this.labelPerSec);
            this.groupBoxOptions.Controls.Add(this.maskedTextBoxMetadataTimeout);
            this.groupBoxOptions.Controls.Add(this.labelFrameRate);
            this.groupBoxOptions.Controls.Add(this.labelMetaDataTimeout);
            this.groupBoxOptions.Controls.Add(this.maskedTextBoxFrameRate);
            this.groupBoxOptions.Controls.Add(this.labelEndTime);
            this.groupBoxOptions.Controls.Add(this.dateTimePickerEndTime);
            this.groupBoxOptions.Controls.Add(this.labelStartTime);
            this.groupBoxOptions.Controls.Add(this.dateTimePickerSourceTime);
            this.groupBoxOptions.Location = new System.Drawing.Point(206, 8);
            this.groupBoxOptions.Margin = new System.Windows.Forms.Padding(2);
            this.groupBoxOptions.Name = "groupBoxOptions";
            this.groupBoxOptions.Padding = new System.Windows.Forms.Padding(2);
            this.groupBoxOptions.Size = new System.Drawing.Size(447, 219);
            this.groupBoxOptions.TabIndex = 1;
            this.groupBoxOptions.TabStop = false;
            this.groupBoxOptions.Text = "&Options";
            // 
            // checkBoxUseUTCTime
            // 
            this.checkBoxUseUTCTime.AutoSize = true;
            this.checkBoxUseUTCTime.Checked = true;
            this.checkBoxUseUTCTime.CheckState = System.Windows.Forms.CheckState.Checked;
            this.checkBoxUseUTCTime.Location = new System.Drawing.Point(222, 49);
            this.checkBoxUseUTCTime.Name = "checkBoxUseUTCTime";
            this.checkBoxUseUTCTime.Size = new System.Drawing.Size(96, 17);
            this.checkBoxUseUTCTime.TabIndex = 20;
            this.checkBoxUseUTCTime.Text = "Use UTC Time";
            this.checkBoxUseUTCTime.UseVisualStyleBackColor = true;
            // 
            // textBox1
            // 
            this.textBox1.Location = new System.Drawing.Point(172, 169);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new System.Drawing.Size(100, 20);
            this.textBox1.TabIndex = 19;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(5, 172);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(161, 13);
            this.label1.TabIndex = 18;
            this.label1.Text = "Sample Count Window (Minutes)";
            // 
            // buttonBrowse
            // 
            this.buttonBrowse.Location = new System.Drawing.Point(384, 191);
            this.buttonBrowse.Name = "buttonBrowse";
            this.buttonBrowse.Size = new System.Drawing.Size(52, 23);
            this.buttonBrowse.TabIndex = 17;
            this.buttonBrowse.Text = "Browse";
            this.buttonBrowse.UseVisualStyleBackColor = true;
            // 
            // textBoxDestination
            // 
            this.textBoxDestination.Location = new System.Drawing.Point(73, 195);
            this.textBoxDestination.Name = "textBoxDestination";
            this.textBoxDestination.Size = new System.Drawing.Size(304, 20);
            this.textBoxDestination.TabIndex = 16;
            this.textBoxDestination.Text = "Desktop";
            // 
            // labelDestination
            // 
            this.labelDestination.AutoSize = true;
            this.labelDestination.Location = new System.Drawing.Point(4, 199);
            this.labelDestination.Name = "labelDestination";
            this.labelDestination.Size = new System.Drawing.Size(63, 13);
            this.labelDestination.TabIndex = 15;
            this.labelDestination.Text = "Destination:";
            // 
            // labelPointList
            // 
            this.labelPointList.AutoSize = true;
            this.labelPointList.Location = new System.Drawing.Point(10, 51);
            this.labelPointList.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelPointList.Name = "labelPointList";
            this.labelPointList.Size = new System.Drawing.Size(140, 13);
            this.labelPointList.TabIndex = 4;
            this.labelPointList.Text = "Point List / Filter Expression:";
            this.labelPointList.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // textBoxPointList
            // 
            this.textBoxPointList.Anchor = ((System.Windows.Forms.AnchorStyles)(((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Left) 
            | System.Windows.Forms.AnchorStyles.Right)));
            this.textBoxPointList.Location = new System.Drawing.Point(12, 67);
            this.textBoxPointList.Multiline = true;
            this.textBoxPointList.Name = "textBoxPointList";
            this.textBoxPointList.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.textBoxPointList.Size = new System.Drawing.Size(424, 49);
            this.textBoxPointList.TabIndex = 5;
            this.textBoxPointList.Text = "FILTER MeasurementDetail WHERE SignalAcronym IN (\'FREQ\')";
            this.textBoxPointList.TextChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // checkBoxEnableLogging
            // 
            this.checkBoxEnableLogging.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.checkBoxEnableLogging.AutoSize = true;
            this.checkBoxEnableLogging.Checked = true;
            this.checkBoxEnableLogging.CheckState = System.Windows.Forms.CheckState.Checked;
            this.checkBoxEnableLogging.Location = new System.Drawing.Point(336, 148);
            this.checkBoxEnableLogging.Margin = new System.Windows.Forms.Padding(2);
            this.checkBoxEnableLogging.Name = "checkBoxEnableLogging";
            this.checkBoxEnableLogging.Size = new System.Drawing.Size(100, 17);
            this.checkBoxEnableLogging.TabIndex = 14;
            this.checkBoxEnableLogging.Text = "Enable Logging";
            this.checkBoxEnableLogging.UseVisualStyleBackColor = true;
            this.checkBoxEnableLogging.CheckedChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // maskedTextBoxMessageInterval
            // 
            this.maskedTextBoxMessageInterval.Location = new System.Drawing.Point(110, 145);
            this.maskedTextBoxMessageInterval.Margin = new System.Windows.Forms.Padding(2);
            this.maskedTextBoxMessageInterval.Mask = "0000000";
            this.maskedTextBoxMessageInterval.Name = "maskedTextBoxMessageInterval";
            this.maskedTextBoxMessageInterval.Size = new System.Drawing.Size(53, 20);
            this.maskedTextBoxMessageInterval.TabIndex = 13;
            this.maskedTextBoxMessageInterval.Text = "2000";
            this.maskedTextBoxMessageInterval.ValidatingType = typeof(int);
            this.maskedTextBoxMessageInterval.TextChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // labelMessageInterval
            // 
            this.labelMessageInterval.AutoSize = true;
            this.labelMessageInterval.Location = new System.Drawing.Point(16, 147);
            this.labelMessageInterval.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelMessageInterval.Name = "labelMessageInterval";
            this.labelMessageInterval.Size = new System.Drawing.Size(91, 13);
            this.labelMessageInterval.TabIndex = 12;
            this.labelMessageInterval.Text = "Message Interval:";
            this.labelMessageInterval.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // labelSeconds
            // 
            this.labelSeconds.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.labelSeconds.AutoSize = true;
            this.labelSeconds.Location = new System.Drawing.Point(390, 124);
            this.labelSeconds.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelSeconds.Name = "labelSeconds";
            this.labelSeconds.Size = new System.Drawing.Size(47, 13);
            this.labelSeconds.TabIndex = 11;
            this.labelSeconds.Text = "seconds";
            // 
            // labelPerSec
            // 
            this.labelPerSec.AutoSize = true;
            this.labelPerSec.Location = new System.Drawing.Point(144, 124);
            this.labelPerSec.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelPerSec.Name = "labelPerSec";
            this.labelPerSec.Size = new System.Drawing.Size(101, 13);
            this.labelPerSec.TabIndex = 8;
            this.labelPerSec.Text = "samples per second";
            // 
            // maskedTextBoxMetadataTimeout
            // 
            this.maskedTextBoxMetadataTimeout.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.maskedTextBoxMetadataTimeout.Location = new System.Drawing.Point(355, 121);
            this.maskedTextBoxMetadataTimeout.Margin = new System.Windows.Forms.Padding(2);
            this.maskedTextBoxMetadataTimeout.Mask = "000";
            this.maskedTextBoxMetadataTimeout.Name = "maskedTextBoxMetadataTimeout";
            this.maskedTextBoxMetadataTimeout.Size = new System.Drawing.Size(31, 20);
            this.maskedTextBoxMetadataTimeout.TabIndex = 10;
            this.maskedTextBoxMetadataTimeout.Text = "60";
            this.maskedTextBoxMetadataTimeout.ValidatingType = typeof(int);
            this.maskedTextBoxMetadataTimeout.TextChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // labelFrameRate
            // 
            this.labelFrameRate.AutoSize = true;
            this.labelFrameRate.Location = new System.Drawing.Point(4, 124);
            this.labelFrameRate.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelFrameRate.Name = "labelFrameRate";
            this.labelFrameRate.Size = new System.Drawing.Size(103, 13);
            this.labelFrameRate.TabIndex = 6;
            this.labelFrameRate.Text = "Frame-rate Estimate:";
            this.labelFrameRate.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // labelMetaDataTimeout
            // 
            this.labelMetaDataTimeout.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Right)));
            this.labelMetaDataTimeout.AutoSize = true;
            this.labelMetaDataTimeout.Location = new System.Drawing.Point(252, 124);
            this.labelMetaDataTimeout.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelMetaDataTimeout.Name = "labelMetaDataTimeout";
            this.labelMetaDataTimeout.Size = new System.Drawing.Size(99, 13);
            this.labelMetaDataTimeout.TabIndex = 9;
            this.labelMetaDataTimeout.Text = "Meta-data Timeout:";
            this.labelMetaDataTimeout.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // maskedTextBoxFrameRate
            // 
            this.maskedTextBoxFrameRate.Location = new System.Drawing.Point(110, 121);
            this.maskedTextBoxFrameRate.Margin = new System.Windows.Forms.Padding(2);
            this.maskedTextBoxFrameRate.Mask = "000";
            this.maskedTextBoxFrameRate.Name = "maskedTextBoxFrameRate";
            this.maskedTextBoxFrameRate.Size = new System.Drawing.Size(31, 20);
            this.maskedTextBoxFrameRate.TabIndex = 7;
            this.maskedTextBoxFrameRate.Text = "30";
            this.maskedTextBoxFrameRate.ValidatingType = typeof(int);
            this.maskedTextBoxFrameRate.TextChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // labelEndTime
            // 
            this.labelEndTime.AutoSize = true;
            this.labelEndTime.Location = new System.Drawing.Point(227, 27);
            this.labelEndTime.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelEndTime.Name = "labelEndTime";
            this.labelEndTime.Size = new System.Drawing.Size(55, 13);
            this.labelEndTime.TabIndex = 2;
            this.labelEndTime.Text = "End Time:";
            this.labelEndTime.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // dateTimePickerEndTime
            // 
            this.dateTimePickerEndTime.CustomFormat = "MM/dd/yyyy HH:mm:ss";
            this.dateTimePickerEndTime.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.dateTimePickerEndTime.Location = new System.Drawing.Point(285, 24);
            this.dateTimePickerEndTime.Margin = new System.Windows.Forms.Padding(2);
            this.dateTimePickerEndTime.Name = "dateTimePickerEndTime";
            this.dateTimePickerEndTime.Size = new System.Drawing.Size(147, 20);
            this.dateTimePickerEndTime.TabIndex = 3;
            this.dateTimePickerEndTime.Value = new System.DateTime(2017, 1, 1, 0, 10, 0, 0);
            this.dateTimePickerEndTime.ValueChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // labelStartTime
            // 
            this.labelStartTime.AutoSize = true;
            this.labelStartTime.Location = new System.Drawing.Point(10, 27);
            this.labelStartTime.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.labelStartTime.Name = "labelStartTime";
            this.labelStartTime.Size = new System.Drawing.Size(58, 13);
            this.labelStartTime.TabIndex = 0;
            this.labelStartTime.Text = "Start Time:";
            this.labelStartTime.TextAlign = System.Drawing.ContentAlignment.MiddleRight;
            // 
            // dateTimePickerSourceTime
            // 
            this.dateTimePickerSourceTime.CustomFormat = "MM/dd/yyyy HH:mm:ss";
            this.dateTimePickerSourceTime.Format = System.Windows.Forms.DateTimePickerFormat.Custom;
            this.dateTimePickerSourceTime.Location = new System.Drawing.Point(72, 24);
            this.dateTimePickerSourceTime.Margin = new System.Windows.Forms.Padding(2);
            this.dateTimePickerSourceTime.Name = "dateTimePickerSourceTime";
            this.dateTimePickerSourceTime.Size = new System.Drawing.Size(147, 20);
            this.dateTimePickerSourceTime.TabIndex = 1;
            this.dateTimePickerSourceTime.Value = new System.DateTime(2017, 1, 1, 0, 0, 0, 0);
            this.dateTimePickerSourceTime.ValueChanged += new System.EventHandler(this.FormElementChanged);
            // 
            // HistorianDataWalker
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(659, 561);
            this.Controls.Add(this.groupBoxOptions);
            this.Controls.Add(this.buttonGo);
            this.Controls.Add(this.groupBoxMessages);
            this.Controls.Add(this.progressBar);
            this.Controls.Add(this.groupBoxSourceHistorian);
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Margin = new System.Windows.Forms.Padding(2);
            this.MinimumSize = new System.Drawing.Size(675, 600);
            this.Name = "HistorianDataWalker";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "openHistorian Data Walker Utility";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.HistorianDataWalker_FormClosing);
            this.Load += new System.EventHandler(this.HistorianDataWalker_Load);
            this.groupBoxSourceHistorian.ResumeLayout(false);
            this.groupBoxSourceHistorian.PerformLayout();
            this.groupBoxMessages.ResumeLayout(false);
            this.groupBoxMessages.PerformLayout();
            this.groupBoxOptions.ResumeLayout(false);
            this.groupBoxOptions.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBoxSourceHistorian;
        private System.Windows.Forms.Label labelSourceHistorianDataPort;
        private System.Windows.Forms.Label labelSourceHistorianMetaDataPort;
        private System.Windows.Forms.Label labelSourceHistorianHostAddress;
        private System.Windows.Forms.ProgressBar progressBar;
        private System.Windows.Forms.GroupBox groupBoxMessages;
        private System.Windows.Forms.TextBox textBoxMessageOutput;
        private System.Windows.Forms.Button buttonGo;
        private System.Windows.Forms.Label labelSourceHistorianInstanceName;
        private System.Windows.Forms.GroupBox groupBoxOptions;
        private System.Windows.Forms.Label labelEndTime;
        private System.Windows.Forms.Label labelStartTime;
        private System.Windows.Forms.Label labelFrameRate;
        private System.Windows.Forms.Label labelSeconds;
        private System.Windows.Forms.Label labelPerSec;
        private System.Windows.Forms.Label labelMetaDataTimeout;
        private System.Windows.Forms.Label labelMessageInterval;
        private System.Windows.Forms.Label labelPointList;
        public System.Windows.Forms.MaskedTextBox maskedTextBoxHistorianDataPort;
        public System.Windows.Forms.MaskedTextBox maskedTextBoxHistorianMetadataPort;
        public System.Windows.Forms.TextBox textBoxHistorianHostAddress;
        public System.Windows.Forms.TextBox textBoxHistorianInstanceName;
        public System.Windows.Forms.DateTimePicker dateTimePickerEndTime;
        public System.Windows.Forms.DateTimePicker dateTimePickerSourceTime;
        public System.Windows.Forms.MaskedTextBox maskedTextBoxFrameRate;
        public System.Windows.Forms.MaskedTextBox maskedTextBoxMetadataTimeout;
        public System.Windows.Forms.MaskedTextBox maskedTextBoxMessageInterval;
        public System.Windows.Forms.CheckBox checkBoxEnableLogging;
        public System.Windows.Forms.TextBox textBoxPointList;
        private System.Windows.Forms.TextBox textBox1;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button buttonBrowse;
        private System.Windows.Forms.TextBox textBoxDestination;
        private System.Windows.Forms.Label labelDestination;
        private System.Windows.Forms.CheckBox checkBoxUseUTCTime;
    }
}