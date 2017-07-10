using System;
using System.Collections.Generic;
using System.Linq;
using GSF.Diagnostics;
using HistorianDataCompletenessReporter.HistorianAPI;
using HistorianDataCompletenessReporter.HistorianAPI.Metadata;
using System.Diagnostics;
using System.IO;
using GSF;
using GSF.IO;
using GSF.Units;

namespace HistorianDataCompletenessReporter
{
    /// <summary>
    /// Defines algorithm to be executed during historian read.
    /// </summary>
    public class Algorithm : IDisposable
    {
        #region [ Members ]

        // Nested Types
        private class SignalHandler
        {
            public string Name;
            public ulong PointID;
            public int ExpectedPoints;
            public int ReceivedPoints = 0;
            public int DataErrors = 0;
            public StreamWriter Stream;
        }

        // Meta-data fields
        private List<MetadataRecord> m_metadata;
        private int m_expectedFrameRate;
        private string m_destination;

        // Algorithm analytic fields
        private SignalHandler[] m_signalHandlers;
        private TimeSpan m_windowSize;
        private DateTime[] m_timeWindows;
        private int m_currentTimeWindow;
        private ulong[] m_expectedSignals;
        bool m_useUTCTime;

        // Algorithm processing statistic fields
        private long m_processedDataBlocks;

        private bool m_disposed;

        #endregion

        #region [ Constructors ]

        public Algorithm(DateTime startTime, DateTime endTime, TimeSpan windowSize, ulong[] expectedSignals, List<MetadataRecord> metadata, string destination, int expectedFrameRate, bool useUTCTime = false)
        {
            StartTime = startTime;
            EndTime = endTime;
            WindowSize = windowSize;
            ExpectedSignals = expectedSignals;
            ExpectedFrameRate = expectedFrameRate;
            Metadata = metadata;
            Destination = destination;
            UseUTCTime = useUTCTime;
            CalculateTimeWindows();
            InitializeSignalHandlers();
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets UI message display function.
        /// </summary>
        public Action<string> ShowMessage { get; set; }

        /// <summary>
        /// Gets or sets current message display interval.
        /// </summary>
        public int MessageInterval { get; set; }

        /// <summary>
        /// Gets or sets current logging publisher.
        /// </summary>
        public LogPublisher Log { get; set; }

        /// <summary>
        /// Gets or sets start time for data read.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets end time for data read.
        /// </summary>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets or sets total time-range, in seconds, for data read.
        /// </summary>
        public double TimeRange { get; }

        /// <summary>
        /// Gets or sets the time window size
        /// </summary>
        public TimeSpan WindowSize
        {
            get
            {
                return m_windowSize;
            }
            set
            {
                m_windowSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the expected signal ids
        /// </summary>
        public ulong[] ExpectedSignals
        {
            get
            {
                return m_expectedSignals;
            }

            set
            {
                m_expectedSignals = value;
                InitializeSignalHandlers();
            }
        }

        /// <summary>
        /// Gets or sets received historian meta-data.
        /// </summary>
        public List<MetadataRecord> Metadata
        {
            get
            {
                return m_metadata;
            }
            set
            {
                // Cache meta-data in case algorithm needs it
                m_metadata = value;
            }
        }

        public int ExpectedFrameRate
        {
            get
            {
                return m_expectedFrameRate;
            }
            set
            {
                m_expectedFrameRate = value;
            }
        }

        public string Destination
        {
            get
            {
                return m_destination;
            }
            set
            {
                m_destination = value;
                if (m_destination == null || m_destination == "")
                    m_destination = FilePath.GetAbsolutePath("");
            }
        }

        public bool UseUTCTime
        {
            get
            {
                return m_useUTCTime;
            }
            set
            {
                m_useUTCTime = value;
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Releases all the resources used by the <see cref="Algorithm"/> object.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="Algorithm"/> object and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                try
                {
                    if (disposing)
                    {
                        foreach (SignalHandler signalHandler in m_signalHandlers)
                            signalHandler.Stream?.Dispose();

                    }
                }
                finally
                {
                    m_disposed = true;  // Prevent duplicate dispose.
                }
            }
        }

        /// <summary>
        /// Calculates the start and end times of each time window based on the StartTime, EndTime, and WindowSize properties 
        /// and populates the m_timeWindows field.
        /// </summary>
        private void CalculateTimeWindows()
        {
            TimeSpan timeRangeSize = EndTime - StartTime;
            m_timeWindows = new DateTime[(timeRangeSize.Ticks + WindowSize.Ticks - 1) / WindowSize.Ticks + 1]; // Extra WindowSize.Ticks - 1 in numerator adds one to quotient if there is a remainder
            if (m_timeWindows.Length > 2)
            {
                m_timeWindows[0] = StartTime;
                m_timeWindows[m_timeWindows.Length - 1] = EndTime;
                m_currentTimeWindow = 0; // m_current time window should go from 0 to m_timeWindows.Length - 2
                for (int i = 1; i < m_timeWindows.Length - 1; i++)
                {
                    m_timeWindows[i] = StartTime + TimeSpan.FromTicks(WindowSize.Ticks * i);
                }
            }
            
            else
            {
                throw new Exception("Invalid combination start time, end time, & time window size");
            }
        }

        /// <summary>
        /// For each ulong signalID in ExpectedSignals, this function finds the name of the signal in the MetaData
        /// list and uses it to to populate it's SignalHandler Name and Stream fields. Also populates the SignalHandler's
        /// other fields.
        /// </summary>
        private void InitializeSignalHandlers()
        {
            if (Metadata != null)
            {
                m_signalHandlers = new SignalHandler[ExpectedSignals.Length];

                int expectedPoints = CalculateExpectedPoints();
                string timeZone = UseUTCTime ? "(UTC)" : "(Local Time)";
                for(int i = 0; i < ExpectedSignals.Length; i++)
                {
                    string signalName = Metadata.Find(record => record.PointID == ExpectedSignals[i]).PointTag.Replace(':', '_');
                    m_signalHandlers[i] = new SignalHandler
                    {
                        PointID = ExpectedSignals[i],
                        Name = signalName,
                        Stream = File.CreateText(Path.Combine(Destination, signalName + ".csv")),
                        ExpectedPoints = expectedPoints
                    };
                    m_signalHandlers[i].Stream.WriteLine($"{m_signalHandlers[i].Name} TimeStamp {timeZone}, Expected Samples, Sample Count, Data Error Count");
                }
            }
        }

        /// <summary>
        /// Calculates how many points should be in a TimeWindow based on it's start time, end time and ExpectedFrameRate
        /// </summary>
        /// <returns></returns>
        private int CalculateExpectedPoints()
        {
            return (int)((m_timeWindows[m_currentTimeWindow + 1] - m_timeWindows[m_currentTimeWindow]).TotalSeconds * ExpectedFrameRate);
        }

        /// <summary>
        /// Increments m_currentTimeWindow, writes each StreamHandler's data to it's StreamWriter and resets the StreamHandler's counts
        /// </summary>
        private void WriteData()
        {
            m_currentTimeWindow++;
            string timeStamp = FixTimeStamp(m_timeWindows[m_currentTimeWindow - 1]);
            for (int i = 0; i < m_signalHandlers.Length; i++)
            {
                m_signalHandlers[i].Stream.WriteLine($"{timeStamp}, {m_signalHandlers[i].ExpectedPoints},  {m_signalHandlers[i].ReceivedPoints}, {m_signalHandlers[i].DataErrors}");
                m_signalHandlers[i].ExpectedPoints = CalculateExpectedPoints();
                m_signalHandlers[i].ReceivedPoints = 0;
                m_signalHandlers[i].DataErrors = 0;
            }
        }

        /// <summary>
        /// Changes timestamp into what MS Excel will parse as a string instead of a DateTime
        /// </summary>
        /// <param name="timeStamp"></param>
        /// <returns></returns>
        private string FixTimeStamp(DateTime timeStamp)
        {
            if (!UseUTCTime)
                timeStamp = timeStamp.ToLocalTime();

            return "=\"" + timeStamp.ToString() + "\"";
        }

        /// <summary>
        /// Allows algorithm to augment point ID list provided by UI.
        /// </summary>
        /// <param name="pointIDList">Point ID list.</param>
        public void AugmentPointIDList(List<ulong> pointIDList)
        {
            // Not used in this algorithm
        }

        /// <summary>
        /// Default data processing entry point for <see cref="Algorithm"/>.
        /// </summary>
        /// <param name="timestamp">Timestamp of <paramref name="dataBlock"/>.</param>
        /// <param name="dataBlock">Points values read at current timestamp.</param>
        public void Execute(DateTime timestamp, DataPoint[] dataBlock)
        {
            while (timestamp > m_timeWindows[m_currentTimeWindow + 1])
                WriteData();

            foreach (DataPoint point in dataBlock)
            {
                int index = Array.IndexOf(m_signalHandlers, m_signalHandlers.First(signalHandler => signalHandler.PointID == point.PointID));
                m_signalHandlers[index].ReceivedPoints++;
                if (Single.IsInfinity(point.ValueAsSingle))
                    m_signalHandlers[index].DataErrors++;
            }
        }

        public void FinalWrite()
        {
            string timeStamp = FixTimeStamp(m_timeWindows[m_currentTimeWindow]);
            for (int i = 0; i < m_signalHandlers.Length; i++)
            {
                m_signalHandlers[i].Stream.WriteLine($"{timeStamp}, {m_signalHandlers[i].ExpectedPoints},  {m_signalHandlers[i].ReceivedPoints}, {m_signalHandlers[i].DataErrors}");
            }
        }

        #endregion
    }
}
