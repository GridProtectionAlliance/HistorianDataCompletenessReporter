using System;
using System.Collections.Generic;
using System.Linq;
using GSF.Diagnostics;
using HistorianDataWalker.HistorianAPI;
using HistorianDataWalker.HistorianAPI.Metadata;
using System.Diagnostics;
using System.IO;
using GSF;
using GSF.IO;
using GSF.Units;

namespace HistorianDataWalker
{
    /// <summary>
    /// Defines algorithm to be executed during historian read.
    /// </summary>
    public class Algorithm : IDisposable
    {
        #region [ Members ]

        private class Imbalance
        {
            public ulong Start;
            public ulong Stop;
            public double Total;
            public long Count;

            public double Range => (new Ticks((long)Stop) - new Ticks((long)Start)).ToSeconds();

            public void Reset()
            {
                Start = 0UL;
                Stop = 0UL;
                Total = 0.0D;
                Count = 0L;
            }
        }

        // Constants
        private const int MinimumSustainedRange = 1;
        private const double ImbalanceThreshold = 0.01D;
        private const string OutputFile = "Output for {0}.txt";

        // Meta-data fields
        private List<MetadataRecord> m_metadata;
        private readonly ulong[][] m_voltageMagnitudeIDs;
        private readonly string[] m_voltageSetDescription;

        // Algorithm analytic fields
        private readonly StreamWriter[] m_outputFiles;
        private readonly StreamWriter m_allOutFile;
        private readonly Imbalance[] m_imbalances;

        // Algorithm processing statistic fields
        private long m_missingPointCount;
        private long m_processedDataBlocks;

        private bool m_disposed;

        #endregion

        #region [ Constructors ]

        public Algorithm()
        {
            m_voltageMagnitudeIDs = new[]
            {
                new ulong[] { 5070, 5003 },
                new ulong[] { 2108, 2110 },
                new ulong[] { 2159, 2161 },
                new ulong[] { 2077, 2079 },
                new ulong[] { 2190, 2192 },
                new ulong[] { 5231, 5243 },
                new ulong[] { 5209, 5256 },
                new ulong[] { 4735, 4737 },
                new ulong[] { 4766, 4768 },
                //new ulong[] { 6026, 6009 },
                //new ulong[] { 5990, 5960 },
                //new ulong[] { 6025, 5964 },
                //new ulong[] { 5989, 5957 },
                //new ulong[] { 2633, 2635 },
                //new ulong[] { 2664, 2666 },
                //new ulong[] { 3181, 3183 },
                //new ulong[] { 2726, 2728 },
                new ulong[] { 4469, 4471 },
                new ulong[] { 1705, 1707 },
                new ulong[] { 2799, 2801 },
                new ulong[] { 2881, 2883 },
                new ulong[] { 2830, 2832 },
                new ulong[] { 2912, 2914 },
                new ulong[] { 1784, 1786 },
                new ulong[] { 1816, 1818 },
                new ulong[] { 1848, 1850 },
                new ulong[] { 1736, 1738 }
            };

            m_voltageSetDescription = new[]
            {
                "Jof-Davidson",
                "Jscc-Ctg1",
                "Jscc-Ctg2",
                "Jscc-Ctg3",
                "Jscc-Stg",
                "Pacc-Ct1",
                "Pacc-Ct2",
                "Pacc-Ct3",
                "Pacc-St1",
                //"RCS1-Mocc-1",
                //"RCS1-Mocc-2",
                //"RCS1-Unit-3",
                //"RCS1-Unit-4",
                //"RCS2-Bank-5",
                //"RCS2-Mocc-3",
                //"RCS2-Unit-1",
                //"RCS2-Unit-2",
                "Riverbend Solar",
                "Sqn 500 Line",
                "Sqn-Gen1",
                "Sqn-Gen2",
                "Sqnxfmr5h",
                "Sqnxfmr5l",
                "Unit 1 BFN1",
                "Unit 2 BFN2",
                "Unit 3 BFN3",
                "Wbn 500 Line"
            };

            Debug.Assert(m_voltageMagnitudeIDs.Length == m_voltageSetDescription.Length);

            m_imbalances = new Imbalance[m_voltageMagnitudeIDs.Length];

            for (int i = 0; i < m_imbalances.Length; i++)
                m_imbalances[i] = new Imbalance();

            m_outputFiles = new StreamWriter[m_voltageMagnitudeIDs.Length];

            for (int i = 0; i < m_outputFiles.Length; i++)
            {
                m_outputFiles[i] = File.CreateText(FilePath.GetAbsolutePath(string.Format(OutputFile, m_voltageSetDescription[i])));
                m_outputFiles[i].AutoFlush = true;
            }

            m_allOutFile = File.CreateText(FilePath.GetAbsolutePath("AllImbalances.txt"));
            m_allOutFile.AutoFlush = true;

            m_missingPointCount = 0;
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
        public double TimeRange { get; set; }

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
                        foreach (StreamWriter outputFile in m_outputFiles)
                            outputFile?.Dispose();

                        m_allOutFile?.Dispose();
                    }
                }
                finally
                {
                    m_disposed = true;  // Prevent duplicate dispose.
                }
            }
        }

        /// <summary>
        /// Notifies algorithm that it should prepare to receive data.
        /// </summary>
        public void Initialize()
        {
            for (int i = 0; i < m_outputFiles.Length; i++)
            {
                m_outputFiles[i].WriteLine($"Sustained imbalances for \"{m_voltageSetDescription[i]}\" exceeding {ImbalanceThreshold:0.00%} for at least {MinimumSustainedRange} seconds.");
                m_outputFiles[i].WriteLine($"Summary covers data from {StartTime:yyyy-MM-dd HH:mm:ss.fff} to {EndTime:yyyy-MM-dd HH:mm:ss.fff} spanning {Time.ToElapsedTimeString(TimeRange, 3).ToLowerInvariant()}{Environment.NewLine}");
            }

            m_allOutFile.WriteLine($"This file captures any instanataneous imbalances exceeding {ImbalanceThreshold:0.00%}");
            m_allOutFile.WriteLine($"Details cover data from {StartTime:yyyy-MM-dd HH:mm:ss.fff} to {EndTime:yyyy-MM-dd HH:mm:ss.fff} spanning {Time.ToElapsedTimeString(TimeRange, 3).ToLowerInvariant()}{Environment.NewLine}");
        }

        private DataPoint[] GetDataPoints(DataPoint[] dataBlock, ulong[] voltageMagnitudeIDs)
        {
            DataPoint[] dataPoints = new DataPoint[voltageMagnitudeIDs.Length];

            for (int i = 0; i < voltageMagnitudeIDs.Length; i++)
                dataPoints[i] = dataBlock.FirstOrDefault(point => point.PointID == voltageMagnitudeIDs[i]);

            return dataPoints;
        }

        private void SummarizeImbalance(int index, Imbalance imbalance)
        {
            double range = imbalance.Range;

            if (range < MinimumSustainedRange)
                return;

            string message = $"{new DateTime((long)imbalance.Start):yyyy-MM-dd HH:mm:ss.fff} for {Time.ToElapsedTimeString(range, 3).ToLowerInvariant()} - average imbalance: {imbalance.Total / imbalance.Count:0.0000%}";
            m_outputFiles[index].WriteLine(message);
            ShowMessage($"{message}{Environment.NewLine}");
        }

        /// <summary>
        /// Allows algorithm to augment point ID list provided by UI.
        /// </summary>
        /// <param name="pointIDList">Point ID list.</param>
        public void AugmentPointIDList(List<ulong> pointIDList)
        {
            // In this algorithm, point ID list is fixed so we ignore user input
            pointIDList.Clear();
            pointIDList.AddRange(m_voltageMagnitudeIDs.SelectMany(ids => ids));
        }

        /// <summary>
        /// Default data processing entry point for <see cref="Algorithm"/>.
        /// </summary>
        /// <param name="timestamp">Timestamp of <paramref name="dataBlock"/>.</param>
        /// <param name="dataBlock">Points values read at current timestamp.</param>
        public void Execute(DateTime timestamp, DataPoint[] dataBlock)
        {
            for (int i = 0; i < m_voltageMagnitudeIDs.Length; i++)
            {
                ulong[] voltageMagnitudeIDs = m_voltageMagnitudeIDs[i];
                DataPoint[] dataPoints = GetDataPoints(dataBlock, voltageMagnitudeIDs);

                if (dataPoints.Length == 2 && (object)dataPoints[0] != null && (object)dataPoints[1] != null)
                {
                    double imbalanceRatio = dataPoints[0].ValueAsSingle / dataPoints[1].ValueAsSingle;
                    Imbalance imbalance = m_imbalances[i];

                    if (!double.IsInfinity(imbalanceRatio) && imbalanceRatio > ImbalanceThreshold)
                    {
                        if (imbalance.Start == 0UL)
                        {
                            imbalance.Start = dataPoints[0].Timestamp;
                            ShowMessage($"*** Imbalance detected for \"{m_voltageSetDescription[i]}\" at {new DateTime((long)imbalance.Start):yyyy-MM-dd HH:mm:ss.ffff} ***{Environment.NewLine}");
                        }

                        imbalance.Total += imbalanceRatio;
                        imbalance.Count++;

                        m_allOutFile.WriteLine($"{new DateTime((long)imbalance.Start):yyyy-MM-dd HH:mm:ss.fff} - imbalance for \"{m_voltageSetDescription[i]}\": {imbalanceRatio:0.0000%}");
                    }
                    else
                    {
                        if (imbalance.Start != 0UL)
                        {
                            imbalance.Stop = dataPoints[0].Timestamp;
                            SummarizeImbalance(i, imbalance);
                            imbalance.Reset();
                        }
                    }
                }
                else
                {
                    m_missingPointCount++;
                }
            }

            if (++m_processedDataBlocks % MessageInterval == 0)
                ShowMessage($"Analyzed {m_processedDataBlocks:N0} timestamps so far with {m_missingPointCount:N0} missing points.{Environment.NewLine}");
        }

        #endregion
    }
}
