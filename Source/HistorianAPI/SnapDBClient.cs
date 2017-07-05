//******************************************************************************************************
//  SnapDBClient.cs - Gbtc
//
//  Copyright © 2017, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
//  file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  04/20/2017 - J. Ritchie Carroll
//       Generated original version of source code.
//
//******************************************************************************************************

using System;
using System.Collections.Generic;
using GSF.Snap;
using GSF.Snap.Filters;
using GSF.Snap.Services;
using GSF.Snap.Services.Reader;
using openHistorian.Net;
using openHistorian.Snap;

namespace HistorianDataWalker.HistorianAPI
{
    public sealed class SnapDBClient : IDisposable
    {
        private readonly HistorianClient m_client;
        private readonly ClientDatabaseBase<HistorianKey, HistorianValue> m_database;
        private readonly TreeStream<HistorianKey, HistorianValue> m_stream;
        private readonly HistorianKey m_key;
        private readonly HistorianValue m_value;
        private bool m_disposed;

        public SnapDBClient(string hostAddress, int port, string instanceName, DateTime startTime, DateTime endTime, int frameRate, IEnumerable<ulong> pointIDs)
        {
            m_client = new HistorianClient(hostAddress, port);
            m_database = m_client.GetDatabase<HistorianKey, HistorianValue>(instanceName);
            m_key = new HistorianKey();
            m_value = new HistorianValue();

            SeekFilterBase<HistorianKey> timeFilter = TimestampSeekFilter.CreateFromRange<HistorianKey>(DataPoint.RoundTimestamp(startTime, frameRate), DataPoint.RoundTimestamp(endTime, frameRate));
            MatchFilterBase<HistorianKey, HistorianValue> pointFilter = PointIdMatchFilter.CreateFromList<HistorianKey, HistorianValue>(pointIDs);

            m_stream = m_database.Read(SortedTreeEngineReaderOptions.Default, timeFilter, pointFilter);
        }

        ~SnapDBClient()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                try
                {
                    if (disposing)
                    {
                        if ((object)m_stream != null)
                            m_stream.Dispose();

                        if ((object)m_database != null)
                            m_database.Dispose();

                        if ((object)m_client != null)
                            m_client.Dispose();
                    }
                }
                finally
                {
                    m_disposed = true;  // Prevent duplicate dispose.
                }
            }
        }

        public bool ReadNext(DataPoint point)
        {
            if ((object)m_stream == null)
                throw new NullReferenceException("Stream is not initialized");

            if (!m_stream.Read(m_key, m_value))
                return false;

            point.Timestamp = m_key.Timestamp;
            point.PointID = m_key.PointID;
            point.Value = m_value.Value1;
            point.Flags = m_value.Value3;

            return true;
        }
    }
}