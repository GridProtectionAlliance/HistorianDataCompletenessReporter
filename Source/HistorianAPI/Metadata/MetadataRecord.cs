//******************************************************************************************************
//  Metadata.cs - Gbtc
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
using System.Data;
using System.Windows.Forms;
using GSF.TimeSeries;

namespace HistorianDataWalker.HistorianAPI.Metadata
{
    public class MetadataRecord
    {
        public ulong PointID;
        public Guid SignalID;
        public string PointTag;
        public string SignalReference;
        public string DeviceName;
        public string SignalAcronym;
        public string Description;

        private MetadataRecord(DataRow row)
        {
            MeasurementKey measurementKey;

            Guid.TryParse(row["SignalID"].ToString(), out SignalID);
            MeasurementKey.TryParse(row["ID"].ToString(), out measurementKey);

            PointID = measurementKey.ID;
            PointTag = row["PointTag"].ToString();
            SignalReference = row["SignalReference"].ToString();
            DeviceName = row["DeviceAcronym"].ToString();
            SignalAcronym = row["SignalAcronym"].ToString();
            Description = row["Description"].ToString();
        }

        public static DataSet Metadata;

        public static List<MetadataRecord> Query(string host, int port, int timeout = -1)
        {
            List<MetadataRecord> measurements = new List<MetadataRecord>();

            // Note that openHistorian internal publisher controls how many tables / fields to send as meta-data to subscribers (user controllable),
            // as a result, not all fields in associated database views will be available. Below are the default SELECT filters the publisher
            // will apply to the "MeasurementDetail", "DeviceDetail" and "PhasorDetail" database views:

            // SELECT NodeID, UniqueID, OriginalSource, IsConcentrator, Acronym, Name, AccessID, ParentAcronym, ProtocolName, FramesPerSecond, CompanyAcronym, VendorAcronym, VendorDeviceName, Longitude, Latitude, InterconnectionName, ContactList, Enabled, UpdatedOn FROM DeviceDetail WHERE IsConcentrator = 0
            // SELECT DeviceAcronym, ID, SignalID, PointTag, SignalReference, SignalAcronym, PhasorSourceIndex, Description, Internal, Enabled, UpdatedOn FROM MeasurementDetail
            // SELECT DeviceAcronym, Label, Type, Phase, SourceIndex, UpdatedOn FROM PhasorDetail
            // SELECT VersionNumber FROM SchemaVersion

            DataTable measurementTable = null;

            string connectionString = $"server={host}:{port}; interface=0.0.0.0";

            Metadata = MetadataRetriever.GetMetadata(connectionString, timeout);

            // Reference needed meta-data tables
            measurementTable = Metadata.Tables["MeasurementDetail"];

            if ((object)measurementTable != null)
            {
                // Do something with measurement records
                foreach (DataRow measurement in measurementTable.Rows)
                    measurements.Add(new MetadataRecord(measurement));
            }

            return measurements;
        }
    }
}