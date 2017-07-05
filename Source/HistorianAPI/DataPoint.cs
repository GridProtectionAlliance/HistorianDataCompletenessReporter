//******************************************************************************************************
//  DataPoint.cs - Gbtc
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
using System.Runtime.CompilerServices;
using GSF;

namespace HistorianDataWalker.HistorianAPI
{
    public class DataPoint
    {
        public ulong Timestamp;
        public ulong PointID;
        public ulong Value;
        public ulong Flags;

        public float ValueAsSingle
        {
            get
            {
                return BitConvert.ToSingle(Value);
            }
            set
            {
                Value = BitConvert.ToUInt64(value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsEmpty() => Timestamp == 0 && PointID == 0 && Value == 0 && Flags == 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            Timestamp = 0;
            PointID = 0;
            Value = 0;
            Flags = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clone(DataPoint dataPoint)
        {
            dataPoint.Timestamp = Timestamp;
            dataPoint.PointID = PointID;
            dataPoint.Value = Value;
            dataPoint.Flags = Flags;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DataPoint Clone()
        {
            DataPoint dataPoint = new DataPoint();
            Clone(dataPoint);
            return dataPoint;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong RoundTimestamp(DateTime timestamp, int frameRate) => (ulong)Ticks.RoundToSubsecondDistribution(timestamp, frameRate).Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong RoundTimestamp(ulong timestamp, int frameRate) => (ulong)Ticks.RoundToSubsecondDistribution((long)timestamp, frameRate).Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareTimestamps(ulong left, ulong right, int frameRate) => RoundTimestamp(left, frameRate).CompareTo(RoundTimestamp(right, frameRate));
    }
}